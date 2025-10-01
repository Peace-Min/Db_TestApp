using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Db_WriteApp
{
    internal class Program
    {
        public static void Main(string[] args)
        {
            string dbPath = "test.db";
            if (File.Exists(dbPath)) File.Delete(dbPath);

            using (var connection = new SQLiteConnection("Data Source=" + dbPath))
            {
                connection.Open();

                // PRAGMA 설정
                using (var pragmaCmd = connection.CreateCommand())
                {
                    // 기존 프로젝트 PRAGMA
                    //pragmaCmd.CommandText = @"
                    //    PRAGMA synchronous = OFF;
                    //    PRAGMA journal_mode = MEMORY;
                    //";

                    // 변경 PRAGMA
                    pragmaCmd.CommandText = @"
                       PRAGMA journal_mode = WAL;
                       PRAGMA synchronous = NORMAL;
                       PRAGMA busy_timeout = 50;
                    ";
                    pragmaCmd.ExecuteNonQuery();
                }

                // 테이블 생성 (Table_0 ~ Table_4)
                for (int t = 0; t < 5; t++)
                {
                    string createTableSql = string.Format(@"
                        CREATE TABLE Table_{0} (
                            s_time REAL PRIMARY KEY
                            {1}
                        );", t, CreateColumns());

                    using (var cmd = connection.CreateCommand())
                    {
                        cmd.CommandText = createTableSql;
                        cmd.ExecuteNonQuery();
                    }
                }

                Console.WriteLine("30초 동안 실시간 데이터 삽입 시작...");

                // 30초 동안 실시간 삽입 (10ms 간격 → 총 3000회)
                int totalRows = 3000;
                for (int i = 0; i < totalRows; i++)
                {
                    double s_time = Math.Round(i * 0.01, 6);

                    for (int t = 0; t < 5; t++)
                    {
                        string insertSql = string.Format(@"
            INSERT OR REPLACE INTO Table_{0} (s_time, {1})
            VALUES (@s_time, {2});",
                            t,
                            string.Join(", ", CreateColumnNames()),
                            string.Join(", ", CreateParameters()));

                        using (var insertCmd = connection.CreateCommand())
                        {
                            insertCmd.CommandText = insertSql;
                            insertCmd.Parameters.AddWithValue("@s_time", s_time);

                            for (int j = 0; j < 20; j++)
                            {
                                insertCmd.Parameters.AddWithValue("@Col" + j,
                                    string.Format("Val_{0}_{1}_{2}", t, i, j));
                            }

                            try
                            {
                                insertCmd.ExecuteNonQuery();
                            }
                            catch (SQLiteException ex)
                            {
                                if (ex.Message.Contains("database is locked"))
                                {
                                    Console.WriteLine("[LOCK DETECTED] Table_{0}, s_time={1}", t, s_time);
                                }
                                else
                                {
                                    Console.WriteLine("[ERROR] {0}", ex.Message);
                                }
                            }
                        }
                    }
                    Console.WriteLine(s_time);
                    Thread.Sleep(10); // 10ms 주기
                }

                Console.WriteLine("30초간 실시간 데이터 삽입 완료.");
            }
        }

        private static string CreateColumns()
        {
            string[] cols = new string[20];
            for (int i = 0; i < 20; i++)
            {
                cols[i] = "Col" + i + " VARCHAR(1024)";
            }
            return ", " + string.Join(", ", cols);
        }

        private static string[] CreateColumnNames()
        {
            string[] cols = new string[20];
            for (int i = 0; i < 20; i++)
            {
                cols[i] = "Col" + i;
            }
            return cols;
        }

        private static string[] CreateParameters()
        {
            string[] cols = new string[20];
            for (int i = 0; i < 20; i++)
            {
                cols[i] = "@Col" + i;
            }
            return cols;
        }
    }
}