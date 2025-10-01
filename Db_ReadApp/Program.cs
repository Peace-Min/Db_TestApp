using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Db_ReadApp
{
    internal class Program
    {
        static void Main(string[] args)
        {
            string dbPath = "test.db";

            if (!File.Exists(dbPath))
            {
                Console.WriteLine("DB 파일이 존재하지 않습니다: " + dbPath);
                return;
            }

            using (var connection = new SQLiteConnection("Data Source=" + dbPath))
            {
                connection.Open();

                // ReadApp에서 Lock 대기 설정
                using (var pragmaCmd = connection.CreateCommand())
                {
                    pragmaCmd.CommandText = @"
            PRAGMA busy_timeout = 50;
        ";
                    pragmaCmd.ExecuteNonQuery();
                }

                // 로그 파일 준비 (테이블별)
                //StreamWriter[] logWriters = new StreamWriter[5];
                //for (int t = 0; t < 5; t++)
                //{
                //    logWriters[t] = new StreamWriter($"Table_{t}.txt", false, Encoding.UTF8);
                //}

                int readerId = 0;
                if (args.Length > 0) int.TryParse(args[0], out readerId);

                // 로그 파일 준비 (테이블별)
                StreamWriter[] logWriters = new StreamWriter[5];
                for (int t = 0; t < 5; t++)
                {
                    string fileName = $"Table_{t}_Read{readerId}.txt"; // ← 고유 파일명
                    logWriters[t] = new StreamWriter(fileName, false, Encoding.UTF8);
                }

                Console.WriteLine("30초 동안 실시간 데이터 읽기 및 로그 기록 시작...");

                int totalRows = 3000; // 30초 / 0.01s
                for (int i = 0; i < totalRows; i++)
                {
                    double s_time = Math.Round(i * 0.01, 6);

                    for (int t = 0; t < 5; t++)
                    {
                        string selectSql = string.Format(
                            "SELECT * FROM Table_{0} WHERE s_time = @s_time;", t);

                        using (var selectCmd = connection.CreateCommand())
                        {
                            selectCmd.CommandText = selectSql;
                            selectCmd.Parameters.AddWithValue("@s_time", s_time);

                            using (var reader = selectCmd.ExecuteReader())
                            {
                                while (reader.Read())
                                {
                                    //Thread.Sleep(50);
                                    StringBuilder logLine = new StringBuilder();
                                    logLine.AppendFormat("s_time={0:F2}", s_time.ToString());

                                    for (int col = 0; col < 20; col++)
                                    {
                                        string colName = "Col" + col;
                                        string colValue = reader[colName].ToString();
                                        logLine.AppendFormat(", {0}={1}", colName, colValue);
                                    }

                                    logWriters[t].WriteLine(logLine.ToString());
                                }
                            }
                        }
                    }
                    Console.WriteLine(s_time.ToString());
                    // 10ms 간격 유지
                    Thread.Sleep(1);
                }

                // 로그 파일 닫기
                for (int t = 0; t < 5; t++)
                {
                    logWriters[t].Close();
                }

                Console.WriteLine("30초간 데이터 읽기 및 로그 기록 완료.");

                using (var checkpointCmd = connection.CreateCommand())
                {
                    checkpointCmd.CommandText = "PRAGMA wal_checkpoint(FULL);";
                    checkpointCmd.ExecuteNonQuery();
                }

            }
        }
    }
}
