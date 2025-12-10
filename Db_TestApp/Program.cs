using System;
using System.Data.SQLite;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace Db_TestApp
{
    internal class Program
    {
        private static object consoleLock = new object();

        static void Main(string[] args)
        {
            while (true)
            {
                Console.Clear();
                Console.WriteLine("=== SQLite 성능 테스트 프로그램 ===");
                Console.WriteLine();
                Console.WriteLine("1. WAL vs MEMORY 성능 비교 (단일 DB, 1000만 레코드)");
                Console.WriteLine("2. WAL 동시성 테스트 (Write + Read 부하 검증)");
                //Console.WriteLine("3. 몬테카를로 시뮬레이션 (100회, DB 매번 생성)");
                Console.WriteLine("0. 종료");
                Console.WriteLine();
                Console.Write("선택: ");

                string choice = Console.ReadLine();

                switch (choice)
                {
                    case "1":
                        RunPerformanceTest();
                        break;
                    case "2":
                        RunConcurrentReadWriteTest();
                        break;
                    //case "3":
                    //    RunMonteCarloTest();
                    //    break;
                    case "0":
                        return;
                    default:
                        Console.WriteLine("잘못된 선택입니다.");
                        Thread.Sleep(1000);
                        break;
                }
            }
        }

        static void RunConcurrentReadWriteTest()
        {
            Console.Clear();
            Console.WriteLine("=== WAL 동시성 테스트 (Write + Read 부하 검증) ===");
            Console.WriteLine("설명: Reader(단일 스레드) 유무에 따른 Write 소요 시간 비교");
            Console.WriteLine();

            Console.Write("총 레코드 수 (기본 10000000): ");
            string input = Console.ReadLine();
            int totalRecords = string.IsNullOrEmpty(input) ? 10_000_000 : int.Parse(input);

            Console.Write("트랜잭션 당 레코드 수 (기본 1): ");
            input = Console.ReadLine();
            int recordsPerTransaction = string.IsNullOrEmpty(input) ? 1 : int.Parse(input);

            Console.Write("업데이트 간격 (기본 10000): ");
            input = Console.ReadLine();
            int updateInterval = string.IsNullOrEmpty(input) ? 10000 : int.Parse(input);

            Console.WriteLine();
            Console.WriteLine($"설정: 총 {totalRecords:N0}개, 트랜잭션 당 {recordsPerTransaction:N0}개, 업데이트 {updateInterval:N0}개마다");
            Console.WriteLine();

            // Test2_Concurrency 폴더에 DB 파일 생성
            string testFolder = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Test2_Concurrency");
            if (Directory.Exists(testFolder)) Directory.Delete(testFolder, true);
            Directory.CreateDirectory(testFolder);

            string dbPathBaseline = Path.Combine(testFolder, "baseline.db");
            string dbPathConcurrency = Path.Combine(testFolder, "concurrency.db");

            int startLine = Console.CursorTop;
            Console.WriteLine("______________________________________________________");

            // 1. Baseline Test (Only Write)
            Console.WriteLine(">>> 1단계: Baseline 테스트 (Reader 없음) 시작...");
            Stopwatch sw1 = Stopwatch.StartNew();
            WriteWithMode(dbPathBaseline, "WAL", totalRecords, recordsPerTransaction, updateInterval, startLine + 2, true);
            sw1.Stop();
            Console.WriteLine($"\n>>> 1단계 완료. 소요 시간: {sw1.Elapsed.TotalSeconds:F3}초");
            Console.WriteLine("______________________________________________________");

            // 2. Concurrency Test (Write + Read)
            Console.WriteLine($">>> 2단계: Concurrency 테스트 (Reader 1개 가동) 시작...");

            CancellationTokenSource cts = new CancellationTokenSource();
            long totalReads = 0;
            double lastReadTime = 0;

            // Reader Task (1개 고정)
            Stopwatch swReader = Stopwatch.StartNew();
            long queryCount = 0; // 실제 쿼리 시도 횟수

            Task readerTask = Task.Run(() =>
            {
                using (var connection = new SQLiteConnection($"Data Source={dbPathConcurrency};Version=3;Pooling=False;Read Only=True;"))
                {
                    try { connection.Open(); } catch { return; }

                    long previousCount = 0;
                    while (!cts.Token.IsCancellationRequested)
                    {
                        try
                        {
                            using (var cmd = connection.CreateCommand())
                            {
                                // 단순 COUNT(*) 조회
                                cmd.CommandText = "SELECT COUNT(*) FROM Object_Table_0";
                                var result = cmd.ExecuteScalar();
                                long currentCount = result != null ? Convert.ToInt64(result) : 0;

                                Interlocked.Exchange(ref totalReads, currentCount); // 조회된 레코드 수 업데이트
                                Interlocked.Increment(ref queryCount); // 쿼리 시도 횟수 증가

                                if (currentCount > previousCount)
                                {
                                    previousCount = currentCount;

                                    // 마지막 s_time 조회
                                    cmd.CommandText = "SELECT MAX(s_time) FROM Object_Table_0";
                                    var timeResult = cmd.ExecuteScalar();
                                    if (timeResult != null && timeResult != DBNull.Value)
                                    {
                                        lastReadTime = Convert.ToDouble(timeResult);
                                    }
                                }

                                // 모든 데이터를 읽었으면 종료
                                if (currentCount >= totalRecords)
                                {
                                    break;
                                }
                            }
                            // Checkpoint 부하 테스트를 위해 Sleep 제거 (Continuous Read)
                            Thread.Sleep(1); 
                        }
                        catch { /* 무시 */ }
                    }
                }
            });

            // Writer 시작 (Write + Read 정보 동시 표시)
            int displayStartLine = Console.CursorTop;
            Console.WriteLine("___________________");
            Console.WriteLine("WAL 모드 진행중 ~~");
            Console.WriteLine("Write - 쓴 개수: 0 / 0");
            Console.WriteLine("Write - 진행시간: 00:00:00.000");
            Console.WriteLine("Read  - 읽은 개수: 0");
            Console.WriteLine("Read  - 진행시간: 00:00:00.000");
            Console.WriteLine("___________________");

            Stopwatch sw2 = Stopwatch.StartNew();

            // Write 작업 수행
            string connectionString = $"Data Source={dbPathConcurrency};Version=3;Pooling=False;";
            using (var connection = new SQLiteConnection(connectionString))
            {
                connection.Open();

                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = "PRAGMA journal_mode=WAL;";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "PRAGMA wal_autocheckpoint=0;";
                    cmd.ExecuteNonQuery();
                    cmd.CommandText = "PRAGMA synchronous=NORMAL;";
                    cmd.ExecuteNonQuery();
                }

                // 테이블 3개 생성 (Baseline과 동일하게 맞춤)
                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = @"CREATE TABLE IF NOT EXISTS Object_Table_0 (
                        s_time REAL, 
                        COL0 REAL, COL1 REAL, COL2 REAL, COL3 REAL, COL4 REAL, COL5 REAL, COL6 REAL, COL7 REAL, 
                        COL8 REAL, COL9 INTEGER, COL10 REAL, COL11 REAL, COL12 REAL, COL13 REAL, COL14 REAL, COL15 REAL
                    );";
                    cmd.ExecuteNonQuery();
                }

                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = @"CREATE TABLE IF NOT EXISTS Object_Table_1 (
                        s_time REAL, 
                        COL0 REAL, COL1 REAL, COL2 REAL, COL3 REAL, COL4 REAL, COL5 REAL, COL6 REAL, COL7 REAL, 
                        COL8 INTEGER, COL9 INTEGER, COL10 INTEGER, COL11 INTEGER, 
                        COL12 REAL, COL13 REAL, COL14 REAL, COL15 REAL, COL16 REAL, COL17 REAL
                    );";
                    cmd.ExecuteNonQuery();
                }

                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = @"CREATE TABLE IF NOT EXISTS Object_Table_2 (
                        s_time REAL, 
                        COL0 REAL, COL1 INTEGER, COL2 REAL, COL3 REAL, COL4 REAL, COL5 REAL, COL6 REAL, COL7 REAL, 
                        COL8 REAL, COL9 REAL, COL10 INTEGER, COL11 INTEGER, COL12 INTEGER, 
                        COL13 REAL, COL14 REAL, COL15 REAL, COL16 REAL, COL17 REAL, COL18 REAL, COL19 REAL, COL20 REAL, COL21 REAL
                    );";
                    cmd.ExecuteNonQuery();
                }

                int recordsWritten = 0;
                int totalTransactions = totalRecords / recordsPerTransaction;

                // 파라미터 재사용을 위한 커맨드 준비 (3개 테이블)
                using (var cmd0 = connection.CreateCommand())
                using (var cmd1 = connection.CreateCommand())
                using (var cmd2 = connection.CreateCommand())
                {
                    cmd0.CommandText = "INSERT INTO Object_Table_0 VALUES (@s_time, @c0, @c1, @c2, @c3, @c4, @c5, @c6, @c7, @c8, @c9, @c10, @c11, @c12, @c13, @c14, @c15)";
                    cmd1.CommandText = "INSERT INTO Object_Table_1 VALUES (@s_time, @c0, @c1, @c2, @c3, @c4, @c5, @c6, @c7, @c8, @c9, @c10, @c11, @c12, @c13, @c14, @c15, @c16, @c17)";
                    cmd2.CommandText = "INSERT INTO Object_Table_2 VALUES (@s_time, @c0, @c1, @c2, @c3, @c4, @c5, @c6, @c7, @c8, @c9, @c10, @c11, @c12, @c13, @c14, @c15, @c16, @c17, @c18, @c19, @c20, @c21)";

                    // 파라미터 초기화
                    InitializeParameters(cmd0, 16);
                    InitializeParameters(cmd1, 18);
                    InitializeParameters(cmd2, 22);

                    for (int tx = 0; tx < totalTransactions; tx++)
                    {
                        using (var transaction = connection.BeginTransaction())
                        {
                            for (int i = 0; i < recordsPerTransaction; i++)
                            {
                                double s_time = tx * 0.01; // 시간 시뮬레이션

                                // Table 0
                                cmd0.Parameters["@s_time"].Value = s_time;
                                SetDummyValues(cmd0, 16);
                                cmd0.ExecuteNonQuery();

                                // Table 1
                                cmd1.Parameters["@s_time"].Value = s_time;
                                SetDummyValues(cmd1, 18);
                                cmd1.ExecuteNonQuery();

                                // Table 2
                                cmd2.Parameters["@s_time"].Value = s_time;
                                SetDummyValues(cmd2, 22);
                                cmd2.ExecuteNonQuery();
                            }
                            transaction.Commit();
                        }

                        recordsWritten += recordsPerTransaction;

                        // 진행 상황 업데이트
                        if (recordsWritten % updateInterval == 0 || recordsWritten == totalRecords)
                        {
                            long currentWalSize = 0;
                            try
                            {
                                string walPath = dbPathConcurrency + "-wal";
                                if (File.Exists(walPath)) currentWalSize = new FileInfo(walPath).Length;
                            }
                            catch { }

                            Console.SetCursorPosition(0, displayStartLine + 2);
                            Console.WriteLine($"Write - 쓴 개수: {recordsWritten:N0} / {totalRecords:N0}".PadRight(60));
                            Console.WriteLine($"Write - 진행시간: {sw2.Elapsed:hh\\:mm\\:ss\\.fff} (WAL 크기: {currentWalSize / 1024.0:F1} KB)".PadRight(60));
                            Console.WriteLine($"Read  - 조회된 행: {totalReads:N0} (시도: {Interlocked.Read(ref queryCount):N0}회)".PadRight(60));
                            Console.WriteLine($"Read  - 진행시간: {swReader.Elapsed:hh\\:mm\\:ss\\.fff}".PadRight(60));
                        }
                    }
                }
            }

            sw2.Stop();

            // 테스트 종료 후 Reader 중단
            cts.Cancel();
            swReader.Stop();
            try { readerTask.Wait(); } catch { }

            Console.SetCursorPosition(0, displayStartLine + 7);
            Console.WriteLine(">>> 2단계 완료.");
            Console.WriteLine($"    Write 소요 시간: {sw2.Elapsed.TotalSeconds:F3}초");
            Console.WriteLine($"    Read 작업 시간: {swReader.Elapsed.TotalSeconds:F3}초 (총 {Interlocked.Read(ref queryCount):N0}회 시도)");
            Console.WriteLine("______________________________________________________");

            // 결과 요약
            Console.WriteLine();
            Console.WriteLine("=== 테스트 결과 요약 ===");
            Console.WriteLine($"1. Baseline (Write Only)  : {sw1.Elapsed.TotalSeconds:F3} 초");
            Console.WriteLine($"2. Concurrent (Write+Read): {sw2.Elapsed.TotalSeconds:F3} 초");

            double diff = sw2.Elapsed.TotalSeconds - sw1.Elapsed.TotalSeconds;
            double percent = (diff / sw1.Elapsed.TotalSeconds) * 100;

            Console.WriteLine($"차이 (오버헤드): {diff:F3}초 ({percent:F1}%)");

            if (Math.Abs(percent) < 5.0)
                Console.WriteLine("=> 결론: Reader가 Write 성능에 거의 영향을 주지 않음 (오버헤드 미미함)");
            else
                Console.WriteLine("=> 결론: 유의미한 오버헤드(또는 변동)가 관측됨");

            Console.WriteLine();
            Console.WriteLine("엔터키를 누르면 메뉴로 돌아갑니다...");
            Console.ReadLine();
        }

        static void InitializeDatabase(string dbPath)
        {
            string connectionString = $"Data Source={dbPath};Version=3;Pooling=False;";
            using (var connection = new SQLiteConnection(connectionString))
            {
                connection.Open();
                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;";
                    cmd.ExecuteNonQuery();

                    cmd.CommandText = @"CREATE TABLE IF NOT EXISTS Object_Table_0 (
                        s_time REAL, 
                        COL0 REAL, COL1 REAL, COL2 REAL, COL3 REAL, COL4 REAL, COL5 REAL, COL6 REAL, COL7 REAL, 
                        COL8 REAL, COL9 INTEGER, COL10 REAL, COL11 REAL, COL12 REAL, COL13 REAL, COL14 REAL, COL15 REAL
                    );";
                    cmd.ExecuteNonQuery();
                }
            }
        }

        static void RunPerformanceTest()
        {
            Console.Clear();
            Console.WriteLine("=== WAL vs MEMORY 성능 비교 ===");
            Console.WriteLine();
            Console.Write("총 레코드 수 (기본 10000000): ");
            string input = Console.ReadLine();
            int totalRecords = string.IsNullOrEmpty(input) ? 10_000_000 : int.Parse(input);

            Console.Write("트랜잭션 당 레코드 수 (기본 1): ");
            input = Console.ReadLine();
            int recordsPerTransaction = string.IsNullOrEmpty(input) ? 1 : int.Parse(input);

            Console.Write("업데이트 간격 (기본 10000): ");
            input = Console.ReadLine();
            int updateInterval = string.IsNullOrEmpty(input) ? 10000 : int.Parse(input);

            Console.WriteLine();
            Console.WriteLine($"설정: 총 {totalRecords:N0}개, 트랜잭션 당 {recordsPerTransaction:N0}개, 업데이트 {updateInterval:N0}개마다");
            Console.WriteLine();

            // Test1_Performance 폴더에 DB 파일 생성
            string testFolder = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Test1_Performance");
            if (Directory.Exists(testFolder)) Directory.Delete(testFolder, true);
            Directory.CreateDirectory(testFolder);

            string walDbPath = Path.Combine(testFolder, "wal.db");
            string memoryDbPath = Path.Combine(testFolder, "memory.db");

            int walStartLine = Console.CursorTop;
            Console.WriteLine("___________________");
            Console.WriteLine("WAL 모드 준비중...");
            Console.WriteLine("쓴 개수: 0");
            Console.WriteLine("진행시간: 00:00:00");
            Console.WriteLine("___________________");
            Console.WriteLine();

            int memoryStartLine = Console.CursorTop;
            Console.WriteLine("___________________");
            Console.WriteLine("Memory 모드 준비중...");
            Console.WriteLine("쓴 개수: 0");
            Console.WriteLine("진행시간: 00:00:00");
            Console.WriteLine("___________________");
            Console.WriteLine();

            Task walTask = Task.Run(() => WriteWithMode(walDbPath, "WAL", totalRecords, recordsPerTransaction, updateInterval, walStartLine, false));
            Task memoryTask = Task.Run(() => WriteWithMode(memoryDbPath, "MEMORY", totalRecords, recordsPerTransaction, updateInterval, memoryStartLine, false));

            Task.WaitAll(walTask, memoryTask);

            Console.SetCursorPosition(0, memoryStartLine + 6);
            Console.WriteLine();
            Console.WriteLine("=== 테스트 완료 ===");
            Console.WriteLine("엔터키를 누르면 메뉴로 돌아갑니다...");
            Console.ReadLine();
        }

        static void RunMonteCarloTest()
        {
            Console.Clear();
            Console.WriteLine("=== 몬테카를로 시뮬레이션 (100회) ===");
            Console.WriteLine();
            Console.Write("각 시뮬레이션 트랜잭션 수 (기본 3000): ");
            string input = Console.ReadLine();
            int transactionsPerSim = string.IsNullOrEmpty(input) ? 3000 : int.Parse(input);

            Console.Write("트랜잭션 당 레코드 수 (기본 100): ");
            input = Console.ReadLine();
            int recordsPerTransaction = string.IsNullOrEmpty(input) ? 100 : int.Parse(input);

            Console.WriteLine();
            Console.WriteLine($"설정: 100회 × {transactionsPerSim:N0} 트랜잭션, 트랜잭션 당 {recordsPerTransaction:N0}개");
            Console.WriteLine();

            int memoryStartLine = Console.CursorTop;
            Console.WriteLine("___________________");
            Console.WriteLine("MEMORY 모드");
            Console.WriteLine("진행: 0 / 100");
            Console.WriteLine("경과 시간: 00:00:00");
            Console.WriteLine("___________________");
            Console.WriteLine();

            int walDefaultStartLine = Console.CursorTop;
            Console.WriteLine("___________________");
            Console.WriteLine("WAL (기본 Checkpoint)");
            Console.WriteLine("진행: 0 / 100");
            Console.WriteLine("경과 시간: 00:00:00");
            Console.WriteLine("___________________");
            Console.WriteLine();

            int walNoCheckpointStartLine = Console.CursorTop;
            Console.WriteLine("___________________");
            Console.WriteLine("WAL (Checkpoint=0)");
            Console.WriteLine("진행: 0 / 100");
            Console.WriteLine("경과 시간: 00:00:00");
            Console.WriteLine("___________________");
            Console.WriteLine();

            // 순차 실행으로 변경하여 I/O 간섭 제거
            Console.WriteLine(">>> MEMORY 모드 테스트 시작...");
            RunMonteCarloSimulation("MEMORY", transactionsPerSim, recordsPerTransaction, memoryStartLine, false);

            Console.WriteLine();
            Console.WriteLine(">>> WAL (기본 Checkpoint) 테스트 시작...");
            RunMonteCarloSimulation("WAL_DEFAULT", transactionsPerSim, recordsPerTransaction, walDefaultStartLine, false);

            Console.WriteLine();
            Console.WriteLine(">>> WAL (Checkpoint=0) 테스트 시작...");
            RunMonteCarloSimulation("WAL_NO_CHECKPOINT", transactionsPerSim, recordsPerTransaction, walNoCheckpointStartLine, true);

            Console.SetCursorPosition(0, walNoCheckpointStartLine + 6);
            Console.WriteLine();
            Console.WriteLine("=== 몬테카를로 시뮬레이션 완료 ===");
            Console.WriteLine("엔터키를 누르면 메뉴로 돌아갑니다...");
            Console.ReadLine();
        }

        static void RunMonteCarloSimulation(string mode, int transactionsPerSim, int recordsPerTransaction, int startLine, bool disableCheckpoint)
        {
            Stopwatch totalSw = Stopwatch.StartNew();
            int simulations = 100;

            // 별도 폴더 생성하여 간섭 방지
            string baseFolder = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, $"Test_Data_{mode}");
            if (Directory.Exists(baseFolder)) Directory.Delete(baseFolder, true); // 초기화
            Directory.CreateDirectory(baseFolder);

            for (int sim = 0; sim < simulations; sim++)
            {
                string dbPath = Path.Combine(baseFolder, $"monte_{mode}_{sim}.db");

                WriteWithMode(dbPath, mode.StartsWith("WAL") ? "WAL" : "MEMORY", transactionsPerSim, recordsPerTransaction, int.MaxValue, -1, disableCheckpoint);

                UpdateMonteCarloProgress(mode, sim + 1, simulations, totalSw.Elapsed, startLine);
            }

            totalSw.Stop();
            UpdateMonteCarloFinal(mode, simulations, totalSw.Elapsed, startLine);
        }

        static void UpdateMonteCarloProgress(string mode, int current, int total, TimeSpan elapsed, int startLine)
        {
            lock (consoleLock)
            {
                try
                {
                    Console.CursorVisible = false;
                    Console.SetCursorPosition(0, startLine);
                    Console.Write("___________________".PadRight(Console.WindowWidth - 1));
                    Console.SetCursorPosition(0, startLine + 1);
                    string modeName = mode == "MEMORY" ? "MEMORY 모드" :
                                     mode == "WAL_DEFAULT" ? "WAL (기본 Checkpoint)" :
                                     "WAL (Checkpoint=0)";
                    Console.Write(modeName.PadRight(Console.WindowWidth - 1));
                    Console.SetCursorPosition(0, startLine + 2);
                    Console.Write($"진행: {current} / {total}".PadRight(Console.WindowWidth - 1));
                    Console.SetCursorPosition(0, startLine + 3);
                    Console.Write($"경과 시간: {elapsed:hh\\:mm\\:ss\\.fff}".PadRight(Console.WindowWidth - 1));
                    Console.SetCursorPosition(0, startLine + 4);
                    Console.Write("___________________".PadRight(Console.WindowWidth - 1));
                }
                finally
                {
                    Console.CursorVisible = true;
                }
            }
        }

        static void UpdateMonteCarloFinal(string mode, int total, TimeSpan elapsed, int startLine)
        {
            lock (consoleLock)
            {
                try
                {
                    Console.CursorVisible = false;
                    Console.SetCursorPosition(0, startLine);
                    Console.Write("___________________".PadRight(Console.WindowWidth - 1));
                    Console.SetCursorPosition(0, startLine + 1);
                    string modeName = mode == "MEMORY" ? "MEMORY 완료!" :
                                     mode == "WAL_DEFAULT" ? "WAL (기본) 완료!" :
                                     "WAL (CP=0) 완료!";
                    Console.Write(modeName.PadRight(Console.WindowWidth - 1));
                    Console.SetCursorPosition(0, startLine + 2);
                    Console.Write($"총 {total}회 완료".PadRight(Console.WindowWidth - 1));
                    Console.SetCursorPosition(0, startLine + 3);
                    Console.Write($"총 시간: {elapsed:hh\\:mm\\:ss\\.fff}".PadRight(Console.WindowWidth - 1));
                    Console.SetCursorPosition(0, startLine + 4);
                    Console.Write("___________________".PadRight(Console.WindowWidth - 1));
                }
                finally
                {
                    Console.CursorVisible = true;
                }
            }
        }

        static void WriteWithMode(string dbPath, string mode, int totalRecords, int recordsPerTransaction, int updateInterval, int startLine, bool disableCheckpoint)
        {
            Stopwatch sw = Stopwatch.StartNew();
            // ✅ Pooling=False: 연결 풀링 비활성화 -> 매번 물리적 파일 생성/닫기 강제
            string connectionString = $"Data Source={dbPath};Version=3;Pooling=False;";

            try
            {
                // DB 연결 (파일이 없으면 여기서 실제 생성됨)
                using (var connection = new SQLiteConnection(connectionString))
                {
                    connection.Open();

                    using (var cmd = connection.CreateCommand())
                    {
                        if (mode.Contains("WAL")) // "WAL", "WAL_DEFAULT", "WAL_NO_CHECKPOINT" 모두 처리
                        {
                            cmd.CommandText = "PRAGMA journal_mode=WAL;";
                            cmd.ExecuteNonQuery();

                            //// 실제 환경(Db_WriteApp)과 동일하게 WAL은 NORMAL 모드 사용 (안전성 확보)
                            cmd.CommandText = "PRAGMA synchronous=OFF;";
                            cmd.ExecuteNonQuery();

                            if (disableCheckpoint)
                            {
                                cmd.CommandText = "PRAGMA wal_autocheckpoint=0;";
                                cmd.ExecuteNonQuery();
                            }
                        }
                        else
                        {
                            cmd.CommandText = "PRAGMA journal_mode=MEMORY;";
                            cmd.ExecuteNonQuery();

                            // 실제 환경(Legacy)과 동일하게 MEMORY는 OFF 모드 사용 (속도 최유선)
                            cmd.CommandText = "PRAGMA synchronous=OFF;";
                            cmd.ExecuteNonQuery();
                        }
                    }

                    // 테이블 3개 생성 (실제 데이터 구조 반영)
                    // Object_Table_0: s_time, COL0..15
                    using (var cmd = connection.CreateCommand())
                    {
                        cmd.CommandText = @"CREATE TABLE IF NOT EXISTS Object_Table_0 (
                            s_time REAL, 
                            COL0 REAL, COL1 REAL, COL2 REAL, COL3 REAL, COL4 REAL, COL5 REAL, COL6 REAL, COL7 REAL, 
                            COL8 REAL, COL9 INTEGER, COL10 REAL, COL11 REAL, COL12 REAL, COL13 REAL, COL14 REAL, COL15 REAL
                        );";
                        cmd.ExecuteNonQuery();
                    }

                    // Object_Table_1: s_time, COL0..17
                    using (var cmd = connection.CreateCommand())
                    {
                        cmd.CommandText = @"CREATE TABLE IF NOT EXISTS Object_Table_1 (
                            s_time REAL, 
                            COL0 REAL, COL1 REAL, COL2 REAL, COL3 REAL, COL4 REAL, COL5 REAL, COL6 REAL, COL7 REAL, 
                            COL8 INTEGER, COL9 INTEGER, COL10 INTEGER, COL11 INTEGER, 
                            COL12 REAL, COL13 REAL, COL14 REAL, COL15 REAL, COL16 REAL, COL17 REAL
                        );";
                        cmd.ExecuteNonQuery();
                    }

                    // Object_Table_2: s_time, COL0..21
                    using (var cmd = connection.CreateCommand())
                    {
                        cmd.CommandText = @"CREATE TABLE IF NOT EXISTS Object_Table_2 (
                            s_time REAL, 
                            COL0 REAL, COL1 INTEGER, COL2 REAL, COL3 REAL, COL4 REAL, COL5 REAL, COL6 REAL, COL7 REAL, 
                            COL8 REAL, COL9 REAL, COL10 INTEGER, COL11 INTEGER, COL12 INTEGER, 
                            COL13 REAL, COL14 REAL, COL15 REAL, COL16 REAL, COL17 REAL, COL18 REAL, COL19 REAL, COL20 REAL, COL21 REAL
                        );";
                        cmd.ExecuteNonQuery();
                    }

                    int writtenCount = 0;
                    int totalTransactions = totalRecords / recordsPerTransaction;

                    // 파라미터 재사용을 위한 커맨드 준비
                    using (var cmd0 = connection.CreateCommand())
                    using (var cmd1 = connection.CreateCommand())
                    using (var cmd2 = connection.CreateCommand())
                    {
                        cmd0.CommandText = "INSERT INTO Object_Table_0 VALUES (@s_time, @c0, @c1, @c2, @c3, @c4, @c5, @c6, @c7, @c8, @c9, @c10, @c11, @c12, @c13, @c14, @c15)";
                        cmd1.CommandText = "INSERT INTO Object_Table_1 VALUES (@s_time, @c0, @c1, @c2, @c3, @c4, @c5, @c6, @c7, @c8, @c9, @c10, @c11, @c12, @c13, @c14, @c15, @c16, @c17)";
                        cmd2.CommandText = "INSERT INTO Object_Table_2 VALUES (@s_time, @c0, @c1, @c2, @c3, @c4, @c5, @c6, @c7, @c8, @c9, @c10, @c11, @c12, @c13, @c14, @c15, @c16, @c17, @c18, @c19, @c20, @c21)";

                        // 파라미터 초기화
                        InitializeParameters(cmd0, 16);
                        InitializeParameters(cmd1, 18);
                        InitializeParameters(cmd2, 22);

                        for (int tx = 0; tx < totalTransactions; tx++)
                        {
                            using (var transaction = connection.BeginTransaction())
                            {
                                for (int i = 0; i < recordsPerTransaction; i++)
                                {
                                    double s_time = tx * 0.01; // 시간 시뮬레이션

                                    // Table 0
                                    cmd0.Parameters["@s_time"].Value = s_time;
                                    SetDummyValues(cmd0, 16);
                                    cmd0.ExecuteNonQuery();

                                    // Table 1
                                    cmd1.Parameters["@s_time"].Value = s_time;
                                    SetDummyValues(cmd1, 18);
                                    cmd1.ExecuteNonQuery();

                                    // Table 2
                                    cmd2.Parameters["@s_time"].Value = s_time;
                                    SetDummyValues(cmd2, 22);
                                    cmd2.ExecuteNonQuery();
                                }
                                transaction.Commit();
                            }

                            writtenCount += recordsPerTransaction;

                            if (startLine >= 0 && (writtenCount % updateInterval == 0 || writtenCount == totalRecords))
                            {
                                UpdateConsole(mode, writtenCount, totalRecords, sw.Elapsed, startLine);
                            }
                        }
                    }

                    if (startLine >= 0)
                    {
                        UpdateConsoleFinal(mode, totalRecords, sw.Elapsed, startLine);
                    }
                }

                sw.Stop();
            }
            catch (Exception ex)
            {
                if (startLine >= 0)
                {
                    lock (consoleLock)
                    {
                        Console.SetCursorPosition(0, startLine + 1);
                        Console.WriteLine($"{mode} 모드 오류: {ex.Message}".PadRight(Console.WindowWidth - 1));
                    }
                }
            }
        }

        static void UpdateConsole(string mode, int count, int total, TimeSpan elapsed, int startLine)
        {
            lock (consoleLock)
            {
                try
                {
                    Console.CursorVisible = false;
                    Console.SetCursorPosition(0, startLine);
                    Console.Write("___________________".PadRight(Console.WindowWidth - 1));
                    Console.SetCursorPosition(0, startLine + 1);
                    Console.Write($"{mode} 모드 진행중 ~~".PadRight(Console.WindowWidth - 1));
                    Console.SetCursorPosition(0, startLine + 2);
                    Console.Write($"쓴 개수: {count:N0} / {total:N0}".PadRight(Console.WindowWidth - 1));
                    Console.SetCursorPosition(0, startLine + 3);
                    Console.Write($"진행시간: {elapsed:hh\\:mm\\:ss\\.ffffff}".PadRight(Console.WindowWidth - 1));
                    Console.SetCursorPosition(0, startLine + 4);
                    Console.Write("___________________".PadRight(Console.WindowWidth - 1));
                }
                finally
                {
                    Console.CursorVisible = true;
                }
            }
        }

        static void UpdateConsoleFinal(string mode, int total, TimeSpan elapsed, int startLine)
        {
            lock (consoleLock)
            {
                try
                {
                    Console.CursorVisible = false;
                    Console.SetCursorPosition(0, startLine);
                    Console.Write("___________________".PadRight(Console.WindowWidth - 1));
                    Console.SetCursorPosition(0, startLine + 1);
                    Console.Write($"{mode} 모드 완료!".PadRight(Console.WindowWidth - 1));
                    Console.SetCursorPosition(0, startLine + 2);
                    Console.Write($"총 개수: {total:N0}".PadRight(Console.WindowWidth - 1));
                    Console.SetCursorPosition(0, startLine + 3);
                    Console.Write($"총 시간: {elapsed:hh\\:mm\\:ss\\.fff}".PadRight(Console.WindowWidth - 1));
                    Console.SetCursorPosition(0, startLine + 4);
                    Console.Write("___________________".PadRight(Console.WindowWidth - 1));
                }
                finally
                {
                    Console.CursorVisible = true;
                }
            }
        }
        static void InitializeParameters(SQLiteCommand cmd, int colCount)
        {
            cmd.Parameters.Add(new SQLiteParameter("@s_time", System.Data.DbType.Double));
            for (int j = 0; j < colCount; j++)
            {
                cmd.Parameters.Add(new SQLiteParameter($"@c{j}", System.Data.DbType.Double)); // 기본 Double, Int는 호환됨
            }
        }

        static void SetDummyValues(SQLiteCommand cmd, int colCount)
        {
            for (int j = 0; j < colCount; j++)
            {
                cmd.Parameters[$"@c{j}"].Value = 1.23456789; // Dummy value
            }
        }
    }
}
