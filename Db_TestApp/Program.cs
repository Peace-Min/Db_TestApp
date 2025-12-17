using System;
using System.Data.SQLite;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Text;

namespace Db_TestApp
{
    internal class Program
    {
        private static object consoleLock = new object();

        static void Main(string[] args)
        {
            Console.OutputEncoding = Encoding.UTF8;
            while (true)
            {
                if (!Console.IsOutputRedirected)
                {
                    try { Console.Clear(); } catch { }
                }
                Console.WriteLine("=== SQLite 성능 테스트 프로그램 ===");
                Console.WriteLine();
                Console.WriteLine("1. WAL vs Default 성능 비교 (단일 DB, 1000만 레코드)");
                Console.WriteLine("2. WAL 동시성 테스트 (Write + Read 부하 검증)");
                Console.WriteLine("3. 분할 실행 시뮬레이션 (WAL 파일 생성 오버헤드 확인)");
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
                    case "3":
                        RunSplitSimulationTest();
                        break;
                    case "4":
                        RunConcurrentReadWriteTest(true);
                        break;
                    case "0":
                        return;
                    default:
                        Console.WriteLine("잘못된 선택입니다.");
                        Thread.Sleep(1000);
                        break;
                }
            }
        }

        static void RunConcurrentReadWriteTest(bool isNotUsedCheckPoint = false)
        {
            if (!Console.IsOutputRedirected) try { Console.Clear(); } catch { }

            if (isNotUsedCheckPoint)
            {
                Console.WriteLine("=== WAL 동시성 테스트 (Write + Read 부하 검증, CheckPoint 비활성화) ===");
            }
            else
            {
                Console.WriteLine("=== WAL 동시성 테스트 (Write + Read 부하 검증) ===");
            }
            Console.WriteLine("설명: Reader(단일 스레드) 유무에 따른 Write 소요 시간 비교");
            Console.WriteLine();

            Console.Write("총 레코드 수 (기본 10000000): ");
            string input = Console.ReadLine();
            int totalRecords = string.IsNullOrEmpty(input) ? 10_000_000 : int.Parse(input);

            Console.Write("트랜잭션 당 레코드 수 (기본 1): ");
            input = Console.ReadLine();
            int recordsPerTransaction = string.IsNullOrEmpty(input) ? 1 : int.Parse(input);

            //Console.Write("업데이트 간격 (기본 10): ");
            //input = Console.ReadLine();
            int updateInterval = recordsPerTransaction >= 10000 ? recordsPerTransaction : recordsPerTransaction * 10;

            Console.WriteLine();
            Console.WriteLine($"설정: 총 {totalRecords:N0}개, 트랜잭션 당 {recordsPerTransaction:N0}개, 업데이트 {updateInterval:N0}개마다");
            Console.WriteLine();

            // Test2_Concurrency 폴더에 DB 파일 생성
            string testFolder = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Test2_Concurrency");
            if (Directory.Exists(testFolder)) Directory.Delete(testFolder, true);
            Directory.CreateDirectory(testFolder);

            string dbPathBaseline = Path.Combine(testFolder, "baseline.db");
            string dbPathConcurrency = Path.Combine(testFolder, "concurrency.db");

            int startLine = 0;
            if (!Console.IsOutputRedirected) try { startLine = Console.CursorTop; } catch { }
            Console.WriteLine("______________________________________________________");

            // 1. Baseline Test (Only Write)
            Console.WriteLine(">>> 1단계: Baseline 테스트 (Reader 없음) 시작...");
            Stopwatch sw1 = Stopwatch.StartNew();
            WriteWithMode(dbPathBaseline, "WAL", totalRecords, recordsPerTransaction, updateInterval,
                (count, elapsed) => UpdateConsole("WAL", count, totalRecords, elapsed, startLine + 2),
                (elapsed) => UpdateConsoleFinal("WAL", totalRecords, elapsed, startLine + 2));
            sw1.Stop();
            Console.WriteLine($"\n>>> 1단계 완료. 소요 시간: {sw1.Elapsed.TotalSeconds:F3}초");
            Console.WriteLine("______________________________________________________");

            // 2. Concurrency Test (Write + Read)
            Console.WriteLine($">>> 2단계: Concurrency 테스트 (Reader 1개 가동) 시작...");

            CancellationTokenSource cts = new CancellationTokenSource();
            long totalReads = 0;

            // Reader Task (1개 고정)
            Stopwatch swReader = Stopwatch.StartNew(); // Reader Task 시작과 동시에 측정
            long queryCount = 0; // 실제 쿼리 시도 횟수

            Task readerTask = Task.Run(() =>
            {
                // DB와 테이블이 생성될 때까지 대기
                SQLiteConnection connection = null;
                bool connected = false;

                while (!connected)
                {
                    try
                    {
                        connection = new SQLiteConnection($"Data Source={dbPathConcurrency};Version=3;Pooling=False;Read Only=True;");
                        connection.Open();

                        // 테이블 존재 여부 확인
                        using (var cmd = connection.CreateCommand())
                        {
                            cmd.CommandText = "SELECT COUNT(*) FROM Object_Table_0";
                            cmd.ExecuteScalar(); // 테이블이 없으면 예외 발생
                        }

                        connected = true; // 연결 및 테이블 확인 성공
                    }
                    catch
                    {
                        // DB 파일이나 테이블이 아직 생성되지 않음 → 재시도
                        connection?.Dispose();
                        connection = null;
                        Thread.Sleep(1); // 1ms 대기 후 재시도
                    }
                }

                // 연결 성공 후 읽기 작업 수행
                // 연결 성공 후 읽기 작업 수행
                using (connection)
                {
                    while (true)
                    {
                        try
                        {
                            using (var cmd = connection.CreateCommand())
                            {
                                cmd.CommandText = "SELECT COUNT(*) FROM Object_Table_0";
                                var result = cmd.ExecuteScalar();

                                long currentCount = result == null || result == DBNull.Value
                                    ? 0
                                    : Convert.ToInt64(result);

                                Interlocked.Exchange(ref totalReads, currentCount);
                                Interlocked.Increment(ref queryCount);

                                // 🔥 모든 데이터를 읽었으면 Reader 종료
                                if (currentCount >= totalRecords)
                                {
                                    // 마지막 조회 결과를 화면에 반영할 시간을 주기 위해 잠깐 대기
                                    Thread.Sleep(10);
                                    break;
                                }
                            }
                        }
                        catch
                        {
                            // WRITER 잠금 등 발생 가능 → 잠깐 대기 후 재시도
                        }

                        // Reader 너무 빠르면 안되니까 살짝 쉼 (Checkpoint 유도)
                        Thread.Sleep(1);
                    }
                }
            });

            // Writer 시작 (Write + Read 정보 동시 표시)
            int displayStartLine = 0;
            if (!Console.IsOutputRedirected) try { displayStartLine = Console.CursorTop; } catch { }
            Console.WriteLine("___________________");
            Console.WriteLine("WAL 모드 진행중 ~~");
            Console.WriteLine("Write - 쓴 개수: 0 / 0");
            Console.WriteLine("Write - 진행시간: 00:00:00.000");
            Console.WriteLine("Read  - 읽은 개수: 0");
            Console.WriteLine("Read  - 진행시간: 00:00:00.000");
            Console.WriteLine("___________________");

            Stopwatch sw2 = Stopwatch.StartNew();

            // Write 작업 수행
            string connectionString = $"Data Source={dbPathConcurrency};Version=3;Pooling=True;";
            using (var connection = new SQLiteConnection(connectionString))
            {
                connection.Open();

                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = "PRAGMA journal_mode=WAL;";
                    cmd.ExecuteNonQuery();
                    if (isNotUsedCheckPoint)
                    {
                        cmd.CommandText = "PRAGMA wal_autocheckpoint=0;";
                        cmd.ExecuteNonQuery();
                    }
                    cmd.CommandText = "PRAGMA synchronous=OFF;";
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

                            if (!Console.IsOutputRedirected) try { Console.SetCursorPosition(0, displayStartLine + 2); } catch { }
                            Console.WriteLine($"Write - 쓴 개수: {recordsWritten:N0} / {totalRecords:N0}".PadRight(60));
                            Console.WriteLine($"Write - 진행시간: {sw2.Elapsed:hh\\:mm\\:ss\\.fff} (WAL 크기: {currentWalSize / 1024.0:F1} KB)".PadRight(60));
                            Console.WriteLine($"Read  - 조회된 행: {totalReads:N0} (시도: {Interlocked.Read(ref queryCount):N0}회)".PadRight(60));
                            Console.WriteLine($"Read  - 진행시간: {swReader.Elapsed:hh\\:mm\\:ss\\.fff}".PadRight(60));
                        }
                    }
                }
            }

            sw2.Stop();

            // Reader가 완료될 때까지 콘솔 업데이트 계속 수행 (Write가 먼저 끝나도 Read 현황 갱신)
            while (!readerTask.IsCompleted)
            {
                long currentWalSize = 0;
                try
                {
                    string walPath = dbPathConcurrency + "-wal";
                    if (File.Exists(walPath)) currentWalSize = new FileInfo(walPath).Length;
                }
                catch { }

                if (!Console.IsOutputRedirected) try { Console.SetCursorPosition(0, displayStartLine + 2); } catch { }
                Console.WriteLine($"Write - 쓴 개수: {totalRecords:N0} / {totalRecords:N0}".PadRight(60));
                Console.WriteLine($"Write - 진행시간: {sw2.Elapsed:hh\\:mm\\:ss\\.fff} (WAL 크기: {currentWalSize / 1024.0:F1} KB)".PadRight(60));
                Console.WriteLine($"Read  - 조회된 행: {Interlocked.Read(ref totalReads):N0}".PadRight(60));
                Console.WriteLine($"Read  - 진행시간: {swReader.Elapsed:hh\\:mm\\:ss\\.fff}".PadRight(60));

                Thread.Sleep(50);
            }

            readerTask.Wait();
            swReader.Stop();

            // 🔥 Reader 최종 결과 강제 갱신 (화면에 100만 개가 찍히도록 보장)
            if (!Console.IsOutputRedirected) try { Console.SetCursorPosition(0, displayStartLine + 2); } catch { }
            Console.WriteLine($"Write - 쓴 개수: {totalRecords:N0} / {totalRecords:N0}".PadRight(60));
            Console.WriteLine($"Write - 진행시간: {sw2.Elapsed:hh\\:mm\\:ss\\.fff} (완료)       ".PadRight(60));
            Console.WriteLine($"Read  - 조회된 행: {Interlocked.Read(ref totalReads):N0}".PadRight(60));
            Console.WriteLine($"Read  - 진행시간: {swReader.Elapsed:hh\\:mm\\:ss\\.fff}".PadRight(60));

            Console.WriteLine();

            if (!Console.IsOutputRedirected) try { Console.SetCursorPosition(0, displayStartLine + 7); } catch { }
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

        static void RunPerformanceTest()
        {
            if (!Console.IsOutputRedirected) try { Console.Clear(); } catch { }
            Console.WriteLine("=== WAL vs Default 성능 비교 ===");
            Console.WriteLine();
            Console.Write("총 레코드 수 (기본 10000000): ");
            string input = Console.ReadLine();
            int totalRecords = string.IsNullOrEmpty(input) ? 10_000_000 : int.Parse(input);

            Console.Write("트랜잭션 당 레코드 수 (기본 1): ");
            input = Console.ReadLine();
            int recordsPerTransaction = string.IsNullOrEmpty(input) ? 1 : int.Parse(input);

            //Console.Write("업데이트 간격 (기본 10): ");
            //input = Console.ReadLine();
            //int updateInterval = recordsPerTransaction >= 10000 ? recordsPerTransaction : recordsPerTransaction * 10;
            int updateInterval = recordsPerTransaction >= 10000 ? recordsPerTransaction : recordsPerTransaction * 10;

            Console.WriteLine();
            Console.WriteLine($"설정: 총 {totalRecords:N0}개, 트랜잭션 당 {recordsPerTransaction:N0}개, 업데이트 {updateInterval:N0}개마다");
            Console.WriteLine();

            // Test1_Performance 폴더에 DB 파일 생성
            string testFolder = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Test1_Performance");
            if (Directory.Exists(testFolder)) Directory.Delete(testFolder, true);
            Directory.CreateDirectory(testFolder);

            string walDbPath = Path.Combine(testFolder, "wal.db");
            string DefaultDbPath = Path.Combine(testFolder, "Default.db");

            int walStartLine = 0;
            if (!Console.IsOutputRedirected) try { walStartLine = Console.CursorTop; } catch { }
            Console.WriteLine("___________________");
            Console.WriteLine("WAL 모드 준비중...");
            Console.WriteLine("쓴 개수: 0");
            Console.WriteLine("진행시간: 00:00:00");
            Console.WriteLine("___________________");
            Console.WriteLine();

            int DefaultStartLine = 0;
            if (!Console.IsOutputRedirected) try { DefaultStartLine = Console.CursorTop; } catch { }
            Console.WriteLine("___________________");
            Console.WriteLine("Default 모드 준비중...");
            Console.WriteLine("쓴 개수: 0");
            Console.WriteLine("진행시간: 00:00:00");
            Console.WriteLine("___________________");
            Console.WriteLine();

            Task walTask = Task.Run(() => WriteWithMode(walDbPath, "WAL", totalRecords, recordsPerTransaction, updateInterval,
                (count, elapsed) => UpdateConsole("WAL", count, totalRecords, elapsed, walStartLine),
                (elapsed) => UpdateConsoleFinal("WAL", totalRecords, elapsed, walStartLine)));

            Task.WaitAll(walTask);

            Task DefaultTask = Task.Run(() => WriteWithMode(DefaultDbPath, "Default", totalRecords, recordsPerTransaction, updateInterval,
                (count, elapsed) => UpdateConsole("Default", count, totalRecords, elapsed, DefaultStartLine),
                (elapsed) => UpdateConsoleFinal("Default", totalRecords, elapsed, DefaultStartLine)));

            Task.WaitAll(DefaultTask);

            if (!Console.IsOutputRedirected) try { Console.SetCursorPosition(0, DefaultStartLine + 6); } catch { }
            Console.WriteLine();
            Console.WriteLine("=== 테스트 완료 ===");
            Console.WriteLine("엔터키를 누르면 메뉴로 돌아갑니다...");
            Console.ReadLine();
        }

        static void RunSplitSimulationTest()
        {
            if (!Console.IsOutputRedirected) try { Console.Clear(); } catch { }
            Console.WriteLine("=== 분할 실행 시뮬레이션 (WAL 파일 생성 부하 확인) ===");
            Console.WriteLine();
            Console.Write("총 레코드 수 (기본 2000000): ");
            string input = Console.ReadLine();
            int totalRecords = string.IsNullOrEmpty(input) ? 2_000_000 : int.Parse(input);

            //Console.Write("트랜잭션 당 레코드 수 (기본 1): ");
            //input = Console.ReadLine();
            //int recordsPerTransaction = string.IsNullOrEmpty(input) ? 1 : int.Parse(input);

            //Console.Write("화면 디스플레이 업데이트 (기본 1000): ");
            //input = Console.ReadLine();
            //int updateInterval = string.IsNullOrEmpty(input) ? 1000 : int.Parse(input);


            Console.Write("분할 실행 횟수 (기본 100): ");
            input = Console.ReadLine();
            int splitCount = string.IsNullOrEmpty(input) ? 100 : int.Parse(input);

            int recordsPerTransaction = 1;
            int updateInterval = recordsPerTransaction >= 10000 ? recordsPerTransaction : recordsPerTransaction * 10;

            Console.WriteLine();
            Console.WriteLine($"설정: 총 {totalRecords:N0}개, 분할 {splitCount:N0}회 (DB당 {totalRecords / splitCount:N0}개), 트랜잭션 당 {recordsPerTransaction:N0}개");
            Console.WriteLine();

            // Test3_Split 폴더에 DB 파일 생성
            string testFolder = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Test3_Split");
            if (Directory.Exists(testFolder)) Directory.Delete(testFolder, true);
            Directory.CreateDirectory(testFolder);

            int walStartLine = 0;
            if (!Console.IsOutputRedirected) try { walStartLine = Console.CursorTop; } catch { }
            Console.WriteLine("___________________");
            Console.WriteLine("WAL 모드 준비중...");
            Console.WriteLine("쓴 개수: 0");
            Console.WriteLine("진행시간: 00:00:00");
            Console.WriteLine("___________________");
            Console.WriteLine();

            int DefaultStartLine = 0;
            if (!Console.IsOutputRedirected) try { DefaultStartLine = Console.CursorTop; } catch { }
            Console.WriteLine("___________________");
            Console.WriteLine("Default 모드 준비중...");
            Console.WriteLine("쓴 개수: 0");
            Console.WriteLine("진행시간: 00:00:00");
            Console.WriteLine("___________________");
            Console.WriteLine();

            int recordsPerSplit = totalRecords / splitCount;

            // WAL 분할 실행
            Task walTask = Task.Run(() =>
            {
                Stopwatch swTotal = Stopwatch.StartNew();
                for (int i = 0; i < splitCount; i++)
                {
                    string currentDbPath = Path.Combine(testFolder, $"wal_{i}.db");
                    int capturedIndex = i;
                    WriteWithMode(currentDbPath, "WAL", recordsPerSplit, recordsPerTransaction, updateInterval,
                        (count, elapsed) => UpdateConsole("WAL", (capturedIndex * recordsPerSplit) + count, totalRecords, swTotal.Elapsed, walStartLine),
                        null);
                }
                swTotal.Stop();
                UpdateConsoleFinal("WAL", totalRecords, swTotal.Elapsed, walStartLine);
            });

            Task.WaitAll(walTask);

            // Default 분할 실행
            Task DefaultTask = Task.Run(() =>
            {
                Stopwatch swTotal = Stopwatch.StartNew();
                for (int i = 0; i < splitCount; i++)
                {
                    string currentDbPath = Path.Combine(testFolder, $"Default_{i}.db");
                    int capturedIndex = i;
                    WriteWithMode(currentDbPath, "Default", recordsPerSplit, recordsPerTransaction, updateInterval,
                        (count, elapsed) => UpdateConsole("Default", (capturedIndex * recordsPerSplit) + count, totalRecords, swTotal.Elapsed, DefaultStartLine),
                        null);
                }
                swTotal.Stop();
                UpdateConsoleFinal("Default", totalRecords, swTotal.Elapsed, DefaultStartLine);
            });

            Task.WaitAll(DefaultTask);

            if (!Console.IsOutputRedirected) try { Console.SetCursorPosition(0, DefaultStartLine + 6); } catch { }
            Console.WriteLine();
            Console.WriteLine("=== 테스트 완료 ===");
            Console.WriteLine("엔터키를 누르면 메뉴로 돌아갑니다...");
            Console.ReadLine();
        }

        static void WriteWithMode(string dbPath, string mode, int totalRecords, int recordsPerTransaction, int updateInterval, Action<int, TimeSpan> onProgress, Action<TimeSpan> onComplete)
        {
            Stopwatch sw = Stopwatch.StartNew();
            // ✅ Pooling=False: 연결 풀링 비활성화 -> 매번 물리적 파일 생성/닫기 강제
            string connectionString = $"Data Source={dbPath};Version=3;Pooling=False;";

            // DB에 쓸 총 레코드 수보다 트랜잭션 당 레코드 수가 크면 조정 (안 그러면 0번 수행됨)
            if (recordsPerTransaction > totalRecords)
            {
                recordsPerTransaction = totalRecords;
            }
            if (recordsPerTransaction <= 0) recordsPerTransaction = 1;

            try
            {
                // DB 연결 (파일이 없으면 여기서 실제 생성됨)
                using (var connection = new SQLiteConnection(connectionString))
                {
                    connection.Open();

                    using (var cmd = connection.CreateCommand())
                    {
                        if (mode.Contains("WAL"))
                        {
                            cmd.CommandText = "PRAGMA journal_mode=WAL;";
                            cmd.ExecuteNonQuery();
                        }
                        else
                        {
                            cmd.CommandText = "PRAGMA journal_mode=MEMORY;";
                            cmd.ExecuteNonQuery();
                        }

                        // 모든 모드에서 Synchronous OFF (속도 최적화)
                        cmd.CommandText = "PRAGMA synchronous=OFF;";
                        cmd.ExecuteNonQuery();

                        // Checkpoint는 기본값 사용 (Auto Checkpoint 1000)
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

                            if (writtenCount % updateInterval == 0 || writtenCount == totalRecords)
                            {
                                onProgress?.Invoke(writtenCount, sw.Elapsed);
                            }
                        }
                    }

                    onComplete?.Invoke(sw.Elapsed);
                }

                sw.Stop();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"{mode} 모드 오류: {ex.Message}");
            }
        }

        static void UpdateConsole(string mode, int count, int total, TimeSpan elapsed, int startLine)
        {
            lock (consoleLock) // 멀티스레드 환경(Option 1)에서 콘솔 깨짐 방지를 위해 lock 필수
            {
                if (!Console.IsOutputRedirected)
                {
                    try
                    {
                        Console.SetCursorPosition(0, startLine);
                    }
                    catch { }
                }

                Console.WriteLine("___________________".PadRight(60));
                Console.WriteLine($"{mode} 모드 진행중 ~~".PadRight(60));
                Console.WriteLine($"쓴 개수: {count:N0} / {total:N0}".PadRight(60));
                Console.WriteLine($"진행시간: {elapsed:hh\\:mm\\:ss\\.ffffff}".PadRight(60));
                Console.WriteLine("___________________".PadRight(60));
            }
        }

        static void UpdateConsoleFinal(string mode, int total, TimeSpan elapsed, int startLine)
        {
            lock (consoleLock)
            {
                // try
                {
                    // Console.CursorVisible = false;
                    if (!Console.IsOutputRedirected)
                    {
                        try { Console.SetCursorPosition(0, startLine); } catch { }
                    }
                    Console.WriteLine("___________________".PadRight(60));
                    Console.WriteLine($"{mode} 모드 완료!".PadRight(60));
                    Console.WriteLine($"총 개수: {total:N0}".PadRight(60));
                    Console.WriteLine($"총 시간: {elapsed:hh\\:mm\\:ss\\.fff}".PadRight(60));
                    Console.WriteLine("___________________".PadRight(60));
                }
                // finally
                {
                    // Console.CursorVisible = true;
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
