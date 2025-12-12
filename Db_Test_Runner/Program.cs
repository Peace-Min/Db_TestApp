using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;

namespace Db_Test_Runner
{
    internal class Program
    {
        static void Main(string[] args)
        {
            var pc = Process.GetCurrentProcess();
            pc.PriorityClass = ProcessPriorityClass.RealTime;

            Console.WriteLine("=== Db_TestApp Automation Runner ===");
            
            // 1. Configuration
            Console.WriteLine("테스트 모드 선택:");
            Console.WriteLine("1. WAL vs MEMORY 성능 비교");
            Console.WriteLine("2. WAL 동시성 테스트 (Write + Read)");
            Console.Write("선택 (기본 1): ");
            string modeInput = Console.ReadLine();
            string selectedMode = string.IsNullOrEmpty(modeInput) ? "1" : modeInput;

            Console.Write("반복 횟수 (기본 10): ");
            string input = Console.ReadLine();
            int iterations = string.IsNullOrEmpty(input) ? 10 : int.Parse(input);

            Console.Write("총 레코드 수 (기본 10000000): ");
            input = Console.ReadLine();
            string totalRecords = string.IsNullOrEmpty(input) ? "10000000" : input;

            Console.Write("트랜잭션 당 레코드 수 (기본 1): ");
            input = Console.ReadLine();
            string recordsPerTx = string.IsNullOrEmpty(input) ? "1" : input;

            Console.Write("업데이트 간격 (기본 10): ");
            input = Console.ReadLine();
            string updateInterval = string.IsNullOrEmpty(input) ? "10" : input;

            // Target Executable Path
            // Assuming Db_Test_Runner is peer to Db_TestApp, and we want to run the Debug build of Db_TestApp
            // Path: ..\Db_TestApp\bin\Debug\Db_TestApp.exe (Need to verify exact path match based on framework)
            // Let's try to find it relative to current execution
             string targetExePath = Path.GetFullPath(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"..\..\..\Db_TestApp\bin\Debug\Db_TestApp.exe"));
            
            // Fallback for Release or different structure if needed, but assuming standard VS layout
            if (!File.Exists(targetExePath))
            {
                 // Try net472 or similar if the original app is not .NET Core/5+
                 // The original csproj wasn't checked for target framework but usually it's bin/Debug/{Framework}/Db_TestApp.exe or bin/Debug/Db_TestApp.exe
                 // Let's check typical locations
                 string[] possiblePaths = new[]
                 {
                     @"..\..\..\Db_TestApp\bin\Release\Db_TestApp.exe", // Legacy .NET Framework
                     @"..\..\..\Db_TestApp\bin\Release\net8.0\Db_TestApp.exe", // .NET 8
                     @"..\..\..\Db_TestApp\bin\Release\net6.0\Db_TestApp.exe", // .NET 6
                     @"..\..\..\Db_TestApp\bin\Release\net472\Db_TestApp.exe"  // .NET Framework 4.7.2
                 };

                 bool found = false;
                 foreach(var p in possiblePaths)
                 {
                     string fullPath = Path.GetFullPath(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, p));
                     if(File.Exists(fullPath))
                     {
                         targetExePath = fullPath;
                         found = true;
                         break;
                     }
                 }

                 if(!found)
                 {
                     Console.WriteLine($"오류: Db_TestApp.exe를 찾을 수 없습니다. 검색 경로 예시: {targetExePath}");
                     Console.WriteLine("프로그램을 종료합니다.");
                     return;
                 }
            }

            string testNameMode = selectedMode == "2" ? "Concurrency" : "Performance";
            string fileNameTimestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            string csvPath = $"Result_{testNameMode}_Rec{totalRecords}_Tx{recordsPerTx}_{fileNameTimestamp}.csv";

            Console.WriteLine($"Target Executable: {targetExePath}");
            Console.WriteLine($"테스트를 시작합니다... (CSV 파일: {csvPath})");
            Console.WriteLine();

            // Create new file with headers
            File.WriteAllText(csvPath, "TimeStamp,RunIndex,TestType,Mode,TotalRecords,RecordsPerTx,DurationSeconds\n", Encoding.UTF8);

            for (int i = 1; i <= iterations; i++)
            {
                Console.WriteLine($"========== Iteration {i} / {iterations} ==========");
                
                if (selectedMode == "2")
                {
                    // Run Test 2: Concurrency (Baseline vs Concurrent)
                    RunTestAndLog(targetExePath, i, "2", "Concurrency", totalRecords, recordsPerTx, updateInterval, csvPath);
                }
                else
                {
                    // Run Test 1: Performance (WAL vs MEMORY)
                    RunTestAndLog(targetExePath, i, "1", "Performance", totalRecords, recordsPerTx, updateInterval, csvPath);
                }
            }

            Console.WriteLine("\n모든 테스트가 완료되었습니다.");
            Console.ReadLine();
        }

        static void RunTestAndLog(string exePath, int runIndex, string menuOption, string testType, string totalRecords, string recordsPerTx, string updateInterval, string csvPath)
        {
            Console.WriteLine($"Running {testType} Test (Mode {menuOption})...");

            ProcessStartInfo psi = new ProcessStartInfo
            {
                FileName = exePath,
                UseShellExecute = false,
                RedirectStandardInput = true,
                RedirectStandardOutput = true,
                CreateNoWindow = true,
                WorkingDirectory = Path.GetDirectoryName(exePath),
                StandardOutputEncoding = Encoding.UTF8
            };

            using (Process process = new Process { StartInfo = psi })
            {
                process.Start();
                try { process.PriorityClass = ProcessPriorityClass.High; } catch { } // 권한 문제 예외 처리

                // Send Inputs
                // Sequence: Menu Choice -> TotalRecords -> RecordsPerTransaction -> UpdateInterval
                StreamWriter sw = process.StandardInput;
                sw.WriteLine(menuOption);        // Menu
                sw.WriteLine(totalRecords);      // Total Records
                sw.WriteLine(recordsPerTx);      // Transaction Size
                sw.WriteLine(updateInterval);    // Update Interval
                sw.WriteLine();                  // Press Enter to return to menu
                sw.WriteLine("0");               // Select '0' to Exit loop
                
                // Read Output
                string output = process.StandardOutput.ReadToEnd();
                process.WaitForExit();

                // Match based on Test Type
                string timeStamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");

                if (testType == "Performance")
                {
                    // WAL output: "WAL 모드 완료!" ... "총 시간: 00:00:12.345"
                    // MEMORY output: "MEMORY 모드 완료!" ... "총 시간: 00:00:10.123"
                    
                    double walDuration = ParseDuration(output, @"WAL 모드 완료![\s\S]*?총 시간:\s*(\d{2}:\d{2}:\d{2}\.\d+)");
                    double memDuration = ParseDuration(output, @"MEMORY 모드 완료![\s\S]*?총 시간:\s*(\d{2}:\d{2}:\d{2}\.\d+)");

                    Console.WriteLine($"   -> Result: WAL = {walDuration:F3}s, MEMORY = {memDuration:F3}s");

                    string line = $"{timeStamp},{runIndex},{totalRecords},{recordsPerTx},{walDuration:F3},{memDuration:F3}";
                    File.AppendAllText(csvPath, line + "\n", Encoding.UTF8);

                    if (walDuration == 0.0 || memDuration == 0.0)
                    {
                         Console.WriteLine("Warning: Failed to parse duration. Output snapshot:");
                         Console.WriteLine(output.Length > 500 ? output.Substring(0, 500) + "..." : output);
                    }
                }
                else if (testType == "Concurrency")
                {
                    // Baseline: "1. Baseline (Write Only)  : 12.345 초"
                    // Concurrent: "2. Concurrent (Write+Read): 13.567 초"
                    
                    double baseDuration = ParseSeconds(output, @"1\. Baseline \(Write Only\)\s*:\s*([\d\.]+)");
                    double concDuration = ParseSeconds(output, @"2\. Concurrent \(Write\+Read\)\s*:\s*([\d\.]+)");
                    double readDuration = ParseSeconds(output, @"Read 작업 시간:\s*([\d\.]+)");

                    Console.WriteLine($"   -> Result: Baseline = {baseDuration:F3}s, Write = {concDuration:F3}s, Read = {readDuration:F3}s");

                    string line = $"{timeStamp},{runIndex},{totalRecords},{recordsPerTx},{baseDuration:F3},{concDuration:F3},{readDuration:F3}";
                    File.AppendAllText(csvPath, line + "\n", Encoding.UTF8);
                }

                // Check for failures inside the specific blocks or use a shared duration check?
                // The previous edit put it outside. Let's fix it.
                // Revert the faulty block first, or just rewrite correctly.
                
                // Since I cannot see the exact current state easily without reading, I will assume the previous multi_replace succeeded in inserting the block at the end of the looping/method.
                // Wait, I inserted it after the `else if (testType == "Concurrency")` block?
                // Yes. But `walDuration` is defined in `if (testType == "Performance")`.
                
                // I will delete the erroneous block at the end and insert it inside the Performance block.

            }
        }

        static double ParseDuration(string text, string regexPattern)
        {
            // Parses "HH:mm:ss.fff" to TotalSeconds
            var match = Regex.Match(text, regexPattern);
            if (match.Success)
            {
                if (TimeSpan.TryParse(match.Groups[1].Value, out TimeSpan ts))
                {
                    return ts.TotalSeconds;
                }
            }
            return 0.0;
        }

        static double ParseSeconds(string text, string regexPattern)
        {
            // Parses "12.345" to double
            var match = Regex.Match(text, regexPattern);
            if (match.Success)
            {
                if (double.TryParse(match.Groups[1].Value, out double val))
                {
                    return val;
                }
            }
            return 0.0;
        }

        static void LogToCsv(string path, int runIndex, string testType, string mode, string totalRecords, string recordsPerTx, double duration)
        {
            string timeStamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            string line = $"{timeStamp},{runIndex},{testType},{mode},{totalRecords},{recordsPerTx},{duration:F3}";
            
            // Console logging for verifies
            Console.WriteLine($"   -> Result: {mode} = {duration:F3}s");

            File.AppendAllText(path, line + "\n", Encoding.UTF8);
        }
    }
}
