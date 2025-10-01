using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Db_TestApp
{
    internal class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Db_TestApp 시작: WriteApp과 ReadApp 동시 실행 테스트");

            try
            {
                // WriteApp 실행
                Process writeProc = new Process();
                writeProc.StartInfo.FileName = "Db_WriteApp.exe"; // 실행 파일 경로
                writeProc.StartInfo.UseShellExecute = false;
                writeProc.StartInfo.RedirectStandardOutput = true;
                writeProc.OutputDataReceived += (s, e) =>
                {
                    if (!string.IsNullOrEmpty(e.Data))
                        Console.WriteLine("[WriteApp] " + e.Data);
                };
                // 시작
                writeProc.Start();
                writeProc.BeginOutputReadLine();
                // 최대 45초 기다림 (Write와 Read가 30초 수행)
                writeProc.WaitForExit(45000);

                Task.Delay(2000);
                for (int r = 0; r < 20; r++)
                {
                    Process readProc = new Process();
                    readProc.StartInfo.FileName = "Db_ReadApp.exe";
                    readProc.StartInfo.Arguments = r.ToString();   // ← 프로세스 인덱스 전달
                    readProc.StartInfo.UseShellExecute = false;
                    readProc.StartInfo.RedirectStandardOutput = true;
                    int id = r;
                    readProc.OutputDataReceived += (s, e) =>
                    {
                        if (!string.IsNullOrEmpty(e.Data))
                            Console.WriteLine("[ReadApp{0}] {1}", id, e.Data);
                    };
                    readProc.Start();
                    readProc.BeginOutputReadLine();
                }

                //// ReadApp 실행
                //Process readProc = new Process();
                //readProc.StartInfo.FileName = "Db_ReadApp.exe"; // 실행 파일 경로
                //readProc.StartInfo.UseShellExecute = false;
                //readProc.StartInfo.RedirectStandardOutput = true;
                //readProc.OutputDataReceived += (s, e) =>
                //{
                //    if (!string.IsNullOrEmpty(e.Data))
                //        Console.WriteLine("[ReadApp] " + e.Data);
                //};



                //// 약간 지연 후 Read 시작 (Write가 DB 생성 먼저 해야 하므로 200ms 대기)
                //readProc.Start();
                //readProc.BeginOutputReadLine();

                
                //readProc.WaitForExit(35000);

                Console.WriteLine("동시 실행 테스트 종료");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Db_TestApp 실행 중 오류: " + ex.Message);
            }

            Console.WriteLine("엔터키를 누르면 종료합니다...");
            Console.ReadLine();
        }
    }
}
