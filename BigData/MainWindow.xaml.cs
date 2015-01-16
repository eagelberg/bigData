using Renci.SshNet;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;

namespace BigData
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        private const string HOST = "192.168.17.148";
        private const string USER = "training";
        private const string PASSWORD = "training";
        private const string STOCKS_TO_SEND_FOLDER = @"..\..\StocksToSend\";
        private const string PROJECT_FOLDER = @"..\..\ProjectFolder\";
        private const string REMOTE_FOLDER = @"/home/training/winInput";
        private const string OUTPUT_FOLDER = @"..\..\Output";

        public MainWindow()
        {
            //var x = File.ReadLines(@"C:\Users\guy\Desktop\a.txt");
            //var stocks = new List<string>();
            //foreach (var item in x)
            //{
            //    var i = item.IndexOf("|");
            //    stocks.Add(item.Substring(0, i));
            //}
            //File.WriteAllLines(@"C:\Users\guy\Desktop\b.txt", stocks);
            InitializeComponent();
        }

        private void Button_Click(object sender, RoutedEventArgs e)
        {
            var stocksNames = File.ReadAllLines(@"..\..\stocksList.txt").Take(int.Parse(numOfStocksTextBox.Text)).ToList();
            var stockManager = new StocksManager();            
            var stocks = stockManager.GetStocks(int.Parse(daysTextBox.Text), stocksNames);

            SaveStocksIn10StocksPerFile(stocks);
            SendAndRecieveDataFromCloudera();
            ShowOutput();
        }

        private void ShowOutput()
        {
            throw new NotImplementedException();
        }

        private void SendAndRecieveDataFromCloudera()
        {
            using (var scpClient = new ScpClient(HOST, USER, PASSWORD))
            using (var sshClient = new SshClient(HOST, USER, PASSWORD))
            {
                scpClient.Connect();
                sshClient.Connect();

                var stocksToSend = Directory.GetFiles(STOCKS_TO_SEND_FOLDER);
                var projectFiles = Directory.GetFiles(PROJECT_FOLDER);
                var files = stocksToSend.Concat(projectFiles);

                SendFilesToCloudera(scpClient, files);
                
                // Creating input folder in hadoop file system
                sshClient.RunCommand("hadoop fs -mkdir /user/ex3");
                sshClient.RunCommand("hadoop fs -mkdir /user/ex3/input");

                // Put file in hadoop fs
                sshClient.RunCommand("hadoop fs -put " + REMOTE_FOLDER + "/* /user/ex3/input");

                // Creating jar file from the application classes
                sshClient.RunCommand("javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop/client-0.20/* -d ex3Folder Exr3.java MyKey.java");
                sshClient.RunCommand("jar -cvf ex3.jar -C ex3Folder/ .");

                // Executing jar by scpecifying input and output directories
                sshClient.RunCommand("hadoop jar ex3.jar ex.Exr3 /user/ex3/input /user/ex3/output");

                // Copy output file from hadoop fs to local file system
                sshClient.RunCommand("hadoop fs -copyToLocal /user/ex3/output/part-00000 ex3Folder");

                // Copy output file file from remote machine to windows
                scpClient.Download("/home/training/ex3Folder/part-00000", new DirectoryInfo(OUTPUT_FOLDER));
            }
        }

        private static void SendFilesToCloudera(ScpClient scpClient, IEnumerable<string> files)
        {
            foreach (string filePath in files)
            {
                var fileInfo = new FileInfo(filePath);
                scpClient.Upload(fileInfo, REMOTE_FOLDER + System.IO.Path.GetFileName(filePath));
            }
        }

        private static void SaveStocksIn10StocksPerFile(List<string> stocks)
        {
            var stocksToSave = new List<string>();

            for (int i = 0; i < stocks.Count; i++)
            {
                if (i % 10 == 0 && (i > 0))
                {
                    File.WriteAllLines(STOCKS_TO_SEND_FOLDER + "stocks" + i / 10 + ".txt", stocksToSave);
                    stocksToSave.Clear();
                }
                else
                {
                    stocksToSave.Add(stocks[i]);
                }
            }
        }
    }
}
