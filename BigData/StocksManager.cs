using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace BigData
{
    class StocksManager
    {
        public List<string> GetStocks(int daysBackwarsd, List<string> stockNames)
        {
            var stocks = new List<string>();

            var path = @"..\..\Stocks\";
            
            DownloadStocks(daysBackwarsd, stockNames, path);

            var files = Directory.GetFiles(path);

            foreach (var file in files)
            {
                var finalLine = Path.GetFileNameWithoutExtension(file);

                var fileContent = File.ReadAllLines(file);

                for (int i = 1; i < fileContent.Length; i++)
			    {
			        var line = fileContent[i];

                    finalLine += "," + string.Join(",", line.Split(',').Take(5));
			    }

                stocks.Add(finalLine);
            }
            return stocks;
        }

        private static void DownloadStocks(int daysBackwarsd, List<string> stockNames, string path)
        {
            var backwerdsDate = DateTime.Now.Subtract(TimeSpan.FromDays(daysBackwarsd));
            var month = backwerdsDate.Month - 1;
            var day = backwerdsDate.Day;
            var year = backwerdsDate.Year;

            using (WebClient Client = new WebClient())
            {
                foreach (var stockName in stockNames)
                {
                    try
                    {
                        Client.DownloadFile("http://ichart.yahoo.com/table.csv?s=" + stockName + "&a=" + month + "&b=" + day + "&c=" + year, path + stockName + ".csv");
                    }
                    catch (Exception e)
                    {

                    }
                }
            }
        }
    }
}
