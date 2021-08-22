using KBCsv;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace ParsCSVFromAba
{
    class Program
    {
        public const char SEPARATOR = ';';
        static async Task Main(string[] args)
        {
            var csvOutputStrings = new List<CSVRecord>();
            using (var streamReader = new StreamReader("inputs/quantity.csv"))
            using (var csvReader = new CsvReader(streamReader))
            {
                csvReader.ValueSeparator = SEPARATOR;
                csvReader.ReadHeaderRecord();

                var totalItems = 0;
                var matchedItems = 0;

                while (csvReader.HasMoreRecords)
                {
                    var record = csvReader.ReadDataRecord();
                    DataRecord priceRecord = null;
                    if (String.IsNullOrEmpty(record["Kod_IC"])) 
                    {
                        continue;
                    }
                    await Task.Run(() => {
                        priceRecord = FindPriceOrNull(record["Kod_IC"]);
                    });
                    CSVRecord csvString = null;
                    if (priceRecord != null)
                    {
                        csvString = new CSVRecord(record["Kod_IC"], record["No_IC"], record["No_TD"], record["Manufacturer"], record["Q_ty"], priceRecord["WHOLESALE_NET_PRICE"]);
                        matchedItems++;
                    }
                    else {
                        csvString = new CSVRecord(record["Kod_IC"], record["No_IC"], record["No_TD"], record["Manufacturer"], record["Q_ty"], null);
                    }

                    Write(csvString);

                    totalItems++;
                    Console.Clear();
                    Console.WriteLine($"Matching....({matchedItems}/{totalItems}) \n Written ({totalItems})");
                    
                }
            }

            Console.Beep();
            Console.WriteLine("Finished");

        }
        private static void Write(CSVRecord csvString) 
        {
            using (var streamWriter = new StreamWriter("outputs/quantity_price.csv", true))
            using (var writer = new CsvWriter(streamWriter))
            {
                writer.ForceDelimit = true;
               
                
                writer.WriteRecordAsync(csvString.Kod_IC, csvString.No_IC, csvString.No_TD, csvString.Manufacturer, csvString.Q_ty, csvString.Price).GetAwaiter().GetResult();
               
                
            }
        }
        private static  DataRecord FindPriceOrNull(string Kod_IC) 
        {
            using (var streamReader = new StreamReader("inputs/price.csv"))
            using (var csvReader = new CsvReader(streamReader))
            {
                csvReader.ValueSeparator = SEPARATOR;
                csvReader.ReadHeaderRecord();
               
                while (csvReader.HasMoreRecords)
                {
                    var record = csvReader.ReadDataRecord();
                    if (record["ID"].ToLower().Equals(Kod_IC.ToLower())) 
                    {
                        return record;
                    }
                    
                }
            }
            return null;
        }

        private class CSVRecord
        {
            public CSVRecord(string kod_IC, string no_IC, string no_TD, string manufacturer, string q_ty, string price)
            {
                Kod_IC = kod_IC ?? throw new ArgumentNullException(nameof(kod_IC));
                No_IC = no_IC;
                No_TD = no_TD;
                Manufacturer = manufacturer;
                Q_ty = q_ty;
                Price = price;
            }

            internal string Kod_IC {get;set;}
            internal string No_IC { get; set; }
            internal string No_TD { get; set; }
            internal string Manufacturer { get; set; }
            internal string Q_ty { get; set; }
            internal string Price { get; set; }


        }
    }
}
