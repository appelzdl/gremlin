using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.IO;
using CsvHelper;

namespace GraphGetStarted
{
    public class EmailData
    {
        public List<dynamic> GetCSV()
        {
            string path = @"C:\Users\DavidAppel\Documents\Anvesa\email-rows-II.csv";

            FileInfo file = new FileInfo(path);
            var reader = new CsvReader(new StringReader(file.OpenText().ReadToEnd()));

            var records = reader.GetRecords<dynamic>();
            return records.ToList();
        }
    }
}
