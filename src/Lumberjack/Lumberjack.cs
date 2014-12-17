using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Serilog;
using ServiceStack;
using ServiceStack.Text;

namespace Lumberjack
{
    public class Lumberjack
    {
        private readonly string _location ;
        private FileInfo _lastFile;
        private int _lastLine;
        private string createIndexUrlFmt;
        public Lumberjack()
        {
            JsConfig.AlwaysUseUtc = true;
            JsConfig.AssumeUtc = true;
            JsConfig.DateHandler = DateHandler.ISO8601;
            var directoryInfo = new FileInfo(Assembly.GetCallingAssembly().Location).Directory;
            if (directoryInfo != null)
            {
                _location = directoryInfo.FullName;
            }
            createIndexUrlFmt = Config.ElasticSearchUrl + ":9200/{_index}/{_type}/_bulk";
        }

        public void Process()
        {
            var files = Directory
                .GetFiles(_location, "*.log")
                .Select(x => new FileInfo(x))
                .OrderBy(x => x.CreationTimeUtc);
            if (!files.Any())
            {
                Log.Information("No *.log files found to process");
            }
            Parallel.ForEach(files, file =>
            {
                var originalFileInfo = new FileInfo(file.FullName);
                try
                {
                    Log.Verbose("Locking file {File}", file.Name);
                    var newFilename = file.FullName + ".lock";
                    File.Move(file.FullName, newFilename);
                    if (File.Exists(newFilename))
                    {
                        file = new FileInfo(file.FullName + ".lock");
                    }
                    else
                    {
                        throw new Exception("File didn't exist after move opertation");
                    }

                }
                catch (Exception x)
                {
                    Log.Error(x, "Error in locking file");
                    return;
                }
                var data = GetData(file);
                //group by date
                var indicies = data.GroupBy(x => x.Item1.Date)
                    .Select(g => Tuple.Create(g.Key, g.Select(x => x.Item2).ToList()));
                    var error = false;

                foreach (var index in indicies)
                {
                    var bulk = "";
                    var esIndex = Config.IndexFormat.Replace("{date}", index.Item1.ToString("yyyy.MM.dd"));
                    Log.Verbose("Using ElasticSearch index {Index}", esIndex);
                    int count = 0;
                    var total = 0;
                    foreach (var doc in index.Item2)
                    {
                        var jsDoc = doc.ToJson();
                        bulk += "{ \"index\" : { \"_index\" : \"" + esIndex + "\", \"_type\" : \"iis\" } }\n";
                        bulk += jsDoc + "\n";
                        count++;
                        total++;
                        if (count >= 1000)
                        {
                            Log.Verbose("Uploading items to elasticsearch");
                            try
                            {
                                var respo = createIndexUrlFmt
                                    .Replace("{_index}", esIndex)
                                    .Replace("{_type}", "iis")
                                    .PostJsonToUrl(bulk);
                            }
                            catch (Exception x)
                            {
                                Log.Error(x, "Error uploading {File} : {Count} to elasticsearch", file.Name, count);
                                error = true;
                            }

                            //read the response and see if anything failed eventually :(
                            Log.Verbose("{TotalItems} items processed", total);
                            count = 0;
                            bulk = "";
                        }
                    }
                    if (!String.IsNullOrEmpty(bulk))
                    {
                        Log.Verbose("Uploading items to elasticsearch");
                        try
                        {
                            var respo = createIndexUrlFmt
                                .Replace("{_index}", esIndex)
                                .Replace("{_type}", "iis")
                                .PostJsonToUrl(bulk);
                        }
                        catch (Exception x)
                        {
                            Log.Error(x, "Error uploading {File} : {Count} to elasticsearch", file.Name, count);
                            error = true;
                        }

                        Log.Verbose("{TotalItems} items processed", total);
                    }

                }
                Log.Verbose("Finished processing file {@File}", file.Name);

                var newLocation = error
                    ? Path.Combine(Config.ProcessedDirectory, Path.GetFileName(originalFileInfo.Name))
                    : Path.Combine(Config.ProcessedDirectory, Path.GetFileName(originalFileInfo.Name + ".error"));
                File.Move(file.FullName, newLocation);
                Log.Verbose("Moved file to {FileName}", newLocation);
            });
        }

        public List<Tuple<DateTime, Dictionary<string, string>>> GetData(FileInfo file)
        {
            Log.Verbose("Processing file {FileName}", file.FullName);
            _lastFile = file;

            var data = new List<Tuple<DateTime, Dictionary<string, string>>>();
            using (var stream = new StreamReader(file.FullName))
            {
                _lastLine = 0;
                var line = default(string);
                var fields = default(string[]);
                while ((line = stream.ReadLine()) != null)
                {
                    if (line.StartsWith("#Fields:"))
                    {
                        fields = line.Substring("#Fields: ".Length)
                            .Split(new[] {" "}, StringSplitOptions.None);

                        _lastLine++;
                        continue;
                    }

                    if (fields == default(string[]) || line.StartsWith("#"))
                    {
                        _lastLine++;
                        continue;
                    }

                    var values = line.Split(new[] {" "}, StringSplitOptions.None);

                    if (values.Length != fields.Length)
                    {
                        _lastLine++;
                        continue;
                    }

                    var dictionary = fields
                        .Zip(values, Tuple.Create)
                        .ToDictionary(x => x.Item1, x => x.Item2);

                    var ts = HandleTimstamp(ref dictionary);
                    HandleEnrich(ref dictionary);

                    data.Add(Tuple.Create(ts,dictionary));

                    _lastLine ++;
                }
            }
            Log.Verbose("File read complete with {Rows}", data.Count);
            return data;
        }

        public static void HandleEnrich(ref Dictionary<string, string> dictionary)
        {
            if (Config.EnrichWith != null && Config.EnrichWith.Any())
            {
                dictionary = dictionary
                    .Concat(Config.EnrichWith)
                    .ToDictionary(x => x.Key, x => x.Value);
            }
        }
        private static DateTime HandleTimstamp(ref Dictionary<string, string> dictionary)
        {
            if (dictionary.ContainsKey(Config.DateField) && dictionary.ContainsKey(Config.TimeField))
            {
                var date = DateTime.ParseExact(dictionary[Config.DateField], Config.DateFormat,
                    CultureInfo.InvariantCulture);
                var time = TimeSpan.Parse(dictionary[Config.TimeField]);

                dictionary.Remove(Config.DateField);
                dictionary.Remove(Config.TimeField);
                dictionary.Add("@timestamp", (date + time).ToString("O"));
                return date + time;
            }
            throw new Exception("Couldn't parse timestamp");
        }
    }
}