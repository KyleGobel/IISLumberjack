using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Serilog;
using ServiceStack.Text;

namespace Lumberjack
{
    class Program
    {
        static void Main(string[] args)
        {
            ConfigHelper.ReadConfig();
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Verbose()
                .WriteTo.RollingFile(Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                    "{Date}-lumberjack.log"))
                .WriteTo.ColoredConsole()
                .CreateLogger();

            Log.Verbose("Starting lumberjack");

            var app = new Lumberjack();
            app.Process();
        }
    }

    public class ConfigHelper
    {
        private const string ConfigFile = "lumberjack.json";
        public static void ReadConfig()
        {
            if (File.Exists(ConfigFile))
            {
                var str = File.ReadAllText(ConfigFile);
                var json = JsonObject.Parse(str);

                Config.DateField = json.Get("date_field");
                Config.DateFormat = json.Get("date_format");
                Config.ElasticSearchUrl = json.Get("elasticsearch_url");
                Config.EnrichWith = json.Get<Dictionary<string, string>>("enrich_with");
                Config.IndexFormat = json.Get("index_format");
                Config.TimeField = json.Get("time_field");
                Config.ProcessedDirectory = json.Get("processed_directory");

                Log.Information("Config file loaded");
            }
            else
            {
                Log.Fatal("No config file found at {ConfigFilePath}", ConfigFile);
                Environment.Exit(-1);
            }
        }
    }

    public static class Config
    {
        public static Dictionary<string,string> EnrichWith { get; set; } 
        public static string ElasticSearchUrl { get; set; }
        public static string IndexFormat { get; set; }
        public static string DateField { get; set; }
        public static string TimeField { get; set; }
        public static string DateFormat { get; set; }
        public static string ProcessedDirectory { get; set; }
    }
}
