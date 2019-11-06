using System;
using System.IO;
using Serilog;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Xml.Serialization;
using OsmSharp.Changesets;
using OsmSharp.Db.Tiled.Build;
using OsmSharp.Db.Tiled.Ids;
using OsmSharp.Db.Tiled.Replication;
using OsmSharp.Db.Tiled.Tiles;
using OsmSharp.Logging;
using OsmSharp.Streams;
using OsmSharp.Tags;

namespace OsmSharp.Db.Tiled.Tests.Functional
{
    class Program
    {
        static async Task Main(string[] args)
        {
//#if DEBUG
            if (args == null || args.Length == 0)
            {
                args = new string[]
                {
                    @"/data/work/data/OSM/wechel.osm.pbf",
                    @"/media/xivk/2T-SSD-EXT/replication-tests/",
                    @"12"
                };
            }
//#endif
            // TODO: implement a functional test handling a 404 on a missing sequence.
            
            // enable logging.
            OsmSharp.Logging.Logger.LogAction = (origin, level, message, parameters) =>
            {
                var formattedMessage = $"{origin} - {message}";
                switch (level)
                {
                    case "critical":
                        Log.Fatal(formattedMessage);
                        break;
                    case "error":
                        Log.Error(formattedMessage);
                        break;
                    case "warning":
                        Log.Warning(formattedMessage);
                        break; 
                    case "verbose":
                        Log.Verbose(formattedMessage);
                        break; 
                    case "information":
                        Log.Information(formattedMessage);
                        break; 
                    default:
                        Log.Debug(formattedMessage);
                        break;
                }
            };

            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Information()
                .WriteTo.Console()
                .WriteTo.File(Path.Combine("logs", "log-.txt"), rollingInterval: RollingInterval.Day)
                .CreateLogger();
            
            try
            {
                // validate arguments.
                if (args.Length < 3)
                {
                    Log.Fatal("Expected 4 arguments: input file db zoom");
                    return;
                }

                if (!File.Exists(args[0]))
                {
                    Log.Fatal("Input file not found: {0}", args[0]);
                    return;
                }

                if (!Directory.Exists(args[1]))
                {
                    Log.Fatal("Db directory doesn't exist: {0}", args[1]);
                    return;
                }

                if (!uint.TryParse(args[2], out var zoom))
                {
                    Log.Fatal("Can't parse zoom: {0}", args[2]);
                    return;
                }

                // try loading the db, if it doesn't exist build it.
                if (!OsmDb.TryLoad(args[1], out var db))
                {
                    Log.Information("The DB doesn't exist yet, building...");
                    var source = new OsmSharp.Streams.PBFOsmStreamSource(
                        File.OpenRead(args[0]));

                    // splitting tiles and writing indexes.
                    db = source.BuildDb(args[1], zoom);
                }
                
                // start catch up.
                var enumerator = new CatchupReplicationDiffEnumerator(db.Latest.Timestamp);
                while (await enumerator.MoveNext())
                {
                    var diff = await enumerator.Diff();

                    Log.Information($"Another diff {enumerator.State}: " +
                                    $"{diff.Create?.Length ?? 0}cre, " +
                                    $"{diff.Modify?.Length ?? 0}mod, " +
                                    $"{diff.Delete?.Length ?? 0}del");
                    Log.Information("Applying changes...");
                    
                    db.ApplyDiff(diff);
                    Log.Information($"Changes applied, new database: {db}");
                }
            }
            catch (Exception e)
            {
                Log.Fatal(e, "Unhandled exception.");
                throw;
            }
        }

        internal static DirectoryInfo BuildPath(string path, ReplicationState state)
        {
            var year = ("0000" + state.Timestamp.Year);
            year = year.Substring(year.Length - 4, 4);
            var month = ("00" + state.Timestamp.Month);
            month = month.Substring(month.Length - 2, 2);
            var day = ("00" + state.Timestamp.Day);
            day = day.Substring(day.Length - 2, 2);
            var hour = ("00" + state.Timestamp.Hour);
            hour = hour.Substring(hour.Length - 2, 2);
            var minute = ("00" + state.Timestamp.Minute);
            minute = minute.Substring(minute.Length - 2, 2);
            
            return new DirectoryInfo(Path.Combine(path, year, month, day, hour, minute));
        }
    }
}