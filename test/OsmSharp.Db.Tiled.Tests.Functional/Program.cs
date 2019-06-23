using System;
using System.IO;
using Serilog;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Xml.Serialization;
using OsmSharp.Changesets;
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
                    @"/media/xivk/2T-SSD-EXT/replication-tests/tiles-db/",
                    @"14"
                };
            }
//#endif
            
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
                    Log.Fatal("Expected 4 arguments: inputfile db zoom");
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

                var ticks = DateTime.Now.Ticks;
                var latest = DateTime.MinValue;
                if (!File.Exists(Path.Combine(args[1], "0", "0", "0.nodes.idx")))
                {
                    Log.Information("The tiled DB doesn't exist yet, rebuilding...");
                    var source = new OsmSharp.Streams.PBFOsmStreamSource(
                        File.OpenRead(args[0]));
                    
                    // add a filter and keep the last date.
                    var filtered = source.Select(x =>
                    {
                        if (x.TimeStamp != null)
                        {
                            if (latest < x.TimeStamp.Value) latest = x.TimeStamp.Value;
                        }

                        return x;
                    });
                    var progress = new OsmSharp.Streams.Filters.OsmStreamFilterProgress();
                    progress.RegisterSource(filtered);

                    // splitting tiles and writing indexes.
                    Build.Builder.Build(progress, args[1], zoom);
                }
                else
                {
                    //throw new Exception("Not rebuilding won't work, delete files.");
                }

                // create a database object that can read individual objects.
                Log.Information($"Loading database: {args[1]}");
                IDatabaseView db = new DatabaseSnapshot(args[1], new DatabaseMeta()
                {
                    Base = null,
                    Zoom = zoom
                });
                
                // start catch up.
                var catchUpEnumerator = Replication.Replication.GetCatchupDiffEnumerator(DateTime.Now.Date.AddHours(-2).AddMinutes(-2));
                while (await catchUpEnumerator.MoveNext())
                {
                    var diff = catchUpEnumerator.Current;

                    Log.Information($"Another diff {catchUpEnumerator.State}: " +
                                    $"{diff.Create?.Length ?? 0}cre, " +
                                    $"{diff.Modify?.Length ?? 0}mod, " +
                                    $"{diff.Delete?.Length ?? 0}del");
                    Log.Information("Applying changes...");

                    var fileName = $"/media/xivk/2T-SSD-EXT/replication-tests/tilesdb-";
                    if (catchUpEnumerator.Config.IsDaily)
                    {
                        fileName = fileName + "daily";
                    }
                    else if (catchUpEnumerator.Config.IsHourly)
                    {
                        fileName = fileName + "hourly";
                    }
                    else
                    {
                        fileName = fileName + "minutely";
                    }

                    fileName = fileName + $"-{catchUpEnumerator.State.SequenceNumber}/";
                    
                    db = db.ApplyChangeset(diff, fileName);
                    Log.Information($"Changes applied, new database: {db}");
                }
                
//                var config = ReplicationConfig.Hourly;
//                var enumerator = await config.GetDiffEnumerator(latest);
//                while (await enumerator.MoveNext())
//                {
//                    var diff = enumerator.Current;
//
//                    Log.Information($"Another diff {enumerator.State}: " +
//                                    $"{diff.Create?.Length ?? 0}cre,  {diff.Modify?.Length ?? 0}mod,  {diff.Delete?.Length ?? 0}del");
//                    Log.Information("Applying changes...");
//                    db = db.ApplyChangeset(diff, $"/media/xivk/2T-SSD-EXT/replication-tests/tilesdb-minutely-{enumerator.State.SequenceNumber}/");
//                    Log.Information($"Changes applied, new database: {db}");
//
//                    if (enumerator.IsLatest)
//                    {
//                        Log.Information($"Changes applied, new database: {db}");
//                        break;
//                    }
//                }
            }
            catch (Exception e)
            {
                Log.Fatal(e, "Unhandled exception.");
                throw;
            }
        }
    }
}