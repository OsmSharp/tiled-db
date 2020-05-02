using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using Serilog;
using System.Threading.Tasks;
using System.Xml.Serialization;
using OsmSharp.Changesets;
using OsmSharp.Db.Tiled.OsmTiled;
using OsmSharp.Db.Tiled.OsmTiled.Build;
using OsmSharp.Db.Tiled.OsmTiled.IO;
using OsmSharp.Db.Tiled.Tiles;
using OsmSharp.Logging;
using OsmSharp.Streams;

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
                    @"/data/work/data/OSM/lille-latest.osm.pbf",
                    @"/media/xivk/2T-SSD-EXT/replication-tests",
                    @"14"
                };
            }
//#endif
            // TODO: implement a functional test handling a 404 on a missing sequence.
            
            // enable logging.
            Logger.LogAction = (origin, level, message, parameters) =>
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

            var result = OsmTiledDbOperations.BuildDbPath("", new DateTime(2020, 04, 22, 20, 59, 02, DateTimeKind.Utc).ToUnixTime(),
                null, "full");
            
            
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Verbose()
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
                
                // prepare source.
                var source = new PBFOsmStreamSource(
                    File.OpenRead(args[0]));
                var progress = new OsmSharp.Streams.Filters.OsmStreamFilterProgress();
                progress.RegisterSource(source);
                
                var ticks = DateTime.Now.Ticks;
                // try loading the db, if it doesn't exist build it.
                if (!OsmTiledHistoryDb.TryLoad(args[1], out var db))
                {
                    Log.Information("The DB doesn't exist yet, building...");
                
                    // splitting tiles and writing indexes.
                    db = OsmTiledHistoryDb.Create(args[1], progress);
                }
                
                // write all tiles.
                Log.Information($"Took {new TimeSpan(DateTime.Now.Ticks - ticks).TotalSeconds}s");
                var latest = db.Latest;
                foreach (var tile in latest.GetModifiedTiles())
                {
                    Log.Information($" {latest.Get(tile, completeWays: false).Count()} objects in {tile}");
                }
            }
            catch (Exception e)
            {
                Log.Fatal(e, "Unhandled exception.");
                throw;
            }
        }
    }
}