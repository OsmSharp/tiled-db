using System;
using System.IO;
using System.Threading;
using Serilog;
using System.Threading.Tasks;
using OsmSharp.Db.Tiled.Build;
using OsmSharp.Db.Tiled.Replication;
using OsmSharp.Db.Tiled.Snapshots;
using OsmSharp.Db.Tiled.Snapshots.Build;
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
                    @"/data/work/data/OSM/luxembourg-latest.osm.pbf",
                    @"/media/xivk/2T-SSD-EXT/replication-tests/",
                    @"12"
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
                    var source = new PBFOsmStreamSource(
                        File.OpenRead(args[0]));

                    // splitting tiles and writing indexes.
                    db = source.BuildDb(args[1], zoom);
                }
                
                // start catch up until we reach hours/days.
                var catchupEnumerator = new CatchupReplicationDiffEnumerator(db.Latest.Timestamp.AddSeconds(1));
                ReplicationState latestState = null;
                while (await catchupEnumerator.MoveNext())
                {
                    if (catchupEnumerator.State.Config.IsHourly ||
                        catchupEnumerator.State.Config.IsDaily)
                    {
                        break;
                    }
                    
                    Log.Information($"Applying changes: {catchupEnumerator.State}");
                    await catchupEnumerator.ApplyCurrent(db);
                    Log.Information($"Changes applied, new database: {db}");

                    latestState = catchupEnumerator.State;
                }
                
                // start enumerator that follows.
                var enumerator = new ReplicationDiffEnumerator(Tiled.Replication.Replication.Hourly);
                var lastDay = db.Latest.Timestamp.Date;
                while (true)
                {
                    if (await enumerator.MoveTo(db.Latest.Timestamp))
                    {
                        Log.Information($"Applying changes: {enumerator.State}");
                        await db.ApplyDiff(enumerator);
                        Log.Information($"Changes applied, new database: {db}");

                        while (await enumerator.MoveNext())
                        {
                            if (lastDay != db.Latest.Timestamp.Date)
                            {
                                Log.Information($"A new day, taking snapshot.");
                                db.TakeSnapshot();
                                lastDay = db.Latest.Timestamp.Date;
                            }
                            
                            Log.Information($"Applying changes: {enumerator.State}");
                            await db.ApplyDiff(enumerator);
                            Log.Information($"Changes applied, new database: {db}");
                        }
                    }
                    
                    Thread.Sleep(10000);
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