using System;
using System.Collections.Generic;
using System.IO;
using Serilog;
using System.Threading.Tasks;
using OsmSharp.Changesets;
using OsmSharp.Db.Tiled.OsmTiled.IO;
using OsmSharp.Logging;
using OsmSharp.Replication;
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
                    @"/data/work/data/OSM/belgium-latest.osm.pbf",
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
            Logging.Log.LogAction = (type, message) =>
            {
                switch (type)
                {
                    case OsmSharp.Db.Tiled.Logging.TraceEventType.Critical:
                        Log.Fatal(message);
                        break;
                    case OsmSharp.Db.Tiled.Logging.TraceEventType.Error:
                        Log.Error(message);
                        break;
                    case OsmSharp.Db.Tiled.Logging.TraceEventType.Warning:
                        Log.Warning(message);
                        break;
                    case OsmSharp.Db.Tiled.Logging.TraceEventType.Verbose:
                        Log.Verbose(message);
                        break;
                    case OsmSharp.Db.Tiled.Logging.TraceEventType.Information:
                        Log.Information(message);
                        break;
                    default:
                        Log.Debug(message);
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
                var progress = new Streams.Filters.OsmStreamFilterProgress();
                progress.RegisterSource(source);
                
                var ticks = DateTime.Now.Ticks;
                // try loading the db, if it doesn't exist build it.
                if (!OsmTiledHistoryDb.TryLoad(args[1], out var db))
                {
                    Log.Information("The DB doesn't exist yet, building...");
                
                    // splitting tiles and writing indexes.
                    db = OsmTiledHistoryDb.Create(args[1], progress);
                }
                
                
                if ((DateTime.Now.ToUniversalTime() - db.Latest.EndTimestamp).TotalHours > 1)
                {
                    // the data is pretty old, update per hour.
                    var hourEnumerator = await ReplicationConfig.Hourly.GetDiffEnumerator(db.Latest);
                    if (hourEnumerator != null)
                    {
                        while (await hourEnumerator.MoveNext())
                        {
                            ticks = DateTime.Now.Ticks;
                            var previousLatest = db.Latest.EndTimestamp;
                            Log.Verbose($"Downloading diff: {hourEnumerator.State}");
                            var diff = await hourEnumerator.Diff();
                            if (diff == null) continue;
                     
                            var latestStatus = hourEnumerator.State;

                            // squash changes.
                            Log.Verbose($"Squashing changes...");
                            var changeSet = new [] { diff }.Squash();

                            // build meta data.
                            var metaData = new List<(string key, string value)>
                            {
                                ("period", latestStatus.Config.Period.ToString()),
                                ("sequence_number", latestStatus.SequenceNumber.ToString())
                            };

                            // apply diff.
                            Log.Information($"Applying changes...");
                            db.ApplyDiff(changeSet, latestStatus.EndTimestamp, metaData);
                            Log.Information($"Took {new TimeSpan(DateTime.Now.Ticks - ticks).TotalSeconds}s");

                            if (previousLatest.Day != db.Latest.EndTimestamp.Day)
                            {
                                // an data was skipped, take a snapshot.
                                ticks = DateTime.Now.Ticks;
                                Log.Information($"Taking snapshot...");
                                db.TakeDiffSnapshot(null, TimeSpan.FromDays(1), metaData);
                                Log.Information($"Snapshot took {new TimeSpan(DateTime.Now.Ticks - ticks).TotalSeconds}s");
                            }
                        }
                    }
                }
                
                // the data is pretty recent, start doing minutes, do as much as available.
                var minuteEnumerator = await ReplicationConfig.Minutely.GetDiffEnumerator(db.Latest);
                if (minuteEnumerator != null)
                {
                    while (await minuteEnumerator.MoveNext())
                    {
                        ticks = DateTime.Now.Ticks;
                        var previousLatest = db.Latest.EndTimestamp;
                        Log.Verbose($"Downloading diff: {minuteEnumerator.State}");
                        var diff = await minuteEnumerator.Diff();
                        if (diff == null) continue;
                        var latestStatus = minuteEnumerator.State;

                        // squash changes.
                        Log.Verbose($"Squashing changes...");
                        var changeSet = new [] { diff }.Squash();

                        // build meta data.
                        var metaData = new List<(string key, string value)>
                        {
                            ("period", latestStatus.Config.Period.ToString()),
                            ("sequence_number", latestStatus.SequenceNumber.ToString())
                        };

                        // apply diff.
                        Log.Information($"Applying changes...");
                        db.ApplyDiff(changeSet, latestStatus.EndTimestamp, metaData);
                        Log.Information($"Took {new TimeSpan(DateTime.Now.Ticks - ticks).TotalSeconds}s");

                        if (previousLatest.Day != db.Latest.EndTimestamp.Day)
                        {
                            // an data was skipped, take a snapshot.
                            ticks = DateTime.Now.Ticks;
                            Log.Information($"Taking snapshot...");
                            db.TakeDiffSnapshot(null, TimeSpan.FromDays(1), metaData);
                            Log.Information($"Snapshot took {new TimeSpan(DateTime.Now.Ticks - ticks).TotalSeconds}s");
                        }
                        else if (previousLatest.Hour != db.Latest.EndTimestamp.Hour)
                        {
                            // an hour was skipped, take a snapshot.
                            ticks = DateTime.Now.Ticks;
                            Log.Information($"Taking snapshot...");
                            db.TakeDiffSnapshot(null, TimeSpan.FromHours(1), metaData);
                            Log.Information($"Snapshot took {new TimeSpan(DateTime.Now.Ticks - ticks).TotalSeconds}s");
                        }
                    }
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