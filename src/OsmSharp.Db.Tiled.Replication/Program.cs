using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using OsmSharp.Changesets;
using OsmSharp.Logging;
using OsmSharp.Replication;
using OsmSharp.Streams;
using Serilog;

namespace OsmSharp.Db.Tiled.Replication
{
    internal static class Program
    {
        static async Task Main(string[] args)
        {
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

            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Verbose()
                .WriteTo.Console()
                .WriteTo.File(Path.Combine("logs", "log-.txt"), rollingInterval: RollingInterval.Day)
                .CreateLogger();

            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json");

            var config = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", true, true)
                .Build();

            var planetFile = config["planet"];
            var dbPath = config["db"];

            var lockFile = new FileInfo(Path.Combine(dbPath, "replication.lock"));
            if (LockHelper.IsLocked(lockFile.FullName))
            {
                return;
            }

            try
            {
                LockHelper.WriteLock(lockFile.FullName);

                // try loading the db, if it doesn't exist build it.
                var ticks = DateTime.Now.Ticks;
                if (!OsmTiledHistoryDb.TryLoad(dbPath, out var db))
                {
                    Log.Information("The DB doesn't exist yet, building...");

                    var source = new PBFOsmStreamSource(
                        File.OpenRead(planetFile));
                    var progress = new OsmSharp.Streams.Filters.OsmStreamFilterProgress();
                    progress.RegisterSource(source);

                    // splitting tiles and writing indexes.
                    db = OsmTiledHistoryDb.Create(dbPath, progress);
                    Log.Information("DB built successfully.");
                    Log.Information($"Took {new TimeSpan(DateTime.Now.Ticks - ticks).TotalSeconds}s");
                    return;
                }

                if (db == null) throw new Exception("Db loading failed!");
                Log.Information("DB loaded successfully.");

                ticks = DateTime.Now.Ticks;
                // play catchup if the database is behind more than one hour.
                // try downloading the latest hour.
                var changeSets = new List<OsmChange>();
                ReplicationState? latestStatus = null;
                if ((DateTime.Now.ToUniversalTime() - db.Latest.EndTimestamp).TotalHours > 1)
                {
                    // the data is pretty old, update per hour.
                    var hourEnumerator = await ReplicationConfig.Hourly.GetDiffEnumerator(db.Latest);
                    if (hourEnumerator != null)
                    {
                        if (await hourEnumerator.MoveNext())
                        {
                            Log.Verbose($"Downloading diff: {hourEnumerator.State}");
                            var diff = await hourEnumerator.Diff();
                            if (diff != null)
                            {
                                latestStatus = hourEnumerator.State;
                                changeSets.Add(diff);
                            }
                        }
                    }
                }
                else
                {
                    // the data is pretty recent, start doing minutes, do as much as available.
                    var minuteEnumerator = await ReplicationConfig.Minutely.GetDiffEnumerator(db.Latest);
                    if (minuteEnumerator != null)
                    {
                        while (await minuteEnumerator.MoveNext())
                        {
                            Log.Verbose($"Downloading diff: {minuteEnumerator.State}");
                            var diff = await minuteEnumerator.Diff();
                            if (diff == null) continue;

                            latestStatus = minuteEnumerator.State;
                            changeSets.Add(diff);
                        }
                    }
                }

                // apply diffs.
                if (latestStatus == null)
                {
                    Log.Information("No more changes, db is up to date.");
                    return;
                }

                // squash changes.
                var changeSet = changeSets[0];
                if (changeSets.Count > 1)
                {
                    Log.Verbose($"Squashing changes...");
                    changeSet = changeSets.Squash();
                }

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
            }
            catch (Exception e)
            {
                Log.Fatal(e, "Unhandled exception during processing.");
            }
            finally
            {
                File.Delete(lockFile.FullName);
            }
        }
    }
}