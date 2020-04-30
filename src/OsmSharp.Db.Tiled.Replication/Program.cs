using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using OsmSharp.Changesets;
using OsmSharp.Db.Tiled.Build;
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
                Log.Information("DB loaded successfully.");
                
                // start catch up until we reach hours/days.
                var catchupEnumerator = new CatchupReplicationDiffEnumerator(db.Latest.Timestamp.AddSeconds(1));
                var changeSets = new List<OsmChange>();
                while (await catchupEnumerator.MoveNext())
                {
                    Log.Verbose($"Downloading diff: {catchupEnumerator.State}");
                    changeSets.Add(await catchupEnumerator.Diff());
                }

                // apply changes.
                if (changeSets.Count == 0)
                {
                    Log.Information("No new changes.");
                    return;
                }

                var changeSet = changeSets[0];
                if (changeSets.Count > 1)
                {
                    Log.Verbose($"Squashing changes...");
                    changeSet = changeSets.Squash();
                }
                Log.Information($"Applying changes...");
                db.ApplyDiff(changeSet);
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