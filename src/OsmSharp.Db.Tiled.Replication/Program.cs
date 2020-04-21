using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using OsmSharp.Db.Tiled.Build;
using OsmSharp.Logging;
using OsmSharp.Streams;
using Serilog;

namespace OsmSharp.Db.Tiled.Replication
{
    class Program
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
            var zoom = 14U;

            var lockFile = new FileInfo(Path.Combine(dbPath, "replication.lock"));
            if (LockHelper.IsLocked(lockFile.FullName))
            {
                return;
            }

            try
            {
                LockHelper.WriteLock(lockFile.FullName);

                var source = new PBFOsmStreamSource(
                    File.OpenRead(planetFile));
                var progress = new OsmSharp.Streams.Filters.OsmStreamFilterProgress();
                progress.RegisterSource(source);

                // try loading the db, if it doesn't exist build it.
                if (!OsmTiledHistoryDb.TryLoad(dbPath, out var db))
                {
                    Log.Information("The DB doesn't exist yet, building...");

                    // splitting tiles and writing indexes.
                    db = await OsmTiledHistoryDb.Create(dbPath, progress);
                }
                else
                {
                    Log.Information("The DB exists, updating...");

                    // add data.
                    await db.Update(progress);
                }

//            // start catch up until we reach hours/days.
//            var catchupEnumerator = new CatchupReplicationDiffEnumerator(db.Latest.Timestamp.AddSeconds(1), moveDown:false);
//            while (await catchupEnumerator.MoveNext())
//            {
//                if (catchupEnumerator.State.Config.Period >= replicationLevel.Period)
//                { // replication level reached.
//                    break;
//                }
//                    
//                Log.Information($"Applying changes: {catchupEnumerator.State}");
//                await catchupEnumerator.ApplyCurrent(db);
//                Log.Information($"Changes applied, new database: {db}");
//            }
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