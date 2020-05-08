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
            var snapshot = config["snapshot"];

            var build = false;
            var update = false;
            var catchup = false;
            if (args.Length > 0)
            {
                snapshot = string.Empty;
                
                if (args.Length < 2)
                {
                    Log.Fatal("Invalid number of arguments expected at least two arguments (--update, --snapshot or --build) with a path given.");
                    return;
                }

                if (args[0] == "--update")
                {
                    dbPath = args[1];
                    if (!Directory.Exists(dbPath))
                    {
                        Log.Fatal($"The given database path doesn't exist: {dbPath}");
                        return;
                    }

                    update = true;
                    catchup = true;
                }
                else if (args[0] == "--build")
                {
                    if (args.Length < 3)
                    {
                        Log.Fatal("Invalid number of arguments expected at least --build with a planet file and a dbpath given.");
                        return;
                    }
                    planetFile = args[1];
                    if (!File.Exists(planetFile))
                    {
                        Log.Fatal($"The given planet file doesn't exist: {planetFile}");
                        return;
                    }
                    dbPath = args[2];

                    build = true;
                }
                else if (args[0] == "--snapshot")
                {
                    dbPath = args[1];
                    if (!Directory.Exists(dbPath))
                    {
                        Log.Fatal($"The given database path doesn't exist: {dbPath}");
                        return;
                    }
                    
                    snapshot = args.Length < 3 ? "day" : args[2];
                }
            }
            else
            {
                build = true;
                update = true;
            }
            
            Log.Verbose($"Running for {dbPath} given planet {planetFile} " +
                        $"with build={build}, update={update}, catchup={catchup}, snapshot={snapshot}");
            if (build && !OsmTiledHistoryDb.TryLoad(dbPath, out _))
            {
                if (string.IsNullOrWhiteSpace(dbPath))
                {
                    Log.Fatal($"No database path given.");
                    return;
                }
                if (!Directory.Exists(dbPath))
                {
                    Log.Fatal($"The given database path doesn't exist: {dbPath}");
                    return;
                }

                if (string.IsNullOrWhiteSpace(planetFile))
                {
                    Log.Fatal($"No planet file given.");
                    return;
                }
                if (!File.Exists(planetFile))
                {
                    Log.Fatal($"The given planet file doesn't exist: {planetFile}");
                    return;
                }
                
                // the database doesn't exist yet and it was requested to build.
                // try to build it with a lock.
                if (!BuildHelper.TryBuildWithLock(dbPath, planetFile))
                {
                    // could not build the database, it's probably already processing or something when wrong.
                    return;
                }

                // if catchup was not requested, building the db is enough.
                if (!catchup) return;
            }
            
            // the database was built or existed already at this point.
            if (OsmTiledHistoryDb.TryLoad(dbPath, out _))
            {
                if (string.IsNullOrWhiteSpace(dbPath))
                {
                    Log.Fatal($"No database path given.");
                    return;
                }
                if (!Directory.Exists(dbPath))
                {
                    Log.Fatal($"The given database path doesn't exist: {dbPath}");
                    return;
                }
                
                do
                {
                    // take a snapshot if needed.
                    if (!string.IsNullOrWhiteSpace(snapshot))
                    {
                        // try to take the snapshot.
                        SnapshotHelper.TrySnapshotWithLock(dbPath, snapshot);
                        
                        // a snapshot was requested.
                        // if update was not requested, stop here.
                        if (!catchup && !update) return;
                    }

                    if (update)
                    {
                        if (!await ReplicationHelper.TryUpdateWithLock(dbPath))
                        {
                            // update was not needed or failed, stop here.
                            return;
                        }
                    }
                } while (catchup);
            }
        }
    }
}