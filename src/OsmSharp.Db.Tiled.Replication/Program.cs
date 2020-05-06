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

            var build = false;
            var update = false;
            var catchup = false;
            if (args.Length > 0)
            {
                if (args.Length < 2)
                {
                    Log.Fatal("Invalid number of arguments expected at least --update or --build with a path given.");
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

                    planetFile = null;

                    update = true;
                    catchup = true;
                }
                else if (args[0] == "--build")
                {
                    planetFile = args[1];
                    if (!File.Exists(planetFile))
                    {
                        Log.Fatal($"The given planet file doesn't exist: {planetFile}");
                        return;
                    }
                    dbPath = args[2];
                    if (!Directory.Exists(dbPath))
                    {
                        Log.Fatal($"The given database path doesn't exist: {dbPath}");
                        return;
                    }

                    build = true;
                }
            }
            else
            {
                build = true;
                update = true;
            }

            var lockFile = new FileInfo(Path.Combine(dbPath, "replication.lock"));
            if (LockHelper.IsLocked(lockFile.FullName))
            {
                if (build || update) Log.Information($"Lockfile found at {lockFile.FullName}, is there another update running?");
                return;
            }

            try
            {
                LockHelper.WriteLock(lockFile.FullName);

                if (!Directory.Exists(dbPath))
                {
                    Log.Fatal($"The given database path doesn't exist: {dbPath}");
                    return;
                }
                
                if (build)
                {
                    if (string.IsNullOrWhiteSpace(planetFile))
                    {
                        Log.Fatal("No valid planet file given.");
                        return;
                    }

                    if (!File.Exists(planetFile))
                    {
                        Log.Fatal($"Planet file {planetFile} not found!");
                        return;
                    }

                    // build the database, if updating is not requested we optionally add the given data, rebuilding the database.
                    if (ReplicationHelper.BuildOrAdd(dbPath, planetFile, !update))
                    {
                        // database was built or updated.
                        // if catchup was not requested, stop here.
                        if (!catchup) return;
                    }
                }

                if (update)
                {
                    // update the database.
                    await ReplicationHelper.Update(dbPath, catchup);
                }
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