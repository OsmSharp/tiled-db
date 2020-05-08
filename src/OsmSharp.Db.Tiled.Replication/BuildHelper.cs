using System;
using System.IO;
using OsmSharp.Streams;
using Serilog;

namespace OsmSharp.Db.Tiled.Replication
{
    internal static class BuildHelper
    {
        /// <summary>
        /// Builds a new database but writes a lock file to prevent duplicate builds.
        /// </summary>
        /// <param name="dbPath">The db path.</param>
        /// <param name="planetFile">The planet file.</param>
        /// <returns>True if the database was built, false if the database was already there or a lock file is preventing the build process.</returns>
        public static bool TryBuildWithLock(string dbPath, string planetFile)
        {
            var lockFile = new FileInfo(Path.Combine(dbPath, "build.lock"));
            if (LockHelper.IsLocked(lockFile.FullName, TimeSpan.FromDays(2)))
            {
                Log.Information($"Lockfile found at {lockFile.FullName}, is there another build running?");
                return false;
            }
            
            try
            {
                LockHelper.WriteLock(lockFile.FullName);

                return Build(dbPath, planetFile);
            }
            catch (Exception e)
            {
                Log.Fatal(e, "Unhandled exception during processing.");
            }
            finally
            {
                File.Delete(lockFile.FullName);
            }

            return false;
        }
        
        /// <summary>
        /// Builds a new database.
        /// </summary>
        /// <param name="dbPath">The db path.</param>
        /// <param name="planetFile">The planet file.</param>
        /// <returns>True if the database was built, false if the database was already there.</returns>
        private static bool Build(string dbPath, string planetFile)
        {
            if (OsmTiledHistoryDb.TryLoad(dbPath, out _))
            {
                Log.Warning(
                    "Not building database, already there. To rebuild, delete database first, to add data, use 'add'.");
                return false;
            }

            Log.Information("The DB doesn't exist yet, building...");

            var source = new PBFOsmStreamSource(
                File.OpenRead(planetFile));
            var progress = new OsmSharp.Streams.Filters.OsmStreamFilterProgress();
            progress.RegisterSource(source);

            // creating a new database.
            var ticks = DateTime.Now.Ticks;
            OsmTiledHistoryDb.Create(dbPath, progress);
            Log.Information("DB built successfully.");
            Log.Information($"Took {new TimeSpan(DateTime.Now.Ticks - ticks).TotalSeconds}s");
            return true;
        }

        /// <summary>
        /// Adds data to an already existing database by adding a new full db.
        /// </summary>
        /// <param name="dbPath">The db path.</param>
        /// <param name="planetFile">The plane file.</param>
        /// <returns>True if adding succeeded, false if the database doesn't exist.</returns>
        /// <exception cref="Exception"></exception>
        public static bool Add(string dbPath, string planetFile)
        {
            if (!OsmTiledHistoryDb.TryLoad(dbPath, out var db) ||
                db == null)
            {
                Log.Warning(
                    "Not add data, database not found. Use 'build' to create a new database.");
                return false;
            }
            
            Log.Information($"Adding new data from {planetFile}");

            var source = new PBFOsmStreamSource(
                File.OpenRead(planetFile));
            var progress = new OsmSharp.Streams.Filters.OsmStreamFilterProgress();
            progress.RegisterSource(source);

            var ticks = DateTime.Now.Ticks;
            db.Add(progress);
            Log.Information("DB updated successfully.");
            Log.Information($"Took {new TimeSpan(DateTime.Now.Ticks - ticks).TotalSeconds}s");
            return true;
        }
    }
}