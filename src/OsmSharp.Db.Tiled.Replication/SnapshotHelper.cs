using System;
using System.IO;
using OsmSharp.Db.Tiled.OsmTiled;
using Serilog;

namespace OsmSharp.Db.Tiled.Replication
{
    internal static class SnapshotHelper
    {
        /// <summary>
        /// Takes a snapshot for the given period and writes a lock file to prevent concurrent snapshot taking.
        /// </summary>
        /// <param name="dbPath">The db path.</param>
        /// <param name="period">The period, 'latest', 'day' or 'week'.</param>
        /// <returns>True if a snapshot was taken, false otherwise.</returns>
        public static bool TrySnapshotWithLock(string dbPath, string period)
        {
            var lockFile = new FileInfo(Path.Combine(dbPath, "snapshot.lock"));
            if (LockHelper.IsLocked(lockFile.FullName, TimeSpan.FromDays(1)))
            {
                Log.Information($"Lockfile found at {lockFile.FullName}, is there another update running?");
                return false;
            }
            
            try
            {
                LockHelper.WriteLock(lockFile.FullName);

                return Snapshot(dbPath, period);
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
        /// Takes a snapshot for the given period.
        /// </summary>
        /// <param name="dbPath">The db path.</param>
        /// <param name="period">The period, 'latest', 'day' or 'week'.</param>
        /// <returns>True if a snapshot was taken, false otherwise.</returns>
        private static bool Snapshot(string dbPath, string period)
        {
            if (!OsmTiledHistoryDb.TryLoad(dbPath, out var db))
            {
                return false;
            }

            if (db == null) throw new Exception("Db was reported as loaded but is null!");
            Log.Information("DB loaded successfully.");
            
            // find latest day/week crossing.
            if (period == "latest")
            {
                Log.Information("Taking snapshot of the latest...");
                var snapshot = db.TakeSnapshot();
                return snapshot != null;
            }
            else if (period == "day")
            {
                var oneDayAgo = db.Latest.EndTimestamp.Date;
                var twoDaysAgo = oneDayAgo.AddDays(-1);

                if (!db.HasOn(twoDaysAgo))
                {
                    Log.Information("No complete day period found compared to latest, no need to snapshot.");
                    return false;
                }
                
                var dayAgoDb = db.GetOn(oneDayAgo);
                if (dayAgoDb == null)
                {
                    Log.Information("No complete day period found compared to latest, no need to snapshot.");
                    return false;
                }

                if (!(dayAgoDb is OsmTiledDbDiff))
                {
                    Log.Information("There is already a snapshot.");
                    return false;
                }
                
                Log.Information($"Building snapshot at {dayAgoDb.EndTimestamp}...");
                var snapshot = db.TakeDiffSnapshot(timeStamp: oneDayAgo, meta: dayAgoDb.Meta);
                return snapshot != null;
            }
            else if (period == "week")
            {
                var weekAgo = DateTime.Now.ToUniversalTime()
                    .StartOfWeek(DayOfWeek.Monday);
                var twoWeeksAgo = weekAgo.AddDays(-1);

                if (!db.HasOn(twoWeeksAgo))
                {
                    Log.Information("No complete week found compared to latest, no need to snapshot.");
                    return false;
                }
                
                var weekAgoDb = db.GetOn(weekAgo);
                if (weekAgoDb == null)
                {
                    Log.Information("No complete week found compared to latest, no need to snapshot.");
                    return false;
                }

                if (!(weekAgoDb is OsmTiledDbDiff))
                {
                    Log.Information("There is already a snapshot.");
                    return false;
                }
                
                Log.Information($"Building snapshot at {weekAgoDb.EndTimestamp}...");
                var snapshot = db.TakeDiffSnapshot(timeStamp: weekAgo, meta: weekAgoDb.Meta);
                return snapshot != null;
            }

            return false;
        }
    }
}