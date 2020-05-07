using System;
using System.Collections.Generic;
using System.IO;
using OsmSharp.Db.Tiled.OsmTiled;
using Serilog;

namespace OsmSharp.Db.Tiled.Replication
{
    public static class SnapshotHelper
    {
        public static bool Snapshot(string dbPath, string type)
        {
            if (!OsmTiledHistoryDb.TryLoad(dbPath, out var db))
            {
                return false;
            }

            if (db == null) throw new Exception("Db was reported as loaded but is null!");
            Log.Information("DB loaded successfully.");
            
            // find latest day/week crossing.
            if (type == "latest")
            {
                Log.Information("Taking snapshot of the latest...");
                return DoSnapshot(db, dbPath);
            }
            else if (type == "day")
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
                return DoSnapshot(db, dbPath, timeStamp: oneDayAgo, meta: dayAgoDb.Meta);
            }
            else if (type == "week")
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
                return DoSnapshot(db, dbPath, timeStamp: weekAgo, meta: weekAgoDb.Meta);
            }

            return false;
        }
        
        private static bool DoSnapshot(OsmTiledHistoryDb db, string dbPath, DateTime? timeStamp = null, TimeSpan? timeSpan = null,
            IEnumerable<(string key, string value)>? meta = null)
        {
            var lockFile = new FileInfo(Path.Combine(dbPath, "snapshot-replication.lock"));
            if (LockHelper.IsLocked(lockFile.FullName))
            {
                return false;
            }

            try
            {
                LockHelper.WriteLock(lockFile.FullName);

                var snapshot = db.TakeSnapshot(meta: meta, timeSpan: timeSpan, timeStamp: timeStamp);
                return snapshot != null;
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
    }
}