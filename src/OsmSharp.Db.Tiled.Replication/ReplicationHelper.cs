using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using OsmSharp.Changesets;
using OsmSharp.Replication;
using Serilog;

namespace OsmSharp.Db.Tiled.Replication
{
    internal static class ReplicationHelper
    {
        /// <summary>
        /// Updates the given database and writes a lock file to prevent concurrent updates.
        ///
        /// When the database is too far behind it first applies hourly diffs. When that is exhausted minutely diffs are applied.   
        /// </summary>
        /// <param name="dbPath">The database path.</param>
        /// <returns>True if there was a diff applied, false if there was no new data or a lockfile preventing the update.</returns>
        public static async Task<bool> TryUpdateWithLock(string dbPath)
        {
            var lockFile = new FileInfo(Path.Combine(dbPath, "replication.lock"));
            if (LockHelper.IsLocked(lockFile.FullName, TimeSpan.FromHours(1)))
            {
                Log.Information($"Lockfile found at {lockFile.FullName}, is there another update running?");
                return false;
            }
            
            try
            {
                LockHelper.WriteLock(lockFile.FullName);

                return await Update(dbPath);
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
        /// Updates the given database.
        ///
        /// When the database is too far behind it first applies hourly diffs. When that is exhausted minutely diffs are applied.   
        /// </summary>
        /// <param name="dbPath">The database path.</param>
        /// <returns>True if there was a diff applied, false if there was no new data.</returns>
        private static async Task<bool> Update(string dbPath)
        {
            if (!OsmTiledHistoryDb.TryLoad(dbPath, out var db))
            {
                Log.Fatal($"Could not load db at {dbPath}.");
                return false;
            }

            if (db == null) throw new Exception("Db was reported as loaded but is null!");
            Log.Information("DB loaded successfully.");

            var ticks = DateTime.Now.Ticks;
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
                return false;
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
            return true;
        }
    }
}