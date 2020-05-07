using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using OsmSharp.Changesets;
using OsmSharp.Db.Tiled.OsmTiled;
using OsmSharp.Replication;
using OsmSharp.Streams;
using Serilog;

namespace OsmSharp.Db.Tiled.Replication
{
    internal static class ReplicationHelper
    {
        public static bool BuildOrAdd(string dbPath, string planetFile,
            bool add = false)
        {
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
                return true;
            }
            else if (add && !string.IsNullOrWhiteSpace(planetFile))
            {
                if (db == null) throw new Exception("Db loading failed!");
                Log.Warning($"Database already exists, adding new data from {planetFile}");

                var source = new PBFOsmStreamSource(
                    File.OpenRead(planetFile));
                var progress = new OsmSharp.Streams.Filters.OsmStreamFilterProgress();
                progress.RegisterSource(source);
                    
                db.Add(progress);
                Log.Information("DB updated successfully.");
                Log.Information($"Took {new TimeSpan(DateTime.Now.Ticks - ticks).TotalSeconds}s");
                return true;
            }

            return false;
        }

        public static async Task Update(string dbPath, bool catchup = false)
        {
            if (!OsmTiledHistoryDb.TryLoad(dbPath, out var db))
            {
                Log.Fatal($"Could not load db at {dbPath}.");
                return;
            }

            if (db == null) throw new Exception("Db was reported as loaded but is null!");
            Log.Information("DB loaded successfully.");

            do
            {
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
            } while (catchup);
        }

        public static void Snapshot(string dbPath, string type)
        {
            if (!OsmTiledHistoryDb.TryLoad(dbPath, out var db))
            {
                Log.Fatal($"Could not load db at {dbPath}.");
                return;
            }

            if (db == null) throw new Exception("Db was reported as loaded but is null!");
            Log.Information("DB loaded successfully.");
            
            // find latest day/week crossing.
            if (type == "day")
            {
                var dayAgo = DateTime.Now.ToUniversalTime().Date;
                var current = db.GetOn(dayAgo);
                if (current == null)
                {
                    Log.Information("No data found that's over a day old, no need to snapshot.");
                    return;
                }

                if (current is OsmTiledDbSnapshot)
                {
                    Log.Information("There is already a snapshot.");
                    return;
                }
                
                Log.Information("Building snapshot...");
                db.TakeSnapshot(dayAgo, TimeSpan.FromDays(1), current.Meta);
            }
            else if (type == "week")
            {
                var weekAgo = DateTime.Now.ToUniversalTime().StartOfWeek(DayOfWeek.Monday);
                var current = db.GetOn(weekAgo);
                if (current == null)
                {
                    Log.Information("No data found that's a week old, no need to snapshot.");
                    return;
                }

                if (current is OsmTiledDbSnapshot)
                {
                    Log.Information("There is already a snapshot.");
                    return;
                }
                
                Log.Information("Building snapshot...");
                db.TakeSnapshot(weekAgo, TimeSpan.FromDays(7), current.Meta);
            }
        }
    }
}