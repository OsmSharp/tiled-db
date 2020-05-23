using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using OsmSharp.Changesets;
using OsmSharp.Db.Tiled.Collections.Search;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.Logging;
using OsmSharp.Db.Tiled.OsmTiled;
using OsmSharp.Db.Tiled.OsmTiled.Build;
using OsmSharp.Db.Tiled.OsmTiled.IO;

namespace OsmSharp.Db.Tiled
{
    /// <summary>
    /// Represents a full OSM db including history built up by snapshots and diffs.
    /// </summary>
    public class OsmTiledHistoryDb
    {
        private readonly string _path;
        private readonly SortedList<long, OsmTiledDbsList?> _dbs;
        private readonly object DiffSync = new object();
        private long _latestId;

        /// <summary>
        /// The default zoom level.
        /// </summary>
        public const uint DefaultZoom = 14;

        /// <summary>
        /// Creates a new OSM db.
        /// </summary>
        /// <param name="path">The path.</param>
        internal OsmTiledHistoryDb(string path)
        {
            _path = path;

            _dbs = new SortedList<long, OsmTiledDbsList?>();
            foreach (var (_, id, _, _) in OsmTiledDbOperations.GetDbPaths(_path))
            {
                _dbs[id] = null;
            }
            if (_dbs.Count == 0) throw new Exception("No databases found!");
            _latestId = _dbs.Keys[_dbs.Count - 1];
        }

        private OsmTiledDbsList GetDb(long id)
        {
            lock (DiffSync)
            {
                if (_dbs.TryGetValue(id, out var osmTiledDbBase) && osmTiledDbBase != null) return osmTiledDbBase;

                osmTiledDbBase = OsmTiledDbOperations.LoadDbs(this._path, id, i => this.GetDb(i).Db);
                _dbs[id] = osmTiledDbBase;

                return osmTiledDbBase;
            }
        }

        /// <summary>
        /// Gets the location of this db.
        /// </summary>
        internal string Path => _path;

        internal long? Previous(long id)
        {
            lock (DiffSync)
            {
                var i = _dbs.IndexOfKey(id);
                if (i < 0) throw new ArgumentException($"Db with id {id} not found.", nameof(id));

                if (i == 0) return null;

                return _dbs.Keys[i - 1];
            }
        }

        internal long? Next(long id)
        {
            lock (DiffSync)
            {
                var i = _dbs.IndexOfKey(id);
                if (i < 0) throw new ArgumentException($"Db with id {id} not found.", nameof(id));

                if (i == _dbs.Count - 1) return null;

                return _dbs.Keys[i + 1];
            }
        }

        internal OsmTiledDbBase GetSmallest(long id)
        {
            return this.GetDb(id).Smallest();
        }

        /// <summary>
        /// Gets the latest snapshot db.
        /// </summary>
        public OsmTiledDbBase Latest => this.GetDb(_latestId).Db;

        /// <summary>
        /// Creates a new db.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <param name="data">The data.</param>
        /// <param name="timeStamp">The timestamp, overrides the timestamps in the data.</param>
        /// <param name="meta">The meta data to store along with the db.</param>
        /// <returns>The new db.</returns>
        public static OsmTiledHistoryDb Create(string path, IEnumerable<OsmGeo> data, DateTime? timeStamp = null, 
            IEnumerable<(string key, string value)>? meta = null)
        {
            return Build.OsmTiledHistoryDbBuilder.Build(data, path, DefaultZoom, timeStamp, meta);
        }
        
        /// <summary>
        /// Adds a new osm tiled db as latest using the given data.
        /// </summary>
        /// <param name="data">The data.</param>
        /// <param name="timeStamp">The timestamp, overrides the timestamps in the data.</param>
        /// <param name="meta">The meta data to store along with the db.</param>
        public void Add(IEnumerable<OsmGeo> data, DateTime? timeStamp = null, 
            IEnumerable<(string key, string value)>? meta = null)
        {
            // update.
            var latest = Build.OsmTiledHistoryDbBuilder.Add(data, this._path, timeStamp: timeStamp, meta: meta);
            _latestId = latest.Id;
            
            lock (DiffSync)
            {
                // update data.
                if (!_dbs.TryGetValue(latest.Id, out var existingDbList))
                {
                    existingDbList = new OsmTiledDbsList(latest, null);
                }
                else
                {
                    existingDbList ??= GetDb(latest.Id);
                    existingDbList = existingDbList.Add(latest);
                }
                
                _dbs[latest.Id] = existingDbList;
            }
        }

        /// <summary>
        /// Applies a diff to this OSM db, the latest db is updated.
        /// </summary>
        /// <param name="diff">The changeset.</param>
        /// <param name="timeStamp">The timestamp from the diff meta-data override the timestamps in the data.</param>
        /// <param name="meta">The meta data to store along with the db.</param>
        public void ApplyDiff(OsmChange diff, DateTime? timeStamp = null, 
            IEnumerable<(string key, string value)>? meta = null)
        {
            // format new path.
            var tempPath = OsmTiledDbOperations.BuildTempDbPath(this._path);
            if (!FileSystemFacade.FileSystem.DirectoryExists(tempPath))
                FileSystemFacade.FileSystem.CreateDirectory(tempPath);

            // build new db.
            var dbMeta = this.Latest.BuildDiff(diff, tempPath, timeStamp: timeStamp, meta: meta);
            if (dbMeta.Timespan == null) throw new InvalidDataException("Snapshot should have a valid timespan.");

            // generate a proper path and move the data there.
            var dbPath = OsmTiledDbOperations.BuildDbPath(this._path, dbMeta.Id, dbMeta.Timespan.Value,
                OsmTiledDbType.Diff);
            FileSystemFacade.FileSystem.MoveDirectory(tempPath, dbPath);

            // update data.
            var latest = new OsmTiledDbDiff(dbPath, (id) => this.GetDb(id).Db);
            _latestId = latest.Id;
            lock (DiffSync)
            {
                // update data.
                if (!_dbs.TryGetValue(latest.Id, out var existingDbList))
                {
                    existingDbList = new OsmTiledDbsList(latest, null);
                }
                else
                {
                    existingDbList ??= GetDb(latest.Id);
                    existingDbList = existingDbList.Add(latest);
                }
                
                _dbs[latest.Id] = existingDbList;
            }
        }
        
        /// <summary>
        /// Groups together all the diff databases into one bigger diff database. Snapshots and full databases are not included.
        /// </summary>
        /// <param name="timeStamp">The timestamp, if none given uses latest.</param>
        /// <param name="timeSpan">The timespan, if none given snapshots all the diffs until it finds a non-diff db.</param>
        /// <param name="meta">The meta data to store along with the db.</param>
        /// <returns>The new db.</returns>
        public OsmTiledDbBase? TakeDiffSnapshot(DateTime? timeStamp = null, TimeSpan? timeSpan = null,
            IEnumerable<(string key, string value)>? meta = null)
        {
            timeStamp ??= this.Latest.EndTimestamp;
            
            var osmTiledDb = this.GetListOn(timeStamp.Value)?.Db;
            if (osmTiledDb == null) return null;
            if (!(osmTiledDb is OsmTiledDbDiff))
            {
                Log.Default.Verbose("No need to snapshot, already there.");
                return null;
            }

            // copy meta from latest db.
            meta ??= osmTiledDb.Meta;
            
            // collect all tiles that have changed.
            var latestDb = osmTiledDb;
            DateTime? earliest = null;
            if (timeSpan.HasValue) earliest = timeStamp - timeSpan.Value;

            var diffs = new List<OsmTiledDbDiff>();
            while (osmTiledDb is OsmTiledDbDiff nextDiff)
            {
                diffs.Add(nextDiff);

                if (osmTiledDb.Base == null) throw new InvalidDataException($"{nameof(OsmTiledDbDiff)} should have a valid base db.");
                osmTiledDb = this.GetDb(osmTiledDb.Base.Value).Db;
                if (osmTiledDb == null) throw new InvalidDataException($"{nameof(OsmTiledDbDiff)} should have a valid base db.");

                // if earliest has value, a timespan was set.
                // make sure to stop when reached.
                if (earliest.HasValue)
                {
                    if (osmTiledDb.EndTimestamp <= earliest) break;
                }
            }
            diffs.Reverse();

            if (diffs.Count == 0)
            {
                Log.Default.Verbose("No diffs found to snapshot.");
                return null;
            }
            if (diffs.Count == 1)
            {
                Log.Default.Verbose("Only one diff found, no need to snapshot.");
                return null;
            }
            
            // format new path.
            var tempPath = OsmTiledDbOperations.BuildTempDbPath(this._path);
            if (!FileSystemFacade.FileSystem.DirectoryExists(tempPath))
                FileSystemFacade.FileSystem.CreateDirectory(tempPath);
                
            // build new db.
            var dbMeta = latestDb.BuildDiffSnapshot(diffs, tempPath, latestDb.Id, osmTiledDb.Id, meta: meta);
            dbMeta.Base = osmTiledDb.Id;
            if (dbMeta.Timespan == null) throw new InvalidDataException("Snapshot should have a valid timespan.");
                
            // generate a proper path and move the data there.
            var dbPath = OsmTiledDbOperations.BuildDbPath(this._path, dbMeta.Id, dbMeta.Timespan.Value, OsmTiledDbType.Diff);
            FileSystemFacade.FileSystem.MoveDirectory(tempPath, dbPath);
                
            // update data.
            var diffSnapshot = new OsmTiledDbDiff(dbPath, i => this.GetDb(i).Db);
            lock (DiffSync)
            {
                // update data.
                if (!_dbs.TryGetValue(diffSnapshot.Id, out var existingDbList))
                {
                    existingDbList = new OsmTiledDbsList(diffSnapshot, null);
                }
                else
                {
                    existingDbList ??= GetDb(diffSnapshot.Id);
                    existingDbList = existingDbList.Add(diffSnapshot);
                }
                
                _dbs[diffSnapshot.Id] = existingDbList;
            }

            return diffSnapshot;
        }
        
        /// <summary>
        /// Groups the data on or right before the given timestamp and the data before in the given timespan.
        /// </summary>
        /// <param name="timeStamp">The timestamp, if none given uses latest.</param>
        /// <param name="timeSpan">The timespan, if none given snapshots all the diffs until it finds a non-diff db.</param>
        /// <param name="meta">The meta data to store along with the db.</param>
        /// <returns>The new db.</returns>
        public OsmTiledDbBase? TakeSnapshot(DateTime? timeStamp = null, TimeSpan? timeSpan = null, 
            IEnumerable<(string key, string value)>? meta = null)
        {
            timeStamp ??= this.Latest.EndTimestamp;
            
            var osmTiledDb = this.GetListOn(timeStamp.Value)?.Db;
            if (osmTiledDb == null) return null;
            if (!(osmTiledDb is OsmTiledDbDiff))
            {
                Log.Default.Verbose("No need to snapshot, already there.");
                return null;
            }

            // copy meta from latest db.
            meta ??= osmTiledDb.Meta;
            
            // collect all tiles that have changed.
            var latestDb = osmTiledDb;
            DateTime? earliest = null;
            if (timeSpan.HasValue) earliest = timeStamp - timeSpan.Value;
            
            var tiles = new HashSet<(uint x, uint y)>();
            while (osmTiledDb is OsmTiledDbDiff nextDiff)
            {
                tiles.UnionWith(nextDiff.GetTiles(true));

                if (osmTiledDb.Base == null) throw new InvalidDataException($"{nameof(OsmTiledDbDiff)} should have a valid base db.");
                osmTiledDb = this.GetDb(osmTiledDb.Base.Value).Db;
                if (osmTiledDb == null) throw new InvalidDataException($"{nameof(OsmTiledDbDiff)} should have a valid base db.");

                // if earliest has value, a timespan was set.
                // make sure to stop when reached.
                if (earliest.HasValue)
                {
                    if (osmTiledDb.EndTimestamp <= earliest) break;
                }
            }

            if (tiles.Count == 0)
            {
                Log.Default.Verbose("No data found to snapshot.");
                return null;
            }
            
            // format new path.
            var tempPath = OsmTiledDbOperations.BuildTempDbPath(this._path);
            if (!FileSystemFacade.FileSystem.DirectoryExists(tempPath))
                FileSystemFacade.FileSystem.CreateDirectory(tempPath);
                
            // build new db.
            var dbMeta = latestDb.BuildSnapshot(tiles.ToArray(), tempPath, latestDb.Id, osmTiledDb.Id, meta: meta);
            dbMeta.Base = osmTiledDb.Id;
            if (dbMeta.Timespan == null) throw new InvalidDataException("Snapshot should have a valid timespan.");
                
            // generate a proper path and move the data there.
            var dbPath = OsmTiledDbOperations.BuildDbPath(this._path, dbMeta.Id, dbMeta.Timespan.Value, OsmTiledDbType.Snapshot);
            FileSystemFacade.FileSystem.MoveDirectory(tempPath, dbPath);
                
            // update data.
            var snapshot = new OsmTiledDbSnapshot(dbPath, (id) => this.GetDb(id).Db);
            lock (DiffSync)
            {
                // update data.
                if (!_dbs.TryGetValue(snapshot.Id, out var existingDbList))
                {
                    existingDbList = new OsmTiledDbsList(snapshot, null);
                }
                else
                {
                    existingDbList ??= GetDb(snapshot.Id);
                    existingDbList = existingDbList.Add(snapshot);
                }
                
                _dbs[snapshot.Id] = existingDbList;
            }

            return snapshot;
        }

        private bool TryOn(DateTime timestamp, out long id)
        {
            lock (DiffSync)
            {
                id = timestamp.ToUnixTime();
                if (id > this.Latest.Id) return false;
                
                var index = _dbs.Keys.BinarySearch(id);

                if (index < 0)
                {
                    index = -index;
                    index -= 1;
                    if (index > _dbs.Keys.Count) return false;
                }

                id = _dbs.Keys[index];
            }

            return true;
        }

        /// <summary>
        /// Returns true if there is a database that was created on or right before the given timestamp.
        /// </summary>
        /// <param name="timestamp">The timestamp.</param>
        /// <returns>True if found.</returns>
        public bool HasOn(DateTime timestamp)
        {
            if (TryOn(timestamp, out _))
            {
                return true;
            }

            return false;
        }

        internal OsmTiledDbsList? GetListOn(DateTime timestamp)
        {
            if (TryOn(timestamp, out var id))
            {
                return GetDb(id);
            }

            return null;
        }

        /// <summary>
        /// Try to load an OSM db from the given path.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <param name="osmDb">The db if any.</param>
        /// <returns>True if a db was loaded, false otherwise.</returns>
        public static bool TryLoad(string path, out OsmTiledHistoryDb? osmDb)
        {
            var dbs = new SortedList<long, OsmTiledDbBase?>();
            foreach (var (_, id, _, _) in OsmTiledDbOperations.GetDbPaths(path))
            {
                dbs[id] = null;
            }

            osmDb = null;
            if (dbs.Count == 0) return false;
            
            osmDb = new OsmTiledHistoryDb(path);
            return true;
        }

        /// <summary>
        /// Tries to reload the database.
        /// </summary>
        /// <returns>True if there is a new latest database.</returns>
        public bool TryReloadLatest()
        {
            try
            {
                var dbs = new SortedList<long, OsmTiledDbBase?>();
                foreach (var (_, id, _, _) in OsmTiledDbOperations.GetDbPaths(_path))
                {
                    dbs[id] = null;
                }

                if (dbs.Count == 0) return false;
                var latestId = dbs.Keys[dbs.Count - 1];

                if (latestId != _latestId)
                {
                    lock (DiffSync)
                    {
                        foreach (var id in dbs.Keys)
                        {
                            if (!_dbs.TryGetValue(id, out _))
                            {
                                _dbs[id] = null;
                            }
                        }
                        
                        if (_dbs.Count == 0) throw new Exception("No databases found!");
                        _latestId = _dbs.Keys[_dbs.Count - 1];
                    }

                    return true;
                }
            }
            catch (Exception)
            {
                // ignored
            }

            return false;
        }

        /// <inheritdoc/>
        public override string ToString()
        {
            return $"{_path}: {this.Latest.Path}@{this.Latest.EndTimestamp}";
        }
    }
}