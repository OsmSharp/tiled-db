using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;
using OsmSharp.Changesets;
using OsmSharp.Db.Tiled.IO;
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
        private readonly Dictionary<long, OsmTiledDbBase> _dbs;
        private OsmTiledHistoryDbMeta _meta;

        /// <summary>
        /// Creates a new OSM db.
        /// </summary>
        /// <param name="path">The path.</param>
        internal OsmTiledHistoryDb(string path)
        {
            _path = path;

            _dbs = new Dictionary<long, OsmTiledDbBase>();
            _meta = OsmTiledHistoryDbOperations.LoadDbMeta(_path);

            this.Latest = this.GetDb(_meta.Latest);
        }

        private OsmTiledDbBase GetDb(long id)
        {
            if (_dbs.TryGetValue(id, out var osmTiledDbBase)) return osmTiledDbBase;
            
            osmTiledDbBase = OsmTiledDbOperations.LoadDb(this._path, id, this.GetDb);
            _dbs[id] = osmTiledDbBase;

            return osmTiledDbBase;
        }

        /// <summary>
        /// Gets the latest snapshot db.
        /// </summary>
        public OsmTiledDbBase Latest { get; private set; }

        private readonly object _diffSync = new object();

        /// <summary>
        /// Creates a new db.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <param name="data">The data.</param>
        /// <returns>The new db.</returns>
        public static OsmTiledHistoryDb Create(string path, IEnumerable<OsmGeo> data)
        {
            return Build.OsmTiledHistoryDbBuilder.Build(data, path, 14);
        }
        
        /// <summary>
        /// Adds a new osm tiled db as latest using the given data.
        /// </summary>
        /// <param name="data">The data.</param>
        public void Update(IEnumerable<OsmGeo> data)
        {
            var latest = Build.OsmTiledHistoryDbBuilder.Update(data, this._path);
            
            lock (_diffSync)
            {
                // update data.
                this.Latest = latest;
                
                // update meta data.
                _meta = new OsmTiledHistoryDbMeta()
                {
                    Latest = latest.Id
                };
                OsmTiledHistoryDbOperations.SaveDbMeta(_path, _meta);
            }
        }

        /// <summary>
        /// Applies a diff to this OSM db, the latest db is updated.
        /// </summary>
        /// <param name="diff">The changeset.</param>
        /// <param name="timeStamp">The timestamp from the diff meta-data override the timestamps in the data.</param>
        public void ApplyDiff(OsmChange diff, DateTime? timeStamp = null)
        {
            lock (_diffSync)
            {           
                // format new path.
                var tempPath = OsmTiledDbOperations.BuildOsmTiledDbPath(this._path, DateTime.Now.ToUnixTime(), "temp");
                if (!FileSystemFacade.FileSystem.DirectoryExists(tempPath))
                    FileSystemFacade.FileSystem.CreateDirectory(tempPath);
                
                // build new db.
                var dbMeta = this.Latest.ApplyChangSet(diff, tempPath);
                timeStamp ??= dbMeta.Timestamp;
                
                // generate a proper path and move the data there.
                var dbPath = OsmTiledDbOperations.BuildOsmTiledDbPath(this._path, timeStamp.Value.ToUnixTime(), OsmTiledDbType.Snapshot);
                FileSystemFacade.FileSystem.MoveDirectory(tempPath, dbPath);
                
                // update data.
                this.Latest = new OsmTiledDbSnapshot(dbPath, this.GetDb);
                
                // update meta data.
                _meta = new OsmTiledHistoryDbMeta()
                {
                    Latest = this.Latest.Id
                };
                OsmTiledHistoryDbOperations.SaveDbMeta(_path, _meta);
            }
        }
        
        /// <summary>
        /// Gets the database for the given timestamp.
        ///
        /// The database that was created on or right before the given timestamp.
        /// </summary>
        /// <param name="timestamp">The timestamp.</param>
        /// <returns>The database closest to the given timestamp.</returns>
        public OsmTiledDb? GetOn(DateTime timestamp)
        {
            throw new NotImplementedException();
        }
        
        /// <summary>
        /// Groups the data on or right before the given timestamp and the data before in the given timespan.
        /// </summary>
        /// <param name="timestamp"></param>
        /// <param name="timeSpan"></param>
        public void TakeSnapshot(DateTime timestamp, TimeSpan? timeSpan = null)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Try to load an OSM db from the given path.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <param name="osmDb">The db if any.</param>
        /// <returns>True if a db was loaded, false otherwise.</returns>
        public static bool TryLoad(string path, out OsmTiledHistoryDb? osmDb)
        {
            if (FileSystemFacade.FileSystem.Exists(
                OsmTiledDbOperations.PathToMeta(path)))
            {
                osmDb = new OsmTiledHistoryDb(path);
                return true;
            }

            osmDb = null;
            return false;
        }

        /// <summary>
        /// Tries to reload the database.
        /// </summary>
        /// <returns>True if the database was modified.</returns>
        public bool TryReload()
        {
            try
            {
                var meta = OsmTiledHistoryDbOperations.LoadDbMeta(_path);
                lock (_diffSync)
                {
                    if (meta.Latest != _meta.Latest)
                    {
                        this.Latest = this.GetDb(meta.Latest);
                        _meta = meta;
                        return true;
                    }
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
            return $"{_path}: {this.Latest.Path}@{this.Latest.Timestamp}";
        }
    }
}