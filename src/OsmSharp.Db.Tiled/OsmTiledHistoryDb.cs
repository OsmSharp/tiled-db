using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.OsmTiled;
using OsmSharp.Db.Tiled.OsmTiled.IO;

namespace OsmSharp.Db.Tiled
{
    /// <summary>
    /// Represents a full OSM db including history built up by snapshots and diffs.
    /// </summary>
    public class OsmTiledHistoryDb
    {
        private readonly string _path;
        private OsmTiledHistoryDbMeta _meta;

        /// <summary>
        /// Creates a new OSM db.
        /// </summary>
        /// <param name="path">The path.</param>
        public OsmTiledHistoryDb(string path)
        {
            _path = path;

            _meta = OsmTiledHistoryDbOperations.LoadDbMeta(_path);
            
            this.Latest = OsmTiledDbOperations.LoadDb(FileSystemFacade.FileSystem.Combine(_path, _meta.Latest));
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
        public static async Task<OsmTiledHistoryDb> Create(string path, IEnumerable<OsmGeo> data)
        {
            return await Build.OsmTiledHistoryDbBuilder.Build(data, path);
        }
        
        /// <summary>
        /// Adds a new osm tiled db as latest using the given data.
        /// </summary>
        /// <param name="data">The data.</param>
        public async Task Update(IEnumerable<OsmGeo> data)
        {
            var latest = await Build.OsmTiledHistoryDbBuilder.Update(data, this._path);
            
            lock (_diffSync)
            {
                // update data.
                this.Latest = latest;
                
                // update meta data.
                _meta = new OsmTiledHistoryDbMeta()
                {
                    Latest =  FileSystemFacade.FileSystem.RelativePath(this._path, this.Latest.Path)
                };
                OsmTiledHistoryDbOperations.SaveDbMeta(_path, _meta);
            }
        }

//        /// <summary>
//        /// Applies a diff to this OSM db.
//        /// </summary>
//        /// <remarks>
//        /// This does not update the latest snapshot but makes a new latest snapshot.
//        /// </remarks>
//        /// <param name="diff">The changeset.</param>
//        /// <param name="timeStamp">The timestamp from the diff meta-data override the timestamps in the data.</param>
//        public void ApplyDiff(OsmChange diff, DateTime? timeStamp = null)
//        {
//            lock (_diffSync)
//            {
//                // update data.
//                this.Latest = this.Latest.BuildDiff(diff, timeStamp);
//                
//                // update meta data.
//                _meta = new OsmDbMeta()
//                {
//                    Latest = this.Latest.Path
//                };
//                OsmDbOperations.SaveDbMeta(_path, _meta);
//            }
//        }
        
//        /// <summary>
//        /// Take the last db and convert it into a snapshot.
//        /// </summary>
//        public void TakeSnapshot()
//        {
//            lock (_diffSync)
//            {
//                // update data.
//                this.Latest = this.Latest.Build();
//                
//                // update meta data.
//                _meta = new OsmDbMeta()
//                {
//                    Latest = this.Latest.Path
//                };
//                OsmDbOperations.SaveDbMeta(_path, _meta);
//            }
//        }

        /// <summary>
        /// Try to load an OSM db from the given path.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <param name="osmDb">The db if any.</param>
        /// <returns>True if a db was loaded, false otherwise.</returns>
        public static bool TryLoad(string path, out OsmTiledHistoryDb? osmDb)
        {
            if (FileSystemFacade.FileSystem.Exists(OsmTiledDbOperations.PathToMeta(path)))
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
                        var latest = OsmTiledDbOperations.LoadDb(meta.Latest);

                        this.Latest = latest;
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