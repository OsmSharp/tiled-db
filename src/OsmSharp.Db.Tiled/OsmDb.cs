using System;
using System.IO;
using OsmSharp.Changesets;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.Snapshots;
using OsmSharp.Db.Tiled.Snapshots.Build;
using OsmSharp.Db.Tiled.Snapshots.IO;

namespace OsmSharp.Db.Tiled
{
    /// <summary>
    /// Represents a full OSM db including history built up by snapshots and diffs.
    /// </summary>
    public class OsmDb
    {
        private readonly string _path;
        private OsmDbMeta _meta;

        /// <summary>
        /// Creates a new OSM db.
        /// </summary>
        /// <param name="path">The path.</param>
        public OsmDb(string path)
        {
            _path = path;

            _meta = OsmDbOperations.LoadDbMeta(_path);
            
            this.Latest = SnapshotDbOperations.LoadDb(_meta.Latest);
        }

        /// <summary>
        /// Gets the latest snapshot db.
        /// </summary>
        public Snapshots.SnapshotDb Latest { get; private set; }

        private readonly object _diffSync = new object();

        /// <summary>
        /// Applies a diff to this OSM db.
        /// </summary>
        /// <remarks>
        /// This does not update the latest snapshot but makes a new latest snapshot.
        /// </remarks>
        /// <param name="diff">The changeset.</param>
        /// <param name="timeStamp">The timestamp from the diff meta-data override the timestamps in the data.</param>
        public void ApplyDiff(OsmChange diff, DateTime? timeStamp = null)
        {
            lock (_diffSync)
            {
                // update data.
                this.Latest = this.Latest.BuildDiff(diff, timeStamp);
                
                // update meta data.
                _meta = new OsmDbMeta()
                {
                    Latest = this.Latest.Path
                };
                OsmDbOperations.SaveDbMeta(_path, _meta);
            }
        }
        
        /// <summary>
        /// Take the last db and convert it into a snapshot.
        /// </summary>
        public void TakeSnapshot()
        {
            lock (_diffSync)
            {
                // update data.
                this.Latest = this.Latest.Build();
                
                // update meta data.
                _meta = new OsmDbMeta()
                {
                    Latest = this.Latest.Path
                };
                OsmDbOperations.SaveDbMeta(_path, _meta);
            }
        }

        /// <summary>
        /// Try to load an OSM db from the given path.
        /// </summary>
        /// <param name="path">The path.</param>
        /// <param name="osmDb">The db if any.</param>
        /// <returns>True if a db was loaded, false otherwise.</returns>
        public static bool TryLoad(string path, out OsmDb osmDb)
        {
            if (File.Exists(OsmDbOperations.PathToMeta(path)))
            {
                osmDb = new OsmDb(path);
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
                var meta = OsmDbOperations.LoadDbMeta(_path);
                lock (_diffSync)
                {
                    if (meta.Latest != _meta.Latest)
                    {
                        var latest = SnapshotDbOperations.LoadDb(meta.Latest);

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