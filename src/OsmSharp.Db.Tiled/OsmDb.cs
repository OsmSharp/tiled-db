using System.IO;
using OsmSharp.Changesets;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.Snapshots;
using OsmSharp.Db.Tiled.Snapshots.Build;

namespace OsmSharp.Db.Tiled
{
    /// <summary>
    /// Represents a full OSM db including history built up by snapshots and diffs.
    /// </summary>
    public class OsmDb
    {
        private readonly string _path;
        private readonly OsmDbMeta _meta;

        /// <summary>
        /// Creates a new OSM db.
        /// </summary>
        /// <param name="path">The path.</param>
        public OsmDb(string path)
        {
            _path = path;

            _meta = OsmDbOperations.LoadDbMeta(_path);
            
            this.Latest = new SnapshotDbFull(_meta.Latest);
        }

        /// <summary>
        /// Gets the latest snapshot db.
        /// </summary>
        public Snapshots.SnapshotDb Latest { get; private set; }

        private object _diffSync = new object();

        /// <summary>
        /// Applies a diff to this OSM db.
        /// </summary>
        /// <remarks>
        /// This does not update the latest snapshot but makes a new latest snapshot.
        /// </remarks>
        /// <param name="diff"></param>
        public void ApplyDiff(OsmChange diff)
        {
            lock (_diffSync)
            {
                this.Latest = this.Latest.BuildDiff(_path, diff);
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
    }
}