using System.IO;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.Snapshots;

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