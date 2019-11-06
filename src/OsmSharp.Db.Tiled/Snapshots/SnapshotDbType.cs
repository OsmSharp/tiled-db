namespace OsmSharp.Db.Tiled.Snapshots
{
    /// <summary>
    /// Holds the types of snapshots.
    /// </summary>
    public static class SnapshotDbType
    {
        /// <summary>
        /// A full snapshot.
        /// </summary>
        public const string Full = "Full";
        
        /// <summary>
        /// A diff snapshot.
        /// </summary>
        public const string Diff = "Diff";
        
        /// <summary>
        /// A snapshot snapshot.
        /// </summary>
        public const string Snapshot = "Snapshot";
    }
}