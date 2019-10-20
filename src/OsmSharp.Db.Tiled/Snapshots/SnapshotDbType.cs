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
        public static string Full = "Full";
        
        /// <summary>
        /// A diff snapshot.
        /// </summary>
        public static string Diff = "Diff";
    }
}