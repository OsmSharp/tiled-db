namespace OsmSharp.Db.Tiled
{
    /// <summary>
    /// Represents OSM db meta.
    /// </summary>
    internal class OsmTiledHistoryDbMeta
    {
        /// <summary>
        /// Gets the latest snapshot db.
        /// </summary>
        public string Latest { get; set; } = string.Empty;
    }
}