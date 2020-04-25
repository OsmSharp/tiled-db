namespace OsmSharp.Db.Tiled.OsmTiled
{
    /// <summary>
    /// Tiled db load settings.
    /// </summary>
    public class OsmTiledDbSettings
    {
        /// <summary>
        /// Gets the flag to open the db as a reader only.
        /// </summary>
        public bool AsReader { get; set; } = false;
    }
}