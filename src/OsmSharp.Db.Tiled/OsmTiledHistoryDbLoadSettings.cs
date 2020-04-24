namespace OsmSharp.Db.Tiled
{
    /// <summary>
    /// History db load settings.
    /// </summary>
    public class OsmTiledHistoryDbLoadSettings
    {
        /// <summary>
        /// Gets the flag to open the db as a reader.
        /// </summary>
        public bool AsReader { get; set; } = false;
    }
}