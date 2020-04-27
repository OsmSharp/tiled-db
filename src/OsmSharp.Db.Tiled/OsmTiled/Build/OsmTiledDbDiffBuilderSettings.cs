namespace OsmSharp.Db.Tiled.OsmTiled.Build
{
    /// <summary>
    /// Build settings.
    /// </summary>
    public class OsmTiledDbDiffBuildSettings
    {
        /// <summary>
        /// Include user name flag.
        /// </summary>
        public bool IncludeUsername { get; set; } = false;

        /// <summary>
        /// Include user id flag.
        /// </summary>
        public bool IncludeUserId { get; set; } = false;

        /// <summary>
        /// Include changeset flag.
        /// </summary>
        public bool IncludeChangeset { get; set; } = false;

        /// <summary>
        /// Include visible flag.
        /// </summary>
        public bool IncludeVisible { get; set; } = false;
    }
}