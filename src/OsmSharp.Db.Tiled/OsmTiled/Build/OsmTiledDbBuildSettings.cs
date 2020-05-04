namespace OsmSharp.Db.Tiled.OsmTiled.Build
{
    /// <summary>
    /// Build settings.
    /// </summary>
    public class OsmTiledDbBuildSettings
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

        internal void Prepare(OsmGeo osmGeo)
        {
            if (!this.IncludeChangeset) osmGeo.ChangeSetId = null;
            if (!this.IncludeUsername) osmGeo.UserName = null;
            if (!this.IncludeUserId) osmGeo.UserId = null;
            if (!this.IncludeVisible) osmGeo.Visible = null;
        }
    }
}