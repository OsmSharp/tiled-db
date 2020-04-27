namespace OsmSharp.Db.Tiled.OsmTiled
{
    internal static class OsmTiledDbConstants
    {
        /// <summary>
        /// A tile id that doesn't exist to place deleted objects in.
        /// </summary>
        public const uint DeletedTile = uint.MaxValue - 1;
    }
}