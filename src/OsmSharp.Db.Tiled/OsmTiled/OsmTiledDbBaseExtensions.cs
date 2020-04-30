using System.Collections.Generic;

namespace OsmSharp.Db.Tiled.OsmTiled
{
    public static class OsmTiledDbBaseExtensions
    {
        /// <summary>
        /// Gets the object for the given type/id.
        /// </summary>
        /// <param name="db">The db.</param>
        /// <param name="type">The type.</param>
        /// <param name="id">The id.</param>
        /// <param name="buffer">The buffer.</param>
        /// <returns>The object if present.</returns>
        public static OsmGeo? Get(this OsmTiledDbBase db, OsmGeoType type, long id, byte[]? buffer = null)
        {
            return db.Get(new OsmGeoKey(type, id), buffer);
        }
        
        /// <summary>
        /// Gets the tiles the object for the given type/id exists in, if any.
        /// </summary>
        /// <param name="db">The db.</param>
        /// <param name="type">The type.</param>
        /// <param name="id">The id.</param>
        /// <returns>The tiles, if any.</returns>
        public static IEnumerable<(uint x, uint y)> GetTiles(this OsmTiledDbBase db, OsmGeoType type, long id)
        {
            return db.GetTiles(new OsmGeoKey(type, id));
        }
    }
}