using System.Collections.Generic;
using OsmSharp.Db.Tiled.Tiles;

namespace OsmSharp.Db.Tiled
{
    /// <summary>
    /// Abstract representation of a database view.
    /// </summary>
    public interface IDatabaseView
    {
        /// <summary>
        /// Gets the object with the given type and id.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <param name="id">The id.</param>
        /// <returns>The object.</returns>
        OsmGeo Get(OsmGeoType type, long id);
        
        /// <summary>
        /// Gets the data in the given tile.
        /// </summary>
        /// <param name="tile">The tile to get the data for.</param>
        /// <returns>The data in the given tile.</returns>
        IEnumerable<OsmGeo> GetTile(Tile tile);
    }
}