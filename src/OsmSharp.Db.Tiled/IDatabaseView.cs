using System.Collections.Generic;
using OsmSharp.Changesets;
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
        /// <param name="type">The type to get the data for.</param>
        /// <returns>The data in the given tile.</returns>
        IEnumerable<OsmGeo> GetTile(Tile tile, OsmGeoType type);

        /// <summary>
        /// Applies a changeset and returns the new view including the given changeset.
        /// </summary>
        /// <param name="changeset">The changeset.</param>
        /// <param name="path">The path to place the new view.</param>
        /// <returns>A new view including the changeset.</returns>
        IDatabaseView ApplyChangeset(OsmChange changeset, string path = null);
    }
}