using System.Collections.Generic;
using System.Linq;

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
            var result =  db.Get(new OsmGeoKey(type, id), buffer);
            return result?.osmGeo;
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
            return db.GetTilesFor(new OsmGeoKey(type, id));
        }
        
        /// <summary>
        /// Gets the object and the tiles it's in for the given type/id.
        /// </summary>
        /// <param name="db">The db.</param>
        /// <param name="osmGeoKey">The key.</param>
        /// <param name="buffer">The buffer.</param>
        /// <returns>The object if present.</returns>
        public static (OsmGeo osmGeo, IEnumerable<(uint x, uint y)>)? Get(this OsmTiledDbBase db, OsmGeoKey osmGeoKey,
            byte[]? buffer = null)
        {
            return db.Get(new[] {osmGeoKey}, buffer).FirstOrDefault();
        }

        /// <summary>
        /// Get the tile the given object exists in, if any.
        /// </summary>
        /// <param name="db">The db.</param>
        /// <param name="key">The key.</param>
        /// <returns>The tiles if any.</returns>
        public static IEnumerable<(uint x, uint y)> GetTilesFor(this OsmTiledDbBase db, OsmGeoKey key)
        {
            var tiles = db.GetTilesFor(new[] {key}).FirstOrDefault().tiles;
            if (tiles == null) yield break;
            
            foreach (var (x, y) in tiles)
            {
                yield return (x, y);
            }
        }

        /// <summary>
        /// Gets all the data in the given tile.
        /// </summary>
        /// <param name="db">The db.</param>
        /// <param name="tile">The tile.</param>
        /// <param name="completeWays">When true all way nodes will be included, if found.</param>
        /// <param name="completeRelations">When true all relation members will be included, if found.</param>
        /// <param name="buffer">The buffer.</param>
        /// <returns>All objects in the given tile(s).</returns>
        public static IEnumerable<OsmGeo> Get(this OsmTiledDbBase db,(uint x, uint y) tile, 
            bool completeWays = false, bool completeRelations = false, byte[]? buffer = null)
        {
            var data = db.Get(new [] {tile}, buffer).Select(x => x.osmGeo);
            
            if (completeWays || completeRelations)
            {
                var sortedList = new SortedList<OsmGeoKey, OsmGeo>();
                foreach (var osmGeo in data)
                {
                    var osmGeoKey = new OsmGeoKey(osmGeo);
                    if (!sortedList.ContainsKey(osmGeoKey)) sortedList.Add(osmGeoKey, osmGeo);

                    if (completeWays &&
                        osmGeo is Way way)
                    {
                        foreach (var n in way.Nodes)
                        {
                            var key = new OsmGeoKey(OsmGeoType.Node, n);
                            if (sortedList.ContainsKey(key)) continue;
                            
                            var node = db.Get(OsmGeoType.Node, n, buffer);
                            if (node == null) continue;
                            sortedList.Add(new OsmGeoKey(node), node);
                        }
                    }
                    if (completeRelations &&
                        osmGeo is Relation relation)
                    {
                        foreach (var m in relation.Members)
                        {
                            var key = new OsmGeoKey(m.Type, m.Id);
                            if (sortedList.ContainsKey(key)) continue;
                            
                            var member = db.Get(key, buffer);
                            if (member == null) continue;
                            sortedList.Add(new OsmGeoKey(member.Value.osmGeo), member.Value.osmGeo);
                        }
                    }
                }

                return sortedList.Values;
            }

            return data;
        }
    }
}