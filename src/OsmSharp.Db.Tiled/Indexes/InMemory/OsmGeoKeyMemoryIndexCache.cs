using System;
using System.Collections.Generic;
using OsmSharp.Db.Tiled.Tiles;

namespace OsmSharp.Db.Tiled.Indexes.InMemory
{
    internal class OsmGeoKeyMemoryIndexCache
    {
        private readonly Dictionary<Tile, OsmGeoKeyMemoryIndex> _indexes;

        public OsmGeoKeyMemoryIndexCache()
        {
            _indexes = new Dictionary<Tile, OsmGeoKeyMemoryIndex>();
        }

        public bool TryGet(Tile tile, out OsmGeoKeyMemoryIndex index)
        {
            return _indexes.TryGetValue(tile, out index);
        }

        public void AddOrUpdate(Tile tile, OsmGeoKeyMemoryIndex index)
        {
            _indexes[tile] = index;
        }

        public IEnumerable<(Tile tile, OsmGeoKeyMemoryIndex index)> GetAll()
        {
            foreach (var pair in _indexes)
            {
                yield return (pair.Key, pair.Value);
            }
        }
    }
}