//using System.Collections.Generic;
//using OsmSharp.Db.Tiled.Tiles;
//
//namespace OsmSharp.Db.Tiled.Indexes
//{
//    internal class OsmGeoKeyIndexCache
//    {
//        private readonly Dictionary<Tile, OsmGeoKeyIndex> _indexes;
//
//        public OsmGeoKeyIndexCache()
//        {
//            _indexes = new Dictionary<Tile, OsmGeoKeyIndex>();
//        }
//
//        public bool TryGet(Tile tile, out OsmGeoKeyIndex index)
//        {
//            return _indexes.TryGetValue(tile, out index);
//        }
//
//        public void AddOrUpdate(Tile tile, OsmGeoKeyIndex index)
//        {
//            _indexes[tile] = index;
//        }
//
//        public IEnumerable<(Tile tile, OsmGeoKeyIndex index)> GetAll()
//        {
//            foreach (var pair in _indexes)
//            {
//                yield return (pair.Key, pair.Value);
//            }
//        }
//    }
//}