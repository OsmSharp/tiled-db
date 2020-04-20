//using System;
//using System.Collections.Generic;
//using OsmSharp.Db.Tiled.Tiles;
//
//namespace OsmSharp.Db.Tiled.Indexes.Cache
//{
//    internal class IndexCache
//    {
//        private readonly Dictionary<Tile, Index> _nodesIndex;
//        private readonly Dictionary<Tile, Index> _waysIndex;
//        private readonly Dictionary<Tile, Index> _relationsIndex;
//
//        public IndexCache()
//        {
//            _nodesIndex = new Dictionary<Tile, Index>();
//            _waysIndex = new Dictionary<Tile, Index>();
//            _relationsIndex = new Dictionary<Tile, Index>();
//        }
//
//        public bool TryGet(Tile tile, OsmGeoType type, out Index index)
//        {
//            switch (type)
//            {
//                case OsmGeoType.Node:
//                    return _nodesIndex.TryGetValue(tile, out index);
//                case OsmGeoType.Way:
//                    return _waysIndex.TryGetValue(tile, out index);
//                case OsmGeoType.Relation:
//                    return _relationsIndex.TryGetValue(tile, out index);
//            }
//
//            throw new Exception();
//        }
//
//        public void AddOrUpdate(Tile tile, OsmGeoType type, Index index)
//        {
//            switch (type)
//            {
//                case OsmGeoType.Node:
//                    _nodesIndex[tile] = index;
//                    break;
//                case OsmGeoType.Way:
//                    _waysIndex[tile] = index;
//                    break;
//                case OsmGeoType.Relation:
//                    _relationsIndex[tile] = index;
//                    break;
//            }
//        }
//
//        public IEnumerable<(Tile tile, OsmGeoType type, Index index)> GetAll()
//        {
//            foreach (var pair in _nodesIndex)
//            {
//                yield return (pair.Key, OsmGeoType.Node, pair.Value);
//            }
//
//            foreach (var pair in _waysIndex)
//            {
//                yield return (pair.Key, OsmGeoType.Way, pair.Value);
//            }
//
//            foreach (var pair in _relationsIndex)
//            {
//                yield return (pair.Key, OsmGeoType.Relation, pair.Value);
//            }
//        }
//    }
//}