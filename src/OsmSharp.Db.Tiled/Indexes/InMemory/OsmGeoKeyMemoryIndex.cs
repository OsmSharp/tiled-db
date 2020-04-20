//using System;
//using System.Collections;
//using System.Collections.Generic;
//
//namespace OsmSharp.Db.Tiled.Indexes.InMemory
//{
//    internal class OsmGeoKeyMemoryIndex : IEnumerable<(OsmGeoType type, long id, int mask)>
//    {        
//        private readonly MemoryIndex? _nodeIndex;
//        private readonly MemoryIndex? _wayIndex;
//        private readonly MemoryIndex? _relationIndex;
//
//        public OsmGeoKeyMemoryIndex(MemoryIndex? nodeIndex = null, MemoryIndex? wayIndex = null, MemoryIndex? relationIndex = null)
//        {
//            _nodeIndex = nodeIndex ?? new MemoryIndex();
//            _wayIndex = wayIndex ?? new MemoryIndex();
//            _relationIndex = relationIndex ?? new MemoryIndex();
//        }
//        
//        public void Add(OsmGeoType type, long id, int mask)
//        {
//            switch (type)
//            {
//                case OsmGeoType.Node:
//                    _nodeIndex.Add(id, mask);
//                    return;
//                case OsmGeoType.Way:
//                    _wayIndex.Add(id, mask);
//                    return;
//                case OsmGeoType.Relation:
//                    _relationIndex.Add(id, mask);
//                    return;
//                default:
//                    throw new ArgumentOutOfRangeException(nameof(type), type, null);
//            }
//        }
//
//        public bool TryGetMask(OsmGeoType type, long id, out int mask)
//        {            
//            switch (type)
//            {
//                case OsmGeoType.Node:
//                    return _nodeIndex.TryGetMask(id, out mask);
//                case OsmGeoType.Way:
//                    return _wayIndex.TryGetMask(id, out mask);
//                case OsmGeoType.Relation:
//                    return _relationIndex.TryGetMask(id, out mask);
//                default:
//                    throw new ArgumentOutOfRangeException(nameof(type), type, null);
//            }
//        }
//
//        public OsmGeoKeyIndex ToIndex()
//        {
//            var index = new OsmGeoKeyIndex();
//
//            foreach (var (type, id, mask) in this)
//            {
//                index.Add(type, id, mask);
//            }
//
//            return index;
//        }
//
//        public static OsmGeoKeyMemoryIndex From(OsmGeoKeyIndex loadedIndex)
//        {
//            var memoryIndex = new OsmGeoKeyMemoryIndex();
//
//            foreach (var (type, id, mask) in loadedIndex)
//            {
//                memoryIndex.Add(type, id, mask);
//            }
//
//            return memoryIndex;
//        }
//        
//        public IEnumerator<(OsmGeoType type, long id, int mask)> GetEnumerator()
//        {
//            IEnumerable<(OsmGeoType type,long id, int mask)> Enumerable()
//            {
//                foreach (var (id, mask) in _nodeIndex)
//                {
//                    yield return (OsmGeoType.Node, id, mask);
//                }
//                
//                foreach (var (id, mask) in _wayIndex)
//                {
//                    yield return (OsmGeoType.Way, id, mask);
//                }
//                
//                foreach (var (id, mask) in _relationIndex)
//                {
//                    yield return (OsmGeoType.Relation, id, mask);
//                }
//            }
//
//            return Enumerable().GetEnumerator();
//        }
//
//        IEnumerator IEnumerable.GetEnumerator()
//        {
//            return GetEnumerator();
//        }
//    }
//}