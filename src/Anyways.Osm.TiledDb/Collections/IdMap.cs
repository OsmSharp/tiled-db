//using System;
//using System.Collections.Generic;

//namespace Anyways.Osm.TiledDb.Collections
//{
//    /// <summary>
//    /// A datastructure to map nodes to their vertex-equivalents.
//    /// </summary>
//    /// <remarks>
//    /// Why not use a regular dictionary?
//    /// - Size limitations of one object.
//    /// - We can have in some exceptional circumstances have two vertices for one original node.
//    /// </remarks>
//    public class IdMap
//    {
//        private readonly UniqueTileIdMap _firstMap; // holds the first vertex for a node, will be enough in 99.9% of cases.
//        private readonly HugeDictionary<long, LinkedListNode> _secondMap; // holds the second and beyond vertices for a node.

//        /// <summary>
//        /// Creates a new core node id map.
//        /// </summary>
//        public IdMap()
//        {
//            _firstMap = new UniqueTileIdMap();
//            _secondMap = new HugeDictionary<long, LinkedListNode>();
//        }

//        /// <summary>
//        /// Adds a pair.
//        /// </summary>
//        public void Add(long id, ulong tileId)
//        {
//            var existingTile = _firstMap[id];
//            if (existingTile == ulong.MaxValue)
//            {
//                _firstMap[id] = tileId;
//                return;
//            }
//            if (existingTile == tileId)
//            {
//                return;
//            }
//            LinkedListNode existing;
//            if (!_secondMap.TryGetValue(id, out existing))
//            {
//                _secondMap.Add(id, new LinkedListNode()
//                {
//                    Value = tileId
//                });
//            }
//            var current = _secondMap[id];
//            while(current != null)
//            {
//                if (current.Value == tileId)
//                {
//                    return;
//                }
//                current = current.Next;
//            }
//            _secondMap[id] = new LinkedListNode()
//            {
//                Value = tileId,
//                Next = existing
//            };
//        }

//        /// <summary>
//        /// Fills the given array with the vertices for the given node.
//        /// </summary>
//        public int Get(long id, ref byte[] tiles)
//        {
//            if (tiles == null || tiles.Length == 0) { throw new ArgumentException("Target array needs to be non-null and have a size > 0."); }
//            var first = _firstMap[id];
//            if (first == byte.MaxValue)
//            {
//                return 0;
//            }
//            tiles[0] = first;

//            LinkedListNode node;
//            if (!_secondMap.TryGetValue(id, out node))
//            {
//                return 1;
//            }
//            var i = 1;
//            while (i < tiles.Length && node != null)
//            {
//                tiles[i] = node.Value;
//                node = node.Next;
//                i++;
//            }
//            return i;
//        }

//        /// <summary>
//        /// Calculates the maximum vertices per node in this map.
//        /// </summary>
//        /// <returns></returns>
//        public int MaxVerticePerNode()
//        {
//            var max = 1;
//            foreach (var keyValue in _secondMap)
//            {
//                var c = 1;
//                var node = keyValue.Value.Next;
//                while (node != null)
//                {
//                    c++;
//                    node = node.Next;
//                }
//                if (c + 1 > max)
//                {
//                    max = c + 1;
//                }
//            }
//            return max;
//        }

//        private class LinkedListNode
//        {
//            public byte Value { get; set; }
//            public LinkedListNode Next { get; set; }
//        }
//    }
//}
