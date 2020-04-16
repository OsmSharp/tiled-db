using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using OsmSharp.Db.Tiled.IO;

namespace OsmSharp.Db.Tiled.Indexes
{
    internal class OsmGeoKeyIndex : IEnumerable<(OsmGeoType type, long id, int mask)>
    {
        private readonly Index? _nodeIndex;
        private readonly Index? _wayIndex;
        private readonly Index? _relationIndex;

        public OsmGeoKeyIndex(Index? nodeIndex = null, Index? wayIndex = null, Index? relationIndex = null)
        {
            _nodeIndex = nodeIndex ?? new Index();
            _wayIndex = wayIndex ?? new Index();
            _relationIndex = relationIndex ?? new Index();
        }
        
        public void Add(OsmGeoType type, long id, int mask)
        {
            switch (type)
            {
                case OsmGeoType.Node:
                    _nodeIndex.Add(id, mask);
                    return;
                case OsmGeoType.Way:
                    _wayIndex.Add(id, mask);
                    return;
                case OsmGeoType.Relation:
                    _relationIndex.Add(id, mask);
                    return;
                default:
                    throw new ArgumentOutOfRangeException(nameof(type), type, null);
            }
        }

        public bool TryGetMask(OsmGeoType type, long id, out int mask)
        {            
            switch (type)
            {
                case OsmGeoType.Node:
                    return _nodeIndex.TryGetMask(id, out mask);
                case OsmGeoType.Way:
                    return _wayIndex.TryGetMask(id, out mask);
                case OsmGeoType.Relation:
                    return _relationIndex.TryGetMask(id, out mask);
                default:
                    throw new ArgumentOutOfRangeException(nameof(type), type, null);
            }
        }
        
        public IEnumerator<(OsmGeoType type, long id, int mask)> GetEnumerator()
        {
            IEnumerable<(OsmGeoType type,long id, int mask)> Enumerable()
            {
                foreach (var (id, mask) in _nodeIndex)
                {
                    yield return (OsmGeoType.Node, id, mask);
                }
                
                foreach (var (id, mask) in _wayIndex)
                {
                    yield return (OsmGeoType.Way, id, mask);
                }
                
                foreach (var (id, mask) in _relationIndex)
                {
                    yield return (OsmGeoType.Relation, id, mask);
                }
            }

            return Enumerable().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public long Serialize(Stream stream)
        {
            var position = stream.Position;
            stream.Seek(position + 8 * 23, SeekOrigin.Begin);

            long nodeIndexSize = 0;
            if (_nodeIndex != null) nodeIndexSize = _nodeIndex.Serialize(stream);
            long wayIndexSize = 0;
            if (_wayIndex != null) wayIndexSize = _wayIndex.Serialize(stream);
            long relationIndexSize = 0;
            if (_relationIndex != null) relationIndexSize = _relationIndex.Serialize(stream);
            var size = stream.Position - position;

            stream.Seek(0, SeekOrigin.Begin);
            stream.Write(BitConverter.GetBytes(nodeIndexSize), 0, 8);
            stream.Write(BitConverter.GetBytes(wayIndexSize), 0, 8);
            stream.Write(BitConverter.GetBytes(relationIndexSize), 0, 8);

            return size;
        }

        public static OsmGeoKeyIndex Deserialize(Stream stream)
        {
            var nodeIndexSize = stream.ReadInt64();
            var wayIndexSize = stream.ReadInt64();
            var relationIndexSize = stream.ReadInt64();

            var nodeIndex = Index.Deserialize(stream);
            var wayIndex = Index.Deserialize(stream);
            var relationIndex = Index.Deserialize(stream);
            
            return new OsmGeoKeyIndex(nodeIndex, wayIndex, relationIndex);
        }
    }
}