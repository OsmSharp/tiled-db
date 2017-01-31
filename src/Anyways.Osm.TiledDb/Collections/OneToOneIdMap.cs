using Reminiscence.Arrays;
using System;
using System.IO;

namespace Anyways.Osm.TiledDb.Collections
{
    /// <summary>
    /// A one-to-one id map, maps id's to tile id's.
    /// </summary>
    public class OneToOneIdMap
    {
        private readonly ArrayBase<long> _ids;
        private readonly ArrayBase<ulong> _tileIds;
        private readonly int BlockSize = 1000;

        /// <summary>
        /// Creates a new one-to-one map.
        /// </summary>
        public OneToOneIdMap()
        {
            _ids = new MemoryArray<long>(BlockSize);
            _tileIds = new MemoryArray<ulong>(BlockSize);
        }

        private long _nextIndex = 0;

        /// <summary>
        /// Adds a new id to this map.
        /// </summary>
        public void Add(long id, ulong tileid)
        {
            if (_nextIndex > 0 &&
                _ids[_nextIndex - 1] > id)
            {
                throw new InvalidOperationException("Id's can only be added from lower->higher id's.");
            }
            if (_nextIndex + 1 >= _ids.Length)
            {
                _ids.Resize(_ids.Length + BlockSize);
                _tileIds.Resize(_tileIds.Length + BlockSize);
            }

            _ids[_nextIndex + 0] = id;
            _tileIds[_nextIndex + 0] = tileid;
            _nextIndex++;
        }

        /// <summary>
        /// Gets the tile id for the given node id.
        /// </summary>
        public ulong Get(long id)
        {
            var index = this.TryGetIndex(id);
            if (index == long.MaxValue)
            {
                return ulong.MaxValue;
            }
            return _tileIds[index];
        }

        /// <summary>
        /// Returns true if this map is readonly.
        /// </summary>
        public bool IsReadonly
        {
            get
            {
                return false;
            }
        }

        /// <summary>
        /// Serializes this map to the given stream.
        /// </summary>
        public long Serialize(Stream stream)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Deserializes a map from the given stream.
        /// </summary>
        public static OneToOneIdMap Deserialize(Stream stream)
        {
            throw new NotImplementedException();
        }
        
        private long TryGetIndex(long id)
        {
            long bottom = 0;
            long top = _nextIndex - 1;
            long bottomId = _ids[bottom];
            if (id == bottomId)
            {
                return bottom;
            }
            long topId = _ids[top];
            if (id == topId)
            {
                while (top - 1 > 0 &&
                    _ids[top - 1] == id)
                {
                    top--;
                }
                return top;
            }

            while (top - bottom > 1)
            {
                var middle = (((top - bottom) / 2) + bottom);
                var middleId = _ids[middle];
                if (middleId == id)
                {
                    while (middle - 1 > 0 &&
                        _ids[middle - 1] == id)
                    {
                        middle--;
                    }
                    return middle;
                }
                if (middleId > id)
                {
                    topId = middleId;
                    top = middle;
                }
                else
                {
                    bottomId = middleId;
                    bottom = middle;
                }
            }

            return long.MaxValue;
        }
    }
}