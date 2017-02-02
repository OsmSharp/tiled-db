using Reminiscence.Arrays;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;

namespace Anyways.Osm.TiledDb.Indexing
{
    /// <summary>
    /// A one-to-one id map, maps id's to tile id's.
    /// </summary>
    public class OneToOneIdMap : IEnumerable<long>
    {
        private readonly ArrayBase<long> _ids;
        private readonly ArrayBase<ulong> _tileIds;
        private readonly int BlockSize = 100000;

        /// <summary>
        /// Creates a new one-to-one map.
        /// </summary>
        public OneToOneIdMap()
        {
            _ids = new MemoryArray<long>(BlockSize);
            _tileIds = new MemoryArray<ulong>(BlockSize);
        }

        /// <summary>
        /// Creates a new one-to-one map.
        /// </summary>
        private OneToOneIdMap(long nextIndex, ArrayBase<long> ids, ArrayBase<ulong> tileIds)
        {
            _nextIndex = nextIndex;
            _ids = ids;
            _tileIds = tileIds;
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
            return this.GetTileId(index);
        }

        /// <summary>
        /// Gets the tile id for the given index.
        /// </summary>
        private ulong GetTileId(long index)
        {
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
        /// Merges the two maps together into one.
        /// </summary>
        public static OneToOneIdMap Merge(OneToOneIdMap map1, OneToOneIdMap map2)
        {
            var enumerator1 = new OneToOneEnumerator(map1);
            var enumerator2 = new OneToOneEnumerator(map2);

            var map = new OneToOneIdMap();

            var next1 = enumerator1.MoveNext();
            var next2 = enumerator2.MoveNext();
            
            while (next1 || next2)
            {
                if (next1 && next2)
                {
                    var lowest = enumerator1.Current;
                    if (enumerator2.Current < lowest)
                    {
                        lowest = enumerator2.Current;
                    }

                    var tileId = ulong.MaxValue;
                    if (enumerator2.Current == lowest)
                    {
                        tileId = enumerator2.TileId;
                    }
                    if (enumerator1.Current == lowest)
                    {
                        tileId = enumerator1.TileId;
                    }

                    map.Add(lowest, tileId);

                    if (enumerator1.Current == enumerator2.Current)
                    {
                        next1 = enumerator1.MoveNext();
                        next2 = enumerator2.MoveNext();
                    }
                    else if (enumerator1.Current < enumerator2.Current)
                    {
                        next1 = enumerator1.MoveNext();
                    }
                    else
                    {
                        next2 = enumerator2.MoveNext();
                    }
                }
                else if (next1)
                {
                    var tileId = enumerator1.TileId;
                    map.Add(enumerator1.Current, tileId);

                    next1 = enumerator1.MoveNext();
                }
                else if (next2)
                {
                    var tileId = enumerator2.TileId;
                    map.Add(enumerator2.Current, tileId);

                    next2 = enumerator2.MoveNext();
                }
            }

            return map;
        }

        /// <summary>
        /// Serializes this map to the given stream.
        /// </summary>
        public long Serialize(Stream stream)
        {
            long size = 0;
            var sizeBytes = BitConverter.GetBytes(_nextIndex);
            stream.Write(sizeBytes, 0, 8);
            size += 8;

            _ids.Resize(_nextIndex);
            _tileIds.Resize(_nextIndex);
            size += _ids.CopyTo(stream);
            size += _tileIds.CopyTo(stream);

            return size;
        }

        /// <summary>
        /// Deserializes a map from the given stream.
        /// </summary>
        public static OneToOneIdMap Deserialize(Stream stream, bool mapped = false)
        {
            var sizeBytes = new byte[8];
            stream.Read(sizeBytes, 0, 8);
            var size = BitConverter.ToInt64(sizeBytes, 0);
            
            if (mapped)
            {
                throw new NotSupportedException("Load an index in a mapped way is not yet supported.");
            }

            var ids = new MemoryArray<long>(size);
            ids.CopyFrom(stream);
            var tileIds = new MemoryArray<ulong>(size);
            tileIds.CopyFrom(stream);

            return new OneToOneIdMap(size, ids, tileIds);
        }
        
        private long TryGetIndex(long id)
        {
            if (_nextIndex == 0)
            {
                return long.MaxValue;
            }

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

        /// <summary>
        /// Gets the enumerator.
        /// </summary>
        public IEnumerator<long> GetEnumerator()
        {
            return new OneToOneEnumerator(this);
        }

        /// <summary>
        /// Gets the enumerator.
        /// </summary>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return new OneToOneEnumerator(this);
        }

        private class OneToOneEnumerator : IEnumerator<long>
        {
            private readonly OneToOneIdMap _map;

            public OneToOneEnumerator(OneToOneIdMap map)
            {
                _map = map;
            }

            private long _current = -1;

            public long Current
            {
                get
                {
                    return _map._ids[_current];
                }
            }

            public ulong TileId
            {
                get
                {
                    return _map._tileIds[_current];
                }
            }

            object IEnumerator.Current
            {
                get
                {
                    return _map._ids[_current];
                }
            }

            public void Dispose()
            {

            }

            public bool MoveNext()
            {
                _current += 1;
                return _current < _map._nextIndex;
            }

            public void Reset()
            {
                _current = -1;
            }
        }
    }
}