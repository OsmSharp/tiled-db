using Reminiscence.Arrays;
using System;
using System.Collections.Generic;
using System.Collections;
using System.IO;

namespace Anyways.Osm.TiledDb.Indexing
{
    /// <summary>
    /// A one-to-many id map, maps id's to one or more tile id's.
    /// </summary>
    public class OneToManyIdMap : IEnumerable<long>
    {
        private readonly ArrayBase<long> _ids;
        private readonly ArrayBase<ulong> _tileIds;
        private readonly int BlockSize = 1000;

        /// <summary>
        /// Creates a new one-to-many id map.
        /// </summary>
        public OneToManyIdMap()
        {
            _ids = new MemoryArray<long>(BlockSize);
            _tileIds = new MemoryArray<ulong>(BlockSize);
        }

        /// <summary>
        /// Creates a new one-to-many id map.
        /// </summary>
        private OneToManyIdMap(long nextIndex, long nextPointer, ArrayBase<long> ids, ArrayBase<ulong> tileIds)
        {
            _nextIndex = nextIndex;
            _nextPointer = nextPointer;
            _ids = ids;
            _tileIds = tileIds;
        }

        private long _nextIndex = 0;
        private long _nextPointer = 0;

        /// <summary>
        /// Adds a new id to this map.
        /// </summary>
        public void Add(long id, params ulong[] tileIds)
        {
            if (_nextIndex > 1 &&
                _ids[_nextIndex - 2] > id)
            {
                throw new InvalidOperationException("Id's can only be added from lower->higher id's.");
            }
            if (_nextIndex + 2 >= _ids.Length)
            {
                _ids.Resize(_ids.Length + BlockSize);
            }
            while (_nextPointer + 1 + tileIds.Length > _tileIds.Length)
            {
                _tileIds.Resize(_tileIds.Length + BlockSize);
            }

            _ids[_nextIndex + 0] = id;
            _ids[_nextIndex + 1] = _nextPointer;
            _tileIds[_nextPointer + 0] = (ulong)tileIds.Length;
            for (var i = 0; i < tileIds.Length; i++)
            {
                _tileIds[_nextPointer + 1 + i] = tileIds[i];
            }
            _nextIndex += 2;
            _nextPointer += 1 + tileIds.Length;
        }
        
        /// <summary>
        /// Gets the tile ids for the given id.
        /// </summary>
        public int TryGet(long id, ref ulong[] data)
        {
            var pointer = this.TryGetPointer(id);
            if (pointer == long.MaxValue)
            {
                return 0;
            }
            var size = (int)_tileIds[pointer];
            for (var i = 0; i < size; i++)
            {
                data[i] = _tileIds[pointer + 1 + i];
            }
            return size;
        }

        private ulong[] GetTileIds(long pointer)
        {
            var size = (int)_tileIds[pointer];
            var tileIds = new ulong[size];
            for (var i = 0; i < size; i++)
            {
                tileIds[i] = _tileIds[pointer + 1 + i];
            }
            return tileIds;
        }

        private long TryGetPointer(long id)
        {
            var index = this.TryGetIndex(id);
            if (index == long.MaxValue)
            {
                return long.MaxValue;
            }
            return _ids[index + 1];
        }

        private long TryGetIndex(long id)
        {
            if (_nextIndex == 0)
            {
                return long.MaxValue;
            }

            long bottom = 0;
            long top = (_nextIndex / 2) - 1;
            long bottomId = _ids[bottom * 2];
            if (id == bottomId)
            {
                return bottom * 2;
            }
            long topId = _ids[top * 2];
            if (id == topId)
            {
                while (top - 1 > 0 &&
                    _ids[(top - 1) * 2] == id)
                {
                    top--;
                }
                return top * 2;
            }

            while (top - bottom > 1)
            {
                var middle = (((top - bottom) / 2) + bottom);
                var middleId = _ids[middle * 2];
                if (middleId == id)
                {
                    while (middle - 1 > 0 &&
                        _ids[(middle - 1) * 2] == id)
                    {
                        middle--;
                    }
                    return middle * 2;
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
        /// Merges the two maps.
        /// </summary>
        public static OneToManyIdMap Merge(OneToManyIdMap map1, OneToManyIdMap map2)
        {
            var enumerator1 = new OneToManyEnumerator(map1);
            var enumerator2 = new OneToManyEnumerator(map2);

            var map = new OneToManyIdMap();

            var next1 = enumerator1.MoveNext();
            var next2 = enumerator2.MoveNext();

            var empty = new ulong[0];

            while (next1 || next2)
            {
                if (next1 && next2)
                {
                    var lowest = enumerator1.Current;
                    if (enumerator2.Current < lowest)
                    {
                        lowest = enumerator2.Current;
                    }

                    var tileIds1 = empty;
                    if (enumerator1.Current == lowest)
                    {
                        tileIds1 = map1.GetTileIds(enumerator1.Pointer);
                    }
                    var tileIds2 = empty;
                    if (enumerator2.Current == lowest)
                    {
                        tileIds2 = map1.GetTileIds(enumerator1.Pointer);
                    }

                    var tileIds = new ulong[tileIds1.Length + tileIds2.Length];
                    tileIds1.CopyTo(tileIds, 0);
                    tileIds2.CopyTo(tileIds, tileIds1.Length);

                    map.Add(lowest, tileIds);

                    if (enumerator1.Current == enumerator2.Current)
                    {
                        next1 = enumerator1.MoveNext();
                        next2 = enumerator2.MoveNext();
                    }
                    else if(enumerator1.Current < enumerator2.Current)
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
                    var tileIds = map1.GetTileIds(enumerator1.Pointer);
                    map.Add(enumerator1.Current, tileIds);

                    next1 = enumerator1.MoveNext();
                }
                else if (next2)
                {
                    var tileIds = map2.GetTileIds(enumerator2.Pointer);
                    map.Add(enumerator2.Current, tileIds);

                    next2 = enumerator2.MoveNext();
                }
            }

            return map;
        }

        /// <summary>
        /// Serializes this map to disk.
        /// </summary>
        public long Serialize(Stream stream)
        {
            long size = 0;
            var sizeBytes = BitConverter.GetBytes(_nextIndex);
            stream.Write(sizeBytes, 0, 8);
            size += 8;
            sizeBytes = BitConverter.GetBytes(_nextPointer);
            stream.Write(sizeBytes, 0, 8);
            size += 8;

            _ids.Resize(_nextIndex);
            _tileIds.Resize(_nextPointer);
            size += _ids.CopyTo(stream);
            size += _tileIds.CopyTo(stream);

            return size;
        }

        /// <summary>
        /// Deserializes a map from the given stream.
        /// </summary>
        public static OneToManyIdMap Deserialize(Stream stream, bool mapped = false)
        { 
            var sizeBytes = new byte[16];
            stream.Read(sizeBytes, 0, 16);
            var nextIndex = BitConverter.ToInt64(sizeBytes, 0);
            var nextPointer = BitConverter.ToInt64(sizeBytes, 8);

            if (mapped)
            {
                throw new NotSupportedException("Load an index in a mapped way is not yet supported.");
            }

            var ids = new MemoryArray<long>(nextIndex);
            ids.CopyFrom(stream);
            var tileIds = new MemoryArray<ulong>(nextPointer);
            tileIds.CopyFrom(stream);

            return new OneToManyIdMap(nextIndex, nextPointer, ids, tileIds);
        }

        /// <summary>
        /// Gets the enumerator.
        /// </summary>
        public IEnumerator<long> GetEnumerator()
        {
            return new OneToManyEnumerator(this);
        }

        /// <summary>
        /// Gets the enumerator.
        /// </summary>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return new OneToManyEnumerator(this);
        }

        private class OneToManyEnumerator : IEnumerator<long>
        {
            private readonly OneToManyIdMap _map;

            public OneToManyEnumerator(OneToManyIdMap map)
            {
                _map = map;
            }

            private long _current = -2;

            public long Current
            {
                get
                {
                    return _map._ids[_current];
                }
            }

            public long Pointer
            {
                get
                {
                    return _map._ids[_current + 1];
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
                _current += 2;
                return _current < _map._nextIndex;
            }

            public void Reset()
            {
                _current = -2;
            }
        }
    }
}