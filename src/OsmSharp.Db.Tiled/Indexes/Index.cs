using System;
using System.IO;
using OsmSharp.Db.Tiled.Collections.Sorting;
using Reminiscence.Arrays;
using Reminiscence.IO;
using Reminiscence.IO.Streams;

namespace OsmSharp.Db.Tiled.Indexes
{
    /// <summary>
    /// Represents an index matching ids to one or more subtiles.
    /// </summary>
    public class Index : IDisposable, IIndex
    {
        private readonly ArrayBase<ulong> _data;
        private readonly bool _mapped = false;
        private const int Subtiles = 16;
        
        /// <summary>
        /// Creates a new index.
        /// </summary>
        public Index()
        {
            _data = new MemoryArray<ulong>(1024);

            this.IsDirty = false;
        }
        
        private Index(ArrayBase<ulong> data)
        {
            _data = data;
            _pointer = _data.Length;
            _mapped = (data is Array<ulong>);

            this.IsDirty = false;
        }

        private long _pointer = 0;
        private bool _sorted = true;
        
        /// <summary>
        /// Returns true if the data in this index wasn't saved to disk.
        /// </summary>
        /// <returns></returns>
        public bool IsDirty
        {
            get;
            private set;
        }
        
        /// <summary>
        /// Adds a new entry in this index.
        /// </summary>
        public void Add(long id, int mask)
        {
            if (mask > MAX_MASK) {throw new ArgumentOutOfRangeException(nameof(mask));}

            if (_pointer > 0 && _sorted)
            { // verify if sorted, if not resort when saving.
                Decode(_data[_pointer - 1], out var previousId, out _);

                if (id < previousId)
                {
                    _sorted = false;
                }
            }

            _data.EnsureMinimumSize(_pointer + 1);
            Encode(id, mask, out var data);
            _data[_pointer] = data;
            _pointer++;

            this.IsDirty = true;
        }

        /// <summary>
        /// Sorts this index.
        /// </summary>
        private void Sort()
        {
            if (_sorted) return;
            
            QuickSort.Sort(i =>
                {
                    Decode(_data[i], out var id, out _);
                    return id;
                },
                (i1, i2) =>
                {
                    var t = _data[i1];
                    _data[i1] = _data[i2];
                    _data[i2] = t;
                }, 0, _pointer - 1);
            _sorted = true;
        }

        /// <summary>
        /// Trims the internal data structure to it's minimum size.
        /// </summary>
        public void Trim()
        {
            _data.Resize(_pointer);

            this.IsDirty = true;
        }

        /// <summary>
        /// Tries to get the mask for the given id.
        /// </summary>
        public bool TryGetMask(long id, out int mask)
        {
            if (!_mapped) return Search(id, out mask) != -1;
            lock (this)
            {
                return Search(id, out mask) != -1;
            }
        }

        /// <summary>
        /// Serializes this index to the given stream.
        /// </summary>
        /// <param name="stream"></param>
        /// <returns></returns>
        public long Serialize(Stream stream)
        {
            this.Trim();
            this.Sort();

            var size = _data.Length * 8 + 8;
            stream.Write(BitConverter.GetBytes(_data.Length), 0, 8);
            _data.CopyTo(stream);

            this.IsDirty = false;

            return size;
        }

        /// <summary>
        /// Deserializes an index from the given stream.
        /// </summary>
        public static Index Deserialize(Stream stream, ArrayProfile profile = null)
        {
            var bytes = new byte[8];
            stream.Read(bytes, 0, 8);
            var size = BitConverter.ToInt64(bytes, 0);

            ArrayBase<ulong> data;
            if (profile == null)
            { // just create arrays and read the data.
                data = new MemoryArray<ulong>(size);
                data.CopyFrom(stream);
            }
            else
            { // create accessors over the exact part of the stream that represents vertices/edges.
                var position = stream.Position;
                var map1 = new MemoryMapStream(new CappedStream(stream, position, size * 8));
                data = new Array<ulong>(map1.CreateUInt64(size), profile);
            }

            return new Index(data);
        }

        private const long MAX_MASK = 65536 - 1;
        private const ulong MAX_ID = ((ulong)1 << 47) - 1;
        private const long ID_MASK = ~(MAX_MASK << (64 - Subtiles));

        /// <summary>
        /// Decodes an id and a mask.
        /// </summary>
        public static void Decode(ulong data, out long id, out int mask)
        {
            mask = (int)(data >> (64 - Subtiles));

            ulong idmask = 0x00007fffffffffff;
            id = (long)(data & idmask);
            if ((((ulong)1 << 47) & data) != 0)
            {
                id = -id;
            }
        }

        /// <summary>
        /// Encodes an id and a mask.
        /// </summary>
        public static void Encode(long id, int mask, out ulong data)
        {
            if (mask > MAX_MASK) {throw new ArgumentOutOfRangeException(nameof(mask));}
            
            var unsignedId = unchecked((ulong)id);
            if (id < 0)
            {
                unsignedId = unchecked((ulong)-id);
            }

            if (unsignedId > MAX_ID) { throw new ArgumentOutOfRangeException(nameof(id)); }

            // left 16-bits are the mask.
            var masked = (ulong)mask << (64 - Subtiles);
            // right 48-bits are signed id.
            // copy 47 left bits (leave 48 for sign).
            ulong idmask = 0x0000ffffffffffff;
            var id48 = unsignedId & idmask;
            // set sign.
            if (id < 0)
            {
                id48 = id48 | ((ulong)1 << 47);
            }
            data = id48 | masked;
        }

        /// <summary>
        /// Gets the number of elements in this index.
        /// </summary>
        public long Count => _pointer;

        /// <summary>
        /// Gets id/mask pair at the given index.
        /// </summary>
        /// <param name="i">The index.</param>
        public (long id, int mask) this[int i]
        {
            get
            {
                Decode(_data[i], out var currentId, out var mask);
                return (currentId, mask);
            }
        }

        private long Search(long id, out int mask)
        {
            if (!_sorted)
            { // unsorted, just try all data.
                for (var i = 0; i < _pointer; i++)
                {
                    Decode(_data[i], out var currentId, out mask);
                    if (currentId == id)
                    {
                        return i;
                    }
                }

                mask = -1;
                return -1;
            }
            
            var min = 0L;
            var max = _pointer - 1;

            Decode(_data[min], out var minId, out mask);
            if (minId == id)
            {
                return min;
            }

            Decode(_data[max], out var maxId, out mask);
            if (maxId == id)
            {
                return max;
            }

            while (true)
            {
                var mid = (min + max) / 2;
                Decode(_data[mid], out var midId, out mask);
                if (midId == id)
                {
                    return mid;
                }

                if (midId > id)
                {
                    max = mid;
                }
                else
                {
                    min = mid;
                }

                if (max - min <= 1)
                {
                    return -1;
                }
            }
        }

        public void Dispose()
        {
            _data?.Dispose();
        }
    }
}