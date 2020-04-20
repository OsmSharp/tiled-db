//using System;
//using System.IO;
//using OsmSharp.Db.Tiled.Collections.Sorting;
//using Reminiscence.Arrays;
//using Reminiscence.IO;
//using Reminiscence.IO.Streams;
//
//namespace OsmSharp.Db.Tiled.Indexes
//{
//    /// <summary>
//    /// An index to store deleted objects.
//    /// </summary>
//    internal class DeletedIndex : IDisposable
//    {
//        private readonly ArrayBase<long> _data;
//        private readonly bool _mapped = false;
//        
//        /// <summary>
//        /// Creates a new index.
//        /// </summary>
//        public DeletedIndex()
//        {
//            _data = new MemoryArray<long>(0);
//
//            this.IsDirty = false;
//        }
//        
//        private DeletedIndex(ArrayBase<long> data)
//        {
//            _data = data;
//            _pointer = _data.Length;
//            _mapped = (data is Array<long>);
//
//            this.IsDirty = false;
//        }
//
//        private long _pointer = 0;
//        private bool _sorted = false;
//
//        /// <summary>
//        /// Returns true if the data in this index wasn't saved to disk.
//        /// </summary>
//        /// <returns></returns>
//        public bool IsDirty
//        {
//            get;
//            private set;
//        }
//        
//        /// <summary>
//        /// Adds a new entry in this index.
//        /// </summary>
//        public void Add(long id)
//        {
//            _data.EnsureMinimumSize(_pointer + 1);
//            _data[_pointer] = id;
//            _pointer++;
//
//            this.IsDirty = true;
//        }
//
//        /// <summary>
//        /// Trims the internal data structure to it's minimum size.
//        /// </summary>
//        public void Trim()
//        {
//            _data.Resize(_pointer);
//
//            this.IsDirty = true;
//        }
//
//        /// <summary>
//        /// Sorts this index.
//        /// </summary>
//        private void Sort()
//        {
//            if (_sorted) return;
//            
//            QuickSort.Sort(i => _data[i],
//                (i1, i2) =>
//                {
//                    var t = _data[i1];
//                    _data[i1] = _data[i2];
//                    _data[i2] = t;
//                }, 0, _pointer - 1);
//            _sorted = true;
//        }
//
//        /// <summary>
//        /// Returns true if the given id in the index.
//        /// </summary>
//        public bool Contains(long id)
//        {
//            if (!_mapped) return Search(id);
//            lock (this)
//            {
//                return Search(id);
//            }
//        }
//
//        /// <summary>
//        /// Serializes this index to the given stream.
//        /// </summary>
//        /// <param name="stream"></param>
//        /// <returns></returns>
//        public long Serialize(Stream stream)
//        {
//            this.Trim();
//            this.Sort();
//
//            var size = _data.Length * 8 + 8;
//            stream.Write(BitConverter.GetBytes(_data.Length), 0, 8);
//            _data.CopyTo(stream);
//
//            this.IsDirty = false;
//
//            return size;
//        }
//
//        /// <summary>
//        /// Deserializes an index from the given stream.
//        /// </summary>
//        public static DeletedIndex Deserialize(Stream stream, ArrayProfile profile = null)
//        {
//            var bytes = new byte[8];
//            stream.Read(bytes, 0, 8);
//            var size = BitConverter.ToInt64(bytes, 0);
//
//            ArrayBase<long> data;
//            if (profile == null)
//            { // just create arrays and read the data.
//                data = new MemoryArray<long>(size);
//                data.CopyFrom(stream);
//            }
//            else
//            { // create accessors over the exact part of the stream that represents vertices/edges.
//                var position = stream.Position;
//                var map1 = new MemoryMapStream(new CappedStream(stream, position, size * 8));
//                data = new Array<long>(map1.CreateInt64(size), profile);
//            }
//
//            return new DeletedIndex(data);
//        }
//
//        private bool Search(long id)
//        {
//            if (!_sorted)
//            { // unsorted, just try all data.
//                for (var i = 0; i < _pointer; i++)
//                {
//                    if (_data[i] == id) return true;
//                }
//                return false;
//            }
//            
//            // do a binary search.
//            var min = 0L;
//            var max = _pointer - 1;
//
//            var minId = _data[min];
//            if (minId == id)
//            {
//                return true;
//            }
//
//            var maxId = _data[max];
//            if (maxId == id)
//            {
//                return true;
//            }
//
//            while (true)
//            {
//                var mid = (min + max) / 2;
//                var midId = _data[mid];
//                if (midId == id)
//                {
//                    return true;
//                }
//
//                if (midId > id)
//                {
//                    max = mid;
//                }
//                else
//                {
//                    min = mid;
//                }
//
//                if (max - min <= 1)
//                {
//                    return false;
//                }
//            }
//        }
//
//        public void Dispose()
//        {
//            _data?.Dispose();
//        }
//    }
//}