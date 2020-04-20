using System;
using System.Collections.Generic;
using System.IO;
using Reminiscence;
using Reminiscence.Arrays;

namespace OsmSharp.Db.Tiled.Indexes.TileMap
{
    internal class SparseArray
    {
        private readonly int _blockSize; 
        private readonly uint _default;
        private readonly MemoryArray<long> _pointers;
        private readonly MemoryArray<uint> _data;

        public SparseArray(long size = 0, int blockSize = 64,
            uint emptyDefault = default)
        {
            if (size < 0) { throw new ArgumentOutOfRangeException(nameof(size), "Size needs to be bigger than or equal to zero."); }
            if (blockSize <= 0) { throw new ArgumentOutOfRangeException(nameof(blockSize),"Block size needs to be bigger than or equal to zero."); }
            
            _default = emptyDefault;
            _blockSize = blockSize;
            _size = size;
            
            var blockCount = (long)System.Math.Ceiling((double)size / _blockSize);
            _pointers = new MemoryArray<long>(blockCount);
            _data = new MemoryArray<uint>(0);
        }

        public SparseArray(long size, int blockSize, uint emptyDefault, long nextBlock,
            MemoryArray<long> pointers, MemoryArray<uint> data)
        {
            _size = size;
            _blockSize = blockSize;
            _default = emptyDefault;
            _nextBlock = nextBlock;

            _pointers = pointers;
            _data = data;
        }
        
        private long _size;
        private long _nextBlock = 0;
        
        /// <summary>
        /// Gets or sets the item at the given index.
        /// </summary>
        /// <param name="idx">The index.</param>
        public uint this[long idx]
        {
            get
            {
                if (idx >= this.Length) throw new ArgumentOutOfRangeException(nameof(idx));
                
                var localIdx = idx % _blockSize;
                var blockId = (idx - localIdx) / _blockSize;
                var blockPointer = _pointers[blockId];
                if (blockPointer == 0) return _default;
                
                return _data[localIdx + blockPointer - 1];
            }
            set
            {
                if (idx >= this.Length) throw new ArgumentOutOfRangeException(nameof(idx));
                
                var localIdx = idx % _blockSize;
                var blockId = (idx - localIdx) / _blockSize;
                var blockPointer = _pointers[blockId];
                if (blockPointer == 0)
                {
                    // don't create a new block for a default value.
                    if (value == 0) return;

                    // create the new block.
                    blockPointer = _nextBlock;
                    _pointers[blockId] = blockPointer + 1;
                    _nextBlock += _blockSize;
                    
                    // make sure there is enough space.
                    var end = blockPointer + _blockSize;
                    ResizeData(end);
                    
                    // format block pointer with one off.
                    blockPointer++;
                }
                
                _data[localIdx + blockPointer - 1] = value;
            }
        }

        private void ResizeData(long end)
        {
            var newSize = _data.Length;
            while (newSize <= end)
            {
                if (newSize == 0) newSize = 1;
                newSize *= 2;
            }

            if (newSize == _data.Length) return;
            
            _data.Resize(newSize);
        }

        /// <summary>
        /// Gets the non-empty entries.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<(long i, uint value)> GetNonDefault()
        {
            for (var b = 0L; b < _pointers.Length; b++)
            {
                var pointer = _pointers[b];
                if (pointer == 0) continue;

                for (var i = 0L; i < _blockSize; i++)
                {
                    var value = _data[i + pointer - 1];
                    if (value == 0) continue;

                    yield return (b * _blockSize + i, value);
                }
            }
        }

        /// <summary>
        /// Resizes this array to the given size.
        /// </summary>
        /// <param name="size">The size.</param>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        public void Resize(long size)
        {
            if (size < 0) { throw new ArgumentOutOfRangeException(nameof(size), "Cannot resize an array to a size of zero or smaller."); }

            _size = size;

            var blockCount = (long)System.Math.Ceiling((double)size / _blockSize);
            if (blockCount != _pointers.Length)
            {
                _pointers.Resize(blockCount);
            }
        }
        
        /// <summary>
        /// Gets the length of this array.
        /// </summary>
        public long Length => _size;

        /// <summary>
        /// Serializes to the given stream.
        /// </summary>
        /// <param name="stream">The stream.</param>
        /// <returns>The number of bytes written.</returns>
        public long Serialize(Stream stream)
        {
            var position = stream.Position;
            stream.Write(BitConverter.GetBytes(_default), 0, 4);
            stream.Write(BitConverter.GetBytes(_blockSize), 0, 4);
            stream.Write(BitConverter.GetBytes(_size), 0, 8);
            stream.Write(BitConverter.GetBytes(_nextBlock), 0, 8);

            _pointers.CopyToWithSize(stream);
            _data.CopyToWithSize(stream);

            return stream.Position - position;
        }

        /// <summary>
        /// Deserializes from stream.
        /// </summary>
        /// <param name="stream">The stream.</param>
        /// <returns>The sparse array.</returns>
        public static SparseArray Deserialize(Stream stream)
        {
            var buffer = new byte[8];
            stream.Read(buffer, 0, 8);
            var emptyDefault = BitConverter.ToUInt32(buffer, 0);
            var blockSize = BitConverter.ToInt32(buffer, 4);
            stream.Read(buffer, 0, 8);
            var size = BitConverter.ToInt64(buffer, 0);
            stream.Read(buffer, 0, 8);
            var nextBlock = BitConverter.ToInt64(buffer, 0);

            var pointers = MemoryArray<long>.CopyFromWithSize(stream);
            var data = MemoryArray<uint>.CopyFromWithSize(stream);

            return new SparseArray(size, blockSize, emptyDefault, nextBlock,
                pointers, data);
        }
    }

    internal static class SparseArrayExtensions
    {
        internal static void EnsureMinimumSize(this SparseArray array, long i)
        {
            if (array.Length > i) return;
            
            var size = array.Length;
            if (size == 0) size = 1;

            while (size <= i)
            {
                size *= 2;
            }
            array.Resize(size);
        }
    }
}