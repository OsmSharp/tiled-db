using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace OsmSharp.Db.Tiled.Collections
{
  internal class SparseArray
    {
        private long[][] _blocks;
        private readonly int _blockSize; // Holds the maximum array size, always needs to be a power of 2.
        private readonly int _arrayPow;
        private long _size; // the total size of this array.
        private readonly long _default = default;

        public SparseArray(long size, int blockSize = 1 << 16,
            long emptyDefault = default)
        {
            if (size < 0) { throw new ArgumentOutOfRangeException(nameof(size), "Size needs to be bigger than or equal to zero."); }
            if (blockSize <= 0) { throw new ArgumentOutOfRangeException(nameof(blockSize),"Block size needs to be bigger than or equal to zero."); }
            if ((blockSize & (blockSize - 1)) != 0) { throw new ArgumentOutOfRangeException(nameof(blockSize),"Block size needs to be a power of 2."); }
            
            _default = emptyDefault;
            _blockSize = blockSize;
            _size = size;
            _arrayPow = ExpOf2(blockSize);

            var blockCount = (long)System.Math.Ceiling((double)size / _blockSize);
            _blocks = new long[blockCount][];
        }

        private static int ExpOf2(int powerOf2)
        { // this can probably be faster but it needs to run once in the constructor,
            // feel free to improve but not crucial.
            if (powerOf2 == 1)
            {
                return 0;
            }
            return ExpOf2(powerOf2 / 2) + 1;
        }
        
        /// <summary>
        /// Gets or sets the item at the given index.
        /// </summary>
        /// <param name="idx">The index.</param>
        public long this[long idx]
        {
            get
            {
                if (idx >= this.Length) throw new ArgumentOutOfRangeException(nameof(idx));
                
                var blockId = idx >> _arrayPow;
                var block = _blocks[blockId];
                if (block == null) return _default;
                
                var localIdx = idx - (blockId << _arrayPow);
                return block[localIdx];
            }
            set
            {
                if (idx >= this.Length) throw new ArgumentOutOfRangeException(nameof(idx));
                
                var blockId = idx >> _arrayPow;
                var block = _blocks[blockId];
                if (block == null)
                {
                    // don't create a new block for a default value.
                    if (EqualityComparer<long>.Default.Equals(value, _default)) return;
                    
                    block = new long[_blockSize];
                    for (var i = 0; i < _blockSize; i++)
                    {
                        block[i] = _default;
                    }
                    _blocks[blockId] = block;
                }
                var localIdx = idx % _blockSize;
                _blocks[blockId][localIdx] = value;
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
            if (blockCount != _blocks.Length)
            {
                Array.Resize(ref _blocks, (int)blockCount);
            }
        }
        
        /// <summary>
        /// Gets the length of this array.
        /// </summary>
        public long Length => _size;

        /// <summary>
        /// Serializes this array.
        /// </summary>
        /// <param name="stream">The stream.</param>
        /// <returns>The number of bytes written.</returns>
        public long Serialize(Stream stream)
        {
            var pos = stream.Position;
            
            stream.WriteByte(1);
            using var streamWriter = new BinaryWriter(stream, Encoding.Default, true);
            streamWriter.Write(_size);
            streamWriter.Write(_blockSize);
            streamWriter.Write(_default);

            for (long b = 0; b < _blocks.Length; b++)
            {
                var block = _blocks[b];
                if (block == null) continue;
                
                streamWriter.Write(b);
                for (var i = 0; i < block.Length; i++)
                {
                    streamWriter.Write(block[i]);   
                }
            }

            return stream.Position - pos;
        }
    }

    internal static class SparseArrayExtensions
    {
        internal static void EnsureMinimumSize(this SparseArray array, long i)
        {
            if (array.Length <= i)
            {
                array.Resize(i + 1);
            }
        }
    }
}