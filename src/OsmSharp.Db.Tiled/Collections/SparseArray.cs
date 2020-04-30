using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using OsmSharp.Db.Tiled.IO;

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
            if (size < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(size), "Size needs to be bigger than or equal to zero.");
            }

            if (blockSize <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(blockSize),
                    "Block size needs to be bigger than or equal to zero.");
            }

            if ((blockSize & (blockSize - 1)) != 0)
            {
                throw new ArgumentOutOfRangeException(nameof(blockSize), "Block size needs to be a power of 2.");
            }

            _default = emptyDefault;
            _blockSize = blockSize;
            _size = size;
            _arrayPow = ExpOf2(blockSize);

            var blockCount = (long) System.Math.Ceiling((double) size / _blockSize);
            _blocks = new long[blockCount][];
        }

        private SparseArray(long[][] blocks, long size, int blockSize, long emptyDefault)
        {           
            _default = emptyDefault;
            _blockSize = blockSize;
            _size = size;
            _arrayPow = ExpOf2(blockSize);
            _blocks = blocks;
        }

        private static int ExpOf2(int powerOf2)
        {
            // this can probably be faster but it needs to run once in the constructor,
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
            if (size < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(size),
                    "Cannot resize an array to a size of zero or smaller.");
            }

            _size = size;

            var blockCount = (long) System.Math.Ceiling((double) size / _blockSize);
            if (blockCount != _blocks.Length)
            {
                Array.Resize(ref _blocks, (int) blockCount);
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
        /// <param name="version">The version of the layout to write.</param>
        /// <returns>The number of bytes written.</returns>
        public long Serialize(Stream stream, byte version = 2)
        {
            var pos = stream.Position;

            if (version == 1)
            {
                stream.WriteByte(1);
                stream.WriteInt64(_size);
                stream.WriteInt32(_blockSize);
                stream.WriteInt64(_default);

                for (long b = 0; b < _blocks.Length; b++)
                {
                    var block = _blocks[b];
                    if (block == null) continue;

                    stream.WriteInt64(b);
                    for (var i = 0; i < block.Length; i++)
                    {
                        stream.WriteInt64(block[i]);
                    }
                }

                stream.WriteInt64(long.MaxValue);
            }
            else if (version == 2)
            {
                stream.WriteByte(2);
                stream.WriteVarInt64(_size);
                stream.WriteVarInt32(_blockSize);
                stream.WriteVarInt64(_default);

                for (long b = 0; b < _blocks.Length; b++)
                {
                    var block = _blocks[b];
                    if (block == null) continue;

                    var min = int.MaxValue;
                    var max = int.MinValue;
                    for (var i = 0; i < block.Length; i++)
                    {
                        if (block[i] == _default) continue;
                        
                        if (min > i) min = i;
                        if (max < i) max = i;
                    }
                    
                    if (max == int.MinValue) continue;

                    stream.WriteVarInt64(b);
                    stream.WriteVarInt32(min);
                    stream.WriteVarInt32(max - min);
                    for (var i = min; i <= max; i++)
                    {
                        stream.WriteInt64(block[i]);
                    }
                }

                stream.WriteVarInt64(long.MaxValue);
            }
            else
            {
                throw new InvalidDataException("Invalid data layout version, cannot write index.");
            }

            return stream.Position - pos;
        }

        public static SparseArray Deserialize(Stream stream)
        {
            var version = stream.ReadByte();

            if (version == 1)
            {
                var size = stream.ReadInt64();
                var blockSize = stream.ReadInt32();
                var emptyDefault = stream.ReadInt64();

                var b = stream.ReadInt64();
                var blockCount = (long) Math.Ceiling((double) size / blockSize);
                var blocks = new long[blockCount][];
                while (b != long.MaxValue)
                {
                    var block = new long[blockSize];
                    for (var i = 0; i < block.Length; i++)
                    {
                        block[i] = stream.ReadInt64();
                    }

                    blocks[b] = block;

                    b = stream.ReadInt64();
                }

                return new SparseArray(blocks, size, blockSize, emptyDefault);
            }
            else
            {                
                var size = stream.ReadVarInt64();
                var blockSize = stream.ReadVarInt32();
                var emptyDefault = stream.ReadVarInt64();
                
                var blockCount = (long) Math.Ceiling((double) size / blockSize);
                var blocks = new long[blockCount][];

                var b = stream.ReadVarInt64();
                while (b != long.MaxValue)
                {
                    var block = new long[blockSize];
                    var min = stream.ReadVarInt32();
                    var max = stream.ReadVarInt32() + min;

                    for (var i = 0; i < min; i++)
                    {
                        block[i] = emptyDefault;
                    }
                    for (var i = min; i <= max; i++)
                    {
                        block[i] = stream.ReadInt64();
                    }
                    for (var i = max + 1; i < block.Length; i++)
                    {
                        block[i] = emptyDefault;
                    }

                    blocks[b] = block;

                    b = stream.ReadVarInt64();
                }

                return new SparseArray(blocks, size, blockSize, emptyDefault);
            }

            throw new InvalidDataException("Invalid version, cannot read index.");
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