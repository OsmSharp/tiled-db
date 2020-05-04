using System.Collections.Generic;
using System.Drawing;
using System.IO;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.OsmTiled.IO;

namespace OsmSharp.Db.Tiled.OsmTiled.Data
{

    internal class OsmTiledDbTileIndexReadOnly : IOsmTiledDbTileIndexReadOnly
    {
        private readonly Stream _data;
        private readonly long _startPosition;
        private readonly long _endPosition;

        public OsmTiledDbTileIndexReadOnly(Stream stream)
        {
            var nonDefaultCount = stream.ReadInt64();
            var size = stream.ReadVarInt64();
            var blockSize = stream.ReadVarInt32();
            Default = stream.ReadVarInt64();

            _startPosition = stream.Position;
            _endPosition = stream.Position + (12 * nonDefaultCount);
            _data = stream;
        }

        public long Default { get; }

        public long Get(uint tile)
        {
            var pointer = Find(tile);
            if (pointer == null) return this.Default;

            _data.Seek(pointer.Value + 4, SeekOrigin.Begin);
            return _data.ReadInt64();
        }

        public IEnumerable<uint> GetTiles()
        {
            _data.Seek(_startPosition, SeekOrigin.Begin);
            while (_data.Position < _endPosition)
            {
                yield return _data.ReadUInt32();
                _data.Seek(8, SeekOrigin.Begin);
            }
        }

        private long? Find(uint encoded)
        {
            _data.Seek(_startPosition, SeekOrigin.Begin);
            const int Size = 8 + 4;
            long start = 0;
            long end = (_endPosition - _startPosition) / Size;

            long middle = (end + start) / 2;
            _data.Seek(_startPosition + middle * Size, SeekOrigin.Begin);
            var middleId = _data.ReadUInt32();
            while (middleId != encoded)
            {
                if (middleId > encoded)
                {
                    if (end == middle) return null;
                    end = middle;
                }
                else
                {
                    if (start == middle) return null;
                    start = middle;
                }
                
                middle = (end + start) / 2;
                _data.Seek(_startPosition + middle * Size, SeekOrigin.Begin);
                middleId = _data.ReadUInt32();
            }

            return _startPosition + middle * Size;
        }
    }
}