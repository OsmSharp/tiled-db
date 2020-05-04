using System;
using System.Collections.Generic;
using System.IO;
using Reminiscence;
using Reminiscence.Arrays;

namespace OsmSharp.Db.Tiled.Indexes.TileMaps
{
    internal class TilesMap
    {
        private readonly TileMap _wayToFirstTile = new TileMap();
        private readonly MemoryArray<uint> _linkedTileList = new MemoryArray<uint>(0);
        private const uint TileMask = (uint) ((long)1 << 31);

        private uint _nextPointer = 0;

        public TilesMap()
        {
            
        }

        private TilesMap(TileMap wayToFirstTile, MemoryArray<uint> linkedTileList,
            uint nextPointer)
        {
            _wayToFirstTile = wayToFirstTile;
            _linkedTileList = linkedTileList;
            _nextPointer = nextPointer;
        }

        public void Add(long id, IEnumerable<uint> tiles)
        {
            using var enumerator = tiles.GetEnumerator();
            if (!enumerator.MoveNext()) return;

            _wayToFirstTile.EnsureMinimumSize(id);
            _wayToFirstTile[id] = enumerator.Current + TileMask;

            if (!enumerator.MoveNext()) return;
            
            // there is a second entry, add to linked list.
            var pointer = _nextPointer;
            _linkedTileList.EnsureMinimumSize(pointer * 2 + 2);
            _nextPointer++;
                
            // add previous data first.
            var previous = _wayToFirstTile[id] - TileMask;
            _linkedTileList[pointer * 2 + 0] = previous;
            _linkedTileList[pointer * 2 + 1] = uint.MaxValue; // no previous!
                
                
            // add current.
            var next = _nextPointer;
            _nextPointer++;
            _linkedTileList.EnsureMinimumSize(next * 2 + 2);
            _linkedTileList[next * 2 + 0] = enumerator.Current;
            _linkedTileList[next * 2 + 1] = pointer;

            // add tile 3 and so on.
            while (enumerator.MoveNext())
            {
                pointer = next;
                next = _nextPointer;
                _nextPointer++;
                _linkedTileList.EnsureMinimumSize(next * 2 + 2);
                _linkedTileList[next * 2 + 0] = enumerator.Current;
                _linkedTileList[next * 2 + 1] = pointer;
            }
                
            // update the first tile array to indicate data in the linked list.
            if (next >= TileMask) throw new Exception("This index cannot handle this much data.");
            _wayToFirstTile[id] = next;
        }

        public bool Has(long id)
        {
            return _wayToFirstTile[id] != 0;
        }
        
        public IEnumerable<uint> Get(long id)
        {
            var idOrPointer = _wayToFirstTile[id];
            if (idOrPointer == 0) yield break;
            if (idOrPointer > TileMask)
            {
                yield return (idOrPointer - TileMask);
                yield break;
            }

            while (idOrPointer < uint.MaxValue)
            {
                var tile = _linkedTileList[idOrPointer * 2 + 0];
                yield return (tile);
                idOrPointer = _linkedTileList[idOrPointer * 2 + 1];
            }
        }

        public long Serialize(Stream stream)
        {
            var position = stream.Position;

            _wayToFirstTile.Serialize(stream);
            
            stream.Write(BitConverter.GetBytes(_nextPointer), 0, 4);
            _linkedTileList.CopyToWithSize(stream);

            return stream.Position - position;
        }

        public static TilesMap Deserialize(Stream stream)
        {
            var wayToFirstTile = TileMap.Deserialize(stream);

            var buffer = new byte[4];
            stream.Read(buffer, 0, 4);
            var nextPointer = BitConverter.ToUInt32(buffer, 0);
            var linkedTileList = MemoryArray<uint>.CopyFromWithSize(stream);
            
            return new TilesMap(wayToFirstTile, linkedTileList, nextPointer);
        }
    }
}