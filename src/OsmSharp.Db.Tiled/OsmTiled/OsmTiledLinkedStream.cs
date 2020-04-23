using System;
using System.Collections.Generic;
using System.IO;
using OsmSharp.Db.Tiled.Collections;
using OsmSharp.IO.Binary;

namespace OsmSharp.Db.Tiled.OsmTiled
{
    internal class OsmTiledLinkedStream
    {
        private readonly Stream _stream;
        private readonly SparseArray<long> _pointers;

        public OsmTiledLinkedStream(Stream stream)
        {
            _stream = stream;
            
            _pointers = new SparseArray<long>(0, emptyDefault: long.MaxValue);
        }

        public long Append(uint tile, OsmGeo osmGeo)
        {
            _pointers.EnsureMinimumSize(tile + 1);

            var pointer = _pointers[tile];
            _pointers[tile] = _stream.Position;
            
            _stream.WriteDynamicUInt32(1);
            _stream.Write(BitConverter.GetBytes(pointer), 0, 8);
            var pos = _stream.Position;
            _stream.Append(osmGeo);
            return pos;
        }

        public long Append(IReadOnlyList<uint> tiles, OsmGeo osmGeo)
        {
            var position = _stream.Position;
            var c = (uint)tiles.Count;
            _stream.WriteDynamicUInt32(c);

            if (c == 1)
            {
                // write pointer only.
                var tile = tiles[0];
                _pointers.EnsureMinimumSize(tile + 1);

                var pointer = _pointers[tile];
                _pointers[tile] = position;

                _stream.Write(BitConverter.GetBytes(pointer), 0, 8);
            }
            else
            { // write tile ids, followed by the pointers.
                foreach (var tile in tiles)
                {
                    _stream.Write(BitConverter.GetBytes(tile), 0, 4);
                }
            
                foreach (var tile in tiles)
                {
                    _pointers.EnsureMinimumSize(tile + 1);

                    var pointer = _pointers[tile];
                    _pointers[tile] = position;

                    _stream.Write(BitConverter.GetBytes(pointer), 0, 8);
                }
            }

            var pos = _stream.Position;
            _stream.Append(osmGeo);
            return pos;
        }

        public IEnumerable<uint> GetTiles()
        {
            for (uint t = 0; t < _pointers.Length; t++)
            {
                var pointer = _pointers[t];
                if (pointer == long.MaxValue) continue;

                yield return t;
            }
        }

        public IEnumerable<OsmGeo> GetForTile(uint tile, byte[] buffer = null)
        {
            if (buffer?.Length < 12) buffer = null;
            buffer ??= new byte[8];
            
            var pointer = _pointers[tile];
            while (pointer != long.MaxValue)
            {
                _stream.Seek(pointer, SeekOrigin.Begin);

                // find tile.
                var cBytes = _stream.ReadDynamicUInt32(out var c);
                if (c == 1)
                {
                    _stream.Read(buffer, 0, 8);
                    pointer = BitConverter.ToInt64(buffer, 0);
                }
                else
                {
                    var tilesBytes = c * 4;
                    var pointerBytes = c * 8;
                    var t = 0;
                    while (true)
                    {
                        _stream.Read(buffer, 0, 4);
                        var currentTile = BitConverter.ToUInt32(buffer, 0);
                        if (currentTile == tile)
                        {
                            break;
                        }

                        t++;
                        if (c == t) throw new InvalidDataException("Cannot find tile, it is expected to always be there.");
                    }

                    // read next pointer.
                    _stream.Seek(pointer + cBytes + tilesBytes + (t * 8), SeekOrigin.Begin);
                    _stream.Read(buffer, 0, 8);
                    var nextPointer = BitConverter.ToInt64(buffer, 0);
                
                    // read data.
                    _stream.Seek(pointer + cBytes + tilesBytes + pointerBytes, SeekOrigin.Begin);

                    pointer = nextPointer;
                }
                yield return _stream.ReadOsmGeo();
            }
        }
    }
}