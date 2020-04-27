using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using OsmSharp.Db.Tiled.Collections;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.OsmTiled.IO;
using OsmSharp.IO.Binary;

namespace OsmSharp.Db.Tiled.OsmTiled
{
    internal class OsmTiledLinkedStream : IDisposable
    {
        private readonly Stream _data;
        private readonly SparseArray _pointers;
        private readonly SparseArray _previousPointers;

        public OsmTiledLinkedStream(Stream data)
        {
            _data = data;
            
            _pointers = new SparseArray(0, emptyDefault: long.MaxValue);
            _previousPointers = new SparseArray(0, emptyDefault: long.MaxValue);
        }

        private OsmTiledLinkedStream(SparseArray pointers, Stream data)
        {
            _data = data;
            _pointers = pointers;
        }

        public OsmGeo Get(long pointer, byte[] buffer = null)
        {
            if (buffer?.Length < 8) buffer = null;
            buffer ??= new byte[8];
            
            _data.Seek(pointer, SeekOrigin.Begin);

            // find tile.
            var cBytes = _data.Position;
            var c = _data.ReadVarUInt32();
            cBytes -= _data.Position;
            if (c == 1)
            {
                _data.Seek(pointer + cBytes + 12, SeekOrigin.Begin);
            }
            else
            {
                var tilesBytes = c * 4;
                var pointerBytes = c * 8;
                
                // read next pointer.
                _data.Seek(pointer + cBytes + tilesBytes + pointerBytes, SeekOrigin.Begin);
            }

            return _data.ReadOsmGeo();
        }

        public IEnumerable<uint> GetTilesFor(long pointer, byte[] buffer)
        {
            if (buffer?.Length < 8) buffer = null;
            buffer ??= new byte[8];
            
            _data.Seek(pointer, SeekOrigin.Begin);

            // find tile.
            var c = _data.ReadVarUInt32();
            if (c == 1)
            {
                _data.Read(buffer, 0, 4);
                yield return BitConverter.ToUInt32(buffer, 0);
            }
            else
            {
                var t = 0;
                while (true)
                {
                    _data.Read(buffer, 0, 4);
                    var currentTile = BitConverter.ToUInt32(buffer, 0);
                    yield return currentTile;

                    t++;
                    if (c == t) break;
                }
            }
        }

        public long Append(uint tile, OsmGeo osmGeo, byte[] buffer = null)
        {
            _pointers.EnsureMinimumSize(tile + 1);
            _previousPointers.EnsureMinimumSize(tile + 1);

            var previousPointer = _previousPointers[tile];
            if (previousPointer == long.MaxValue)
            {
                _pointers[tile] = _data.Position;
            }
            
            _data.WriteVarUInt32(1);
            _data.WriteVarUInt32(tile);
            _previousPointers[tile] = _data.Position;
            if (previousPointer != long.MaxValue)
            {
                var before = _data.Position;
                _data.Seek(previousPointer, SeekOrigin.Begin);
                _data.WriteInt64(previousPointer);
                _data.Seek(before, SeekOrigin.Begin);
            }
            _data.WriteInt64(long.MaxValue);
            _data.Append(osmGeo, buffer);
            return _pointers[tile];
        }

        public long Append(IReadOnlyCollection<uint> tiles, OsmGeo osmGeo, byte[] buffer = null)
        {
            if (tiles.Count == 1)
            {
                // write pointer only.
                var tile = tiles.First();
                return Append(tile, osmGeo, buffer);
            }

            var position = _data.Position;
            var c = (uint) tiles.Count;
            _data.WriteVarUInt32(c);
            // write tile ids, followed by the pointers.
            foreach (var tile in tiles)
            {
                _data.WriteVarUInt32(tile);
            }

            foreach (var tile in tiles)
            {
                _pointers.EnsureMinimumSize(tile + 1);

                var pointer = _pointers[tile];
                _pointers[tile] = position;

                _data.WriteInt64(pointer);
            }

            _data.Append(osmGeo, buffer);
            return position;
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
            using var enumerator = this.GetForTileInternal(tile, buffer).GetEnumerator();
            if (!enumerator.MoveNext()) yield break;
            var osmGeo1 = enumerator.Current;
            if (!enumerator.MoveNext()) 
            {
                yield return osmGeo1; // just one object, no need to reverse.
                yield break;
            }
            var osmGeo2 = enumerator.Current;

            if (osmGeo1?.Id == null) throw new InvalidDataException($"Object that's null or without an id.");
            if (osmGeo2?.Id == null) throw new InvalidDataException($"Object that's null or without an id.");
            if (OsmGeoCoder.Encode(osmGeo1.Type, osmGeo1.Id.Value) > OsmGeoCoder.Encode(osmGeo2.Type, osmGeo2.Id.Value))
            {
                var osmGeos = new List<OsmGeo>(this.GetForTileInternal(tile, buffer));
                osmGeos.Reverse();

                foreach (var osmGeo in osmGeos)
                {
                    yield return osmGeo;
                }

                yield break;
            }

            yield return osmGeo1;
            yield return osmGeo2;

            while (enumerator.MoveNext())
            {
                yield return enumerator.Current;
            }
        }

        private IEnumerable<OsmGeo> GetForTileInternal(uint tile, byte[] buffer = null)
        {
            foreach (var (osmGeoPointer, _) in this.GetForTilePointers(tile, buffer))
            {
                _data.Seek(osmGeoPointer, SeekOrigin.Begin);
                yield return _data.ReadOsmGeo();
            }
        }

        private IEnumerable<(long pointer, long osmGeoPointer)> GetForTilePointers(uint tile, byte[] buffer = null)
        {
            if (buffer?.Length < 8) buffer = null;
            buffer ??= new byte[8];
            
            var pointer = _pointers[tile];
            while (pointer != long.MaxValue)
            {
                var originalPointer = pointer;
                _data.Seek(pointer, SeekOrigin.Begin);

                // read tile count.
                var c = _data.ReadVarUInt32();

                // find tile.
                var tIndex = -1;
                for (var t = 0; t < c; t++)
                {
                    var currentTile = _data.ReadVarUInt32();
                    if (currentTile == tile)
                    {
                        tIndex = t;
                    }
                }
                if (tIndex < 0) throw new InvalidDataException("Tile not found, should always be there!");

                // read next pointer.
                var pointerPosition = _data.Position;
                _data.Seek(pointerPosition + (tIndex * 8), SeekOrigin.Begin);
                var nextPointer = _data.ReadInt64();
                
                // move to data.
                _data.Seek(pointerPosition + (c * 8), SeekOrigin.Begin);

                pointer = nextPointer;
                
                yield return (_data.Position, originalPointer);
            }
        }

        public long SerializeIndex(Stream stream)
        {
            var pos = stream.Position;
            
            stream.WriteByte(1);
            _pointers.Serialize(stream);

            return stream.Position - pos;
        }

        public static OsmTiledLinkedStream Deserialize(Stream indexStream, Stream stream)
        {
            var version = indexStream.ReadByte();
            if (version != 1) throw new InvalidDataException("Invalid version, cannot read index.");

            var pointers = SparseArray.Deserialize(indexStream);
            
            return new OsmTiledLinkedStream(pointers, stream);
        }

        public void Dispose()
        {
            _data?.Dispose();
        }
    }
}