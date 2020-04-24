using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using OsmSharp.Db.Tiled.Collections;
using OsmSharp.IO.Binary;

namespace OsmSharp.Db.Tiled.OsmTiled
{
    internal class OsmTiledLinkedStream : IDisposable
    {
        private readonly Stream _stream;
        private readonly SparseArray _pointers;

        public OsmTiledLinkedStream(Stream stream)
        {
            _stream = stream;
            
            _pointers = new SparseArray(0, emptyDefault: long.MaxValue);
        }

        private OsmTiledLinkedStream(SparseArray pointers, Stream stream)
        {
            _stream = stream;
            _pointers = pointers;
        }

        public OsmGeo Get(long pointer, byte[] buffer = null)
        {
            if (buffer?.Length < 8) buffer = null;
            buffer ??= new byte[8];
            
            _stream.Seek(pointer, SeekOrigin.Begin);

            // find tile.
            var cBytes = _stream.ReadDynamicUInt32(out var c);
            if (c == 1)
            {
                _stream.Seek(pointer + cBytes + 12, SeekOrigin.Begin);
            }
            else
            {
                var tilesBytes = c * 4;
                var pointerBytes = c * 8;
                
                // read next pointer.
                _stream.Seek(pointer + cBytes + tilesBytes + pointerBytes, SeekOrigin.Begin);
            }

            return _stream.ReadOsmGeo();
        }

        public IEnumerable<uint> GetTilesFor(long pointer, byte[] buffer = null)
        {
            if (buffer?.Length < 8) buffer = null;
            buffer ??= new byte[8];
            
            _stream.Seek(pointer, SeekOrigin.Begin);

            // find tile.
            _stream.ReadDynamicUInt32(out var c);
            if (c == 1)
            {
                _stream.Read(buffer, 0, 4);
                yield return BitConverter.ToUInt32(buffer, 0);
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
                    yield return currentTile;

                    t++;
                    if (c == t) break;
                }
            }
        }

        public long Append(uint tile, OsmGeo osmGeo)
        {
            _pointers.EnsureMinimumSize(tile + 1);

            var pointer = _pointers[tile];
            _pointers[tile] = _stream.Position;
            
            _stream.WriteDynamicUInt32(1);
            _stream.Write(BitConverter.GetBytes(tile), 0, 4);
            _stream.Write(BitConverter.GetBytes(pointer), 0, 8);
            _stream.Append(osmGeo);
            return _pointers[tile];
        }

        public long Append(IReadOnlyCollection<uint> tiles, OsmGeo osmGeo)
        {
            if (tiles.Count == 1)
            {
                // write pointer only.
                var tile = tiles.First();
                return Append(tile, osmGeo);
            }

            var position = _stream.Position;
            var c = (uint) tiles.Count;
            _stream.WriteDynamicUInt32(c);
            // write tile ids, followed by the pointers.
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

            _stream.Append(osmGeo);
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
            if (BitCoder.Encode(osmGeo1.Type, osmGeo1.Id.Value) > BitCoder.Encode(osmGeo2.Type, osmGeo2.Id.Value))
            {
                this.ReverseTile(tile, buffer);

                foreach (var osmGeo in this.GetForTileInternal(tile, buffer))
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
                _stream.Seek(osmGeoPointer, SeekOrigin.Begin);
                yield return _stream.ReadOsmGeo();
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
                _stream.Seek(pointer, SeekOrigin.Begin);

                // find tile.
                var cBytes = _stream.ReadDynamicUInt32(out var c);
                if (c == 1)
                {
                    _stream.Seek(4, SeekOrigin.Current); // skip tile.
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
                yield return (_stream.Position, originalPointer);
            }
        }

        public void ReverseAll()
        {
            var buffer = new byte[8];
            var pointers = new List<(long osmGeoPointer, long pointer)>();
            foreach (var tileId in this.GetTiles())
            {
                pointers.Clear();
                pointers.AddRange(this.GetForTilePointers(tileId, buffer));

                _pointers[tileId] = pointers[pointers.Count - 1].pointer;
                for (var i = pointers.Count - 1; i > 0; i--)
                {
                    UpdateNext(pointers[i].pointer, tileId, pointers[i - 1].pointer, buffer);
                }
                UpdateNext(pointers[0].pointer, tileId, long.MaxValue, buffer);
            }
        }

        private void ReverseTile(uint tileId, byte[] buffer)
        {
            if (buffer?.Length < 8) buffer = null;
            buffer ??= new byte[8];
            
            var pointers = new List<(long osmGeoPointer, long pointer)>();
            pointers.Clear();
            pointers.AddRange(this.GetForTilePointers(tileId, buffer));

            _pointers[tileId] = pointers[pointers.Count - 1].pointer;
            for (var i = pointers.Count - 1; i > 0; i--)
            {
                UpdateNext(pointers[i].pointer, tileId, pointers[i - 1].pointer, buffer);
            }
            UpdateNext(pointers[0].pointer, tileId, long.MaxValue, buffer);
        }

        private void UpdateNext(long pointer, uint tile, long next, byte[] buffer)
        {
            _stream.Seek(pointer, SeekOrigin.Begin);

            // find tile.
            var cBytes = _stream.ReadDynamicUInt32(out var c);
            if (c == 1)
            {
                _stream.Seek(4, SeekOrigin.Current);
                _stream.Write(BitConverter.GetBytes(next), 0, 8);
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
                _stream.Write(BitConverter.GetBytes(next), 0, 8);
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
            _stream?.Dispose();
        }
    }
}