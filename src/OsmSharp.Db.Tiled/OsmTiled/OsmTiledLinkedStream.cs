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
        private readonly SparseArray _lastPointers;

        public OsmTiledLinkedStream(Stream stream)
        {
            _stream = stream;
            
            _pointers = new SparseArray(0, emptyDefault: long.MaxValue);
            _lastPointers = new SparseArray(0, emptyDefault: long.MaxValue);
        }

        private OsmTiledLinkedStream(SparseArray pointers, Stream stream)
        {
            _stream = stream;
            _pointers = pointers;
            _lastPointers = null;
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
            if (_lastPointers == null) throw new InvalidOperationException("This stream is readonly.");
            
            _pointers.EnsureMinimumSize(tile + 1);
            _lastPointers.EnsureMinimumSize(tile + 1);

            // set first pointer and get previous pointer.
            var pointer = _pointers[tile];
            var previous = long.MaxValue;
            if (pointer == long.MaxValue)
            { // first object, set pointer.
                _pointers[tile] = _stream.Position;
            }
            else
            { // update last and update pointer in previous.
                previous = _lastPointers[tile];
            }
            
            // update previous object.
            var next = _stream.Position;
            if (previous != long.MaxValue)
            {
                var before = _stream.Position;
                _stream.Seek(previous, SeekOrigin.Begin);
                _stream.Write(next);
                _stream.Seek(before, SeekOrigin.Begin);
            }

            // write data.
            _stream.WriteDynamicUInt32(1);
            _stream.Write(tile);
            _lastPointers[tile] = _stream.Position;
            _stream.Write(long.MaxValue);
            _stream.Append(osmGeo);
            return next;
        }

        public long Append(IReadOnlyCollection<uint> tiles, OsmGeo osmGeo)
        {
            if (_lastPointers == null) throw new InvalidOperationException("This stream is readonly.");
            
            if (tiles.Count == 1)
            {
                // write pointer only.
                var tile = tiles.First();
                return Append(tile, osmGeo);
            }

            var next = _stream.Position;
            var c = (uint) tiles.Count;
            _stream.WriteDynamicUInt32(c);
            
            // write tile ids.
            foreach (var tile in tiles)
            {
                _stream.Write(BitConverter.GetBytes(tile), 0, 4);
            }

            // write pointers and 
            foreach (var tile in tiles)
            {
                _pointers.EnsureMinimumSize(tile + 1);
                _lastPointers.EnsureMinimumSize(tile + 1);
                
                // set first pointer and get previous pointer.
                var pointer = _pointers[tile];
                var previous = long.MaxValue;
                if (pointer == long.MaxValue)
                { // first object, set pointer.
                    _pointers[tile] = next;
                }
                else
                { // update last and update pointer in previous.
                    previous = _lastPointers[tile];
                }
                
                // update previous object.
                if (previous != long.MaxValue)
                {
                    var before = _stream.Position;
                    _stream.Seek(previous, SeekOrigin.Begin);
                    _stream.Write(next);
                    _stream.Seek(before, SeekOrigin.Begin);
                }

                _lastPointers[tile] = _stream.Position;
                _stream.Write(long.MaxValue);
            }

            _stream.Append(osmGeo);
            return next;
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
                
                yield return _stream.ReadOsmGeo();
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