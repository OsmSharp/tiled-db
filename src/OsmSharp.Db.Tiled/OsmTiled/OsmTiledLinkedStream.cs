using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using OsmSharp.Db.Tiled.Collections;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.OsmTiled.IO;
using OsmSharp.Db.Tiled.Tiles;
using OsmSharp.IO.Binary;

namespace OsmSharp.Db.Tiled.OsmTiled
{
    internal class OsmTiledLinkedStream : IDisposable
    {
        private readonly Stream _data;
        private readonly SparseArray _pointers;
        private readonly SparseArray _previousPointers;
        private readonly uint _zoom;

        public OsmTiledLinkedStream(Stream data, uint zoom = 14)
        {
            _data = data;
            _zoom = zoom;
            
            _data.WriteUInt32(zoom);
            
            _pointers = new SparseArray(0, emptyDefault: long.MaxValue);
            _previousPointers = new SparseArray(0, emptyDefault: long.MaxValue);
        }

        private OsmTiledLinkedStream(SparseArray pointers, Stream data)
        {
            _data = data;
            _pointers = pointers;
            _zoom = _data.ReadUInt32();
        }

        public OsmGeo Get(long pointer, byte[] buffer = null)
        {
            if (buffer?.Length < 1024) buffer = null;
            buffer ??= new byte[1024];
            
            _data.Seek(pointer, SeekOrigin.Begin);

            // find tile.
            var c = _data.ReadVarUInt32();
            for (var i = 0; i < c; i++)
            {
                _data.ReadVarUInt32();
            }
            
            // skip pointers.
            _data.Seek(c * 8, SeekOrigin.Current);

            return _data.ReadOsmGeo(buffer);
        }

        public IEnumerable<uint> GetTilesFor(long pointer)
        {
            _data.Seek(pointer, SeekOrigin.Begin);

            var c = _data.ReadVarUInt32();
            for (var i = 0; i < c; i++)
            {
                yield return _data.ReadVarUInt32();
            }
        }

        public long Append(uint tile, OsmGeo osmGeo, byte[] buffer = null)
        {
            _pointers.EnsureMinimumSize(tile + 1);
            _previousPointers.EnsureMinimumSize(tile + 1);

            var isNode = false;
            if (osmGeo is Node node)
            {
                isNode = true;
                var nodeTileId = ToTile(node);
                if (nodeTileId != tile) throw new InvalidDataException("Node is not in correct tile.");
            }

            var previousPointer = _previousPointers[tile];
            if (previousPointer == long.MaxValue)
            {
                _pointers[tile] = _data.Position;
            }

            var pointer = _data.Position;
            if (isNode)
            {
                _data.WriteVarUInt32(1);
            }
            else
            {
                _data.WriteVarUInt32(2);
                _data.WriteVarUInt32(tile);
            }
            _previousPointers[tile] = _data.Position;
            if (previousPointer != long.MaxValue)
            {
                var before = _data.Position;
                _data.Seek(previousPointer, SeekOrigin.Begin);
                _data.WriteInt64(pointer);
                _data.Seek(before, SeekOrigin.Begin);
            }
            _data.WriteInt64(long.MaxValue);
            _data.Append(osmGeo, buffer);
            return pointer;
        }

        public long Append(IReadOnlyCollection<uint> tiles, OsmGeo osmGeo, byte[] buffer = null)
        {
            var position = _data.Position;
            var c = (uint) tiles.Count;

            if (c == 1)
            {
                return this.Append(tiles.First(), osmGeo, buffer);
            }
            
            // write count with one off.
            if (c == 0)
            {
                _data.WriteVarUInt32(0);
            }
            else
            {
                _data.WriteVarUInt32(c + 1);
            }
            
            // write tile ids, followed by the pointers.
            foreach (var tile in tiles)
            {
                _data.WriteVarUInt32(tile);
            }

            // overwrite previous pointers.
            var p = _data.Position;
            var t = 0;
            foreach (var tile in tiles)
            {
                _pointers.EnsureMinimumSize(tile + 1);
                _previousPointers.EnsureMinimumSize(tile + 1);
                
                var previousPointer = _previousPointers[tile];
                if (previousPointer == long.MaxValue)
                {
                    _pointers[tile] = position;
                }
                
                _previousPointers[tile] = p + (t * 8);
                t++;
                
                if (previousPointer == long.MaxValue) continue;
                
                _data.Seek(previousPointer, SeekOrigin.Begin);
                _data.WriteInt64(position);
            }
            
            // write end pointers.
            _data.Seek(p, SeekOrigin.Begin);
            foreach (var _ in tiles)
            {
                _data.WriteInt64(long.MaxValue);
            }

            _data.Append(osmGeo, buffer);
            return position;
        }

        private uint ToTile(Node node)
        {
            if (!node.Longitude.HasValue || !node.Latitude.HasValue) throw new InvalidDataException("Not without latitude or longitude cannot be read.");
            var tile = Tile.FromWorld(node.Longitude.Value, node.Latitude.Value, _zoom);
            return Tile.ToLocalId(tile, _zoom);
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

        public IEnumerable<OsmGeo> GetForTiles(IEnumerable<uint> tiles)
        {
            var queue = new BinaryHeap();
            var tilesSet = new HashSet<uint>(tiles);
            foreach (var tile in tilesSet)
            {
                var pointer = _pointers[tile];
                if (pointer == long.MaxValue) continue;
                
                queue.Push(pointer);
            }

            var tileFlags = new bool[1024];
            var previousPointer = -1L;
            while (queue.Count > 0)
            {
                var pointer = queue.Pop();
                if (previousPointer == pointer) continue;
                previousPointer = pointer;
                
                // seek to object.
                _data.Seek(pointer, SeekOrigin.Begin);
                
                // read tile count.
                var c = _data.ReadVarUInt32();
                var isNode = c == 1;
                if (c > 1) c -= 1;
                if (tileFlags.Length < c) Array.Resize(ref tileFlags, (int)c);

                // find tile.
                if (!isNode)
                {
                    for (var t = 0; t < c; t++)
                    {
                        var currentTile = _data.ReadVarUInt32();
                        tileFlags[t] = tilesSet.Contains(currentTile);
                    }
                }

                // queue next tiles.
                for (var t = 0; t < c; t++)
                {
                    var nextPointer = _data.ReadInt64();
                    if (nextPointer == long.MaxValue) continue;

                    if (tileFlags[t])
                    {
                        queue.Push(nextPointer);
                    }
                }
                
                yield return _data.ReadOsmGeo();
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
                var isNode = c == 1;
                if (c > 1) c -= 1;

                // find tile.
                var tIndex = -1;
                if (isNode)
                {
                    tIndex = 0;
                }
                else
                {
                    for (var t = 0; t < c; t++)
                    {
                        var currentTile = _data.ReadVarUInt32();
                        if (currentTile == tile)
                        {
                            tIndex = t;
                        }
                    }
                    if (tIndex < 0) throw new InvalidDataException("Tile not found, should always be there!");
                }

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