using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using OsmSharp.Db.Tiled.Collections;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.Logging;
using OsmSharp.Db.Tiled.Tiles;
using OsmSharp.IO.Binary;

namespace OsmSharp.Db.Tiled.OsmTiled.Data
{
    internal class OsmTiledLinkedStream : IDisposable
    {
        private readonly Stream _data;
        //private readonly OsmTiledDbTileIndexArray _pointers;
        private readonly OsmTiledDbTileIndex? _previousPointers;
        private readonly long[]? _pointers1;
        private readonly long[]? _pointers2;
        private readonly uint _zoom;

        private const long NoData = long.MaxValue;

        public const int PointerCacheSizeDefault = 1024 * 1024 * 32;

        public OsmTiledLinkedStream(Stream data, uint zoom = 14, int pointersCacheSize = PointerCacheSizeDefault)
        {
            _data = data;
            _zoom = zoom;
            
            _data.WriteUInt32(zoom);
            
            //_pointers = new OsmTiledDbTileIndexArray(0, emptyDefault: NoData);
            _previousPointers = new OsmTiledDbTileIndex(0, emptyDefault: NoData);
            if (pointersCacheSize > 0)
            {
                _pointers1 = new long[pointersCacheSize];
                _pointers2 = new long[pointersCacheSize];
            }
        }

        private OsmTiledLinkedStream(Stream data, uint zoom)
        {
            _data = data;
            //_pointers = pointers;
            _zoom = zoom;
        }

        private long _nextDelayedPointer = 0;

        private void FlushDelayedPointers()
        {
            if (_pointers1 == null || _pointers2 == null) return;
            
            Array.Sort(_pointers1, _pointers2, 0, (int)_nextDelayedPointer);

            Log.Default.Verbose($"Flushing {_nextDelayedPointer}...");
            var before = _data.Position;
            for (var i = 0; i < _nextDelayedPointer; i++)
            {
                _data.Seek(_pointers1[i], SeekOrigin.Begin);
                _data.WriteInt64(_pointers2[i]);
            }

            _data.Seek(before, SeekOrigin.Begin);
            _nextDelayedPointer = 0;
        }

        public OsmGeo Get(long pointer, byte[] buffer)
        {
            if (buffer.Length < 1024) Array.Resize(ref buffer, 1024);
            
            _data.Seek(pointer, SeekOrigin.Begin);

            // find tile.
            var c = _data.ReadVarUInt32();
            if (c != 1)
            {
                c -= 1;
                for (var i = 0; i < c; i++)
                {
                    _data.ReadVarUInt32();
                }
            }

            // skip pointers.
            _data.Seek(c * 8, SeekOrigin.Current);

            return _data.ReadOsmGeo(buffer);
        }

        public IEnumerable<uint> GetTilesFor(long pointer)
        {
            _data.Seek(pointer, SeekOrigin.Begin);

            var c = _data.ReadVarUInt32();
            if (c == 1)
            {
                // pointers, read node, determine tile.
                _data.Seek(c * 8, SeekOrigin.Current);

                // TODO: find a way to only read lat/lon.
                if (!(_data.ReadOsmGeo() is Node node)) throw new InvalidDataException("Expected node.");

                var tileId = ToTile(node);
                if (!tileId.HasValue) throw new InvalidDataException("Expected node with a valid location and tile.");
                yield return tileId.Value;
                yield break;
            }

            if (c == 0) yield break;
            c--;
            for (var i = 0; i < c; i++)
            {
                yield return _data.ReadVarUInt32();
            }
        }

        public long Append(uint tile, OsmGeo osmGeo, byte[] buffer)
        {
            if (buffer.Length < 1024) Array.Resize(ref buffer, 1024);
            if (_previousPointers == null) throw new InvalidOperationException("Stream is not writeable.");
            
            //_pointers.EnsureMinimumSize(tile + 1);
            _previousPointers.EnsureMinimumSize(tile + 1);

            var isNodeInOneTile = false;
            if (osmGeo is Node node)
            {
                isNodeInOneTile = true;
                var nodeTileId = ToTile(node);
                if (nodeTileId == null ||
                    nodeTileId.Value != tile)
                {
                    // when node is not in the correct tile, store it as a way with one tile.
                    isNodeInOneTile = false;
                }
            }

            var previousPointer = _previousPointers[tile];
            // if (previousPointer == NoData)
            // {
            //     _pointers[tile] = _data.Position;
            // }

            var pointer = _data.Position;
            if (isNodeInOneTile)
            {
                _data.WriteVarUInt32(1);
            }
            else
            {
                _data.WriteVarUInt32(2);
                _data.WriteVarUInt32(tile);
            }
            _previousPointers[tile] = _data.Position;
            if (previousPointer != NoData)
            {
                if (_pointers1 != null &&
                    _pointers2 != null &&
                    _data is HugeBufferedStream bufferedStream &&
                    !bufferedStream.IsInBuffer(previousPointer))
                {
                    // delay write, outside of buffer.
                    _pointers1[_nextDelayedPointer] = previousPointer;
                    _pointers2[_nextDelayedPointer] = pointer;
                    _nextDelayedPointer++;

                    if (_nextDelayedPointer == _pointers1.Length) FlushDelayedPointers();
                }
                else
                {
                    // write pointer, in buffer.
                    var before = _data.Position;
                    _data.Seek(previousPointer, SeekOrigin.Begin);
                    _data.WriteInt64(pointer);
                    _data.Seek(before, SeekOrigin.Begin);
                }
            }
            _data.WriteInt64(NoData);
            _data.Append(osmGeo, buffer);
            return pointer;
        }

        public long Append(IReadOnlyCollection<uint> tiles, OsmGeo osmGeo, byte[] buffer)
        {
            if (buffer.Length < 1024) Array.Resize(ref buffer, 1024);
            if (_previousPointers == null) throw new InvalidOperationException("Stream is not writeable.");
            
            var c = (uint) tiles.Count;

            if (c == 1)
            {
                return this.Append(tiles.First(), osmGeo, buffer);
            }
            
            // write count with one off.
            var position = _data.Position;
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
                //_pointers.EnsureMinimumSize(tile + 1);
                _previousPointers.EnsureMinimumSize(tile + 1);
                
                var previousPointer = _previousPointers[tile];
                // if (previousPointer == NoData)
                // {
                //     _pointers[tile] = position;
                // }
                
                _previousPointers[tile] = p + (t * 8);
                t++;
                
                if (previousPointer == NoData) continue;
                
                if (_pointers1 != null &&
                    _pointers2 != null &&
                    _data is HugeBufferedStream bufferedStream &&
                    !bufferedStream.IsInBuffer(previousPointer))
                {
                    // delay write, outside of buffer.
                    _pointers1[_nextDelayedPointer] = previousPointer;
                    _pointers2[_nextDelayedPointer] = position;
                    _nextDelayedPointer++;

                    if (_nextDelayedPointer == _pointers1.Length) FlushDelayedPointers();
                }
                else
                {
                    _data.Seek(previousPointer, SeekOrigin.Begin);
                    _data.WriteInt64(position);
                }
            }
            
            // write end pointers.
            _data.Seek(p, SeekOrigin.Begin);
            foreach (var _ in tiles)
            {
                _data.WriteInt64(NoData);
            }

            _data.Append(osmGeo, buffer);
            return position;
        }

        private uint? ToTile(Node node)
        {
            if (!node.Longitude.HasValue || !node.Latitude.HasValue) return null;
            var tile = Tile.FromWorld(node.Longitude.Value, node.Latitude.Value, _zoom);
            return Tile.ToLocalId(tile, _zoom);
        }

        public IEnumerable<(OsmGeo osmGeo, IEnumerable<uint> tile)> Get(byte[] buffer)
        {
            if (buffer.Length < 1024) Array.Resize(ref buffer, 1024);
            var tilesToReturn = new HashSet<uint>();

            _data.Seek(0, SeekOrigin.Begin);
            _data.ReadUInt32();
            while (_data.Position < _data.Length)
            {
                // read tile count.
                var c = _data.ReadVarUInt32();
                var isNode = c == 1;
                if (c > 1) c -= 1;

                // find tile(s).
                tilesToReturn.Clear();
                if (!isNode)
                {
                    for (var t = 0; t < c; t++)
                    {
                        var currentTile = _data.ReadVarUInt32();
                        tilesToReturn.Add(currentTile);
                    }
                }
                
                // skip over pointers.
                _data.Seek(8 * c, SeekOrigin.Current);

                if (isNode)
                {
                    if (!(_data.ReadOsmGeo(buffer) is Node node)) throw new InvalidDataException("Node expected.");

                    var tileId = ToTile(node);
                    if (!tileId.HasValue) throw new InvalidDataException("Expected node with a valid location and tile.");
                    tilesToReturn.Add(tileId.Value);
                    yield return (node, tilesToReturn);
                }
                else
                {
                    yield return (_data.ReadOsmGeo(buffer), tilesToReturn);
                }
            }
        }
        
        public IEnumerable<OsmGeo> GetForTile(long startPointer, uint tile, byte[] buffer)
        {
            if (buffer.Length < 1024) Array.Resize(ref buffer, 1024); 
            
            foreach (var (osmGeoPointer, _) in this.GetForTilePointers(startPointer, tile))
            {
                _data.Seek(osmGeoPointer, SeekOrigin.Begin);
                yield return _data.ReadOsmGeo(buffer);
            }
        }
        
        public IEnumerable<(OsmGeo osmGeo, List<uint> tile)> GetForTiles(IEnumerable<long> startPointers, IEnumerable<uint> tiles, byte[] buffer)
        {
            if (buffer?.Length < 1024) Array.Resize(ref buffer, 1024);
            var tilesToReturn = new List<uint>();
            
            var queue = new BinaryHeap();
            var tilesSet = new HashSet<uint>(tiles);
            foreach (var pointer in startPointers)
            {
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

                // find tile(s).
                tilesToReturn.Clear();
                if (!isNode)
                {
                    for (var t = 0; t < c; t++)
                    {
                        var currentTile = _data.ReadVarUInt32();
                        if (tilesSet.Contains(currentTile))
                        {
                            tileFlags[t] = true;
                            tilesToReturn.Add(currentTile);
                        }
                        else
                        {
                            tileFlags[t] = false;
                        }
                    }
                }
                else
                {
                    tileFlags[0] = true;
                }

                // queue next tiles.
                for (var t = 0; t < c; t++)
                {
                    var nextPointer = _data.ReadInt64();
                    if (nextPointer == NoData) continue;

                    if (tileFlags[t])
                    {
                        queue.Push(nextPointer);
                    }
                }

                if (isNode)
                {
                    if (!(_data.ReadOsmGeo(buffer) is Node node)) throw new InvalidDataException("Node expected.");

                    var tileId = ToTile(node);
                    if (!tileId.HasValue) throw new InvalidDataException("Expected node with a valid location and tile.");
                    tilesToReturn.Add(tileId.Value);
                    yield return (node, tilesToReturn);
                }
                else
                {
                    yield return (_data.ReadOsmGeo(buffer), tilesToReturn);
                }
            }
        }

        private IEnumerable<(long pointer, long osmGeoPointer)> GetForTilePointers(long startPointer, uint tile)
        {
            var pointer = startPointer;
            while (pointer != NoData)
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

        public static OsmTiledLinkedStream Deserialize(Stream stream)
        {
            var zoom = stream.ReadUInt32();
            
            return new OsmTiledLinkedStream(stream, zoom);
        }

        public void Flush()
        {
            this.FlushDelayedPointers();

            _data.Flush();
        }
        
        public void Dispose()
        {
            _data?.Dispose();
        }
    }
}