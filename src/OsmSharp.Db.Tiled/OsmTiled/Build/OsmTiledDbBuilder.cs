using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using OsmSharp.Db.Tiled.Indexes.TileMap;
using OsmSharp.Db.Tiled.Tiles;
using OsmSharp.Streams;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.OsmTiled.IO;
using OsmSharp.Db.Tiled.OsmTiled.Tiles;
using OsmSharp.IO.Binary;

namespace OsmSharp.Db.Tiled.OsmTiled.Build
{
    /// <summary>
    /// Builds an OSM tiled db from an OSM stream.
    /// </summary>
    internal static class OsmTiledDbBuilder
    {
        /// <summary>
        /// Builds a new database and write the structure to the given path.
        /// </summary>
        /// <param name="source">The source stream.</param>
        /// <param name="path">The path to store the db at.</param>
        /// <param name="zoom">The zoom.</param>
        public static async Task<OsmTiledDb> Build(this OsmStreamSource source, string path, uint zoom = 14)
        {
            if (source == null) { throw new ArgumentNullException(nameof(source)); }
            if (path == null) { throw new ArgumentNullException(nameof(path)); }
            if (!source.CanReset) { throw new ArgumentException("Source cannot be reset."); }
            if (!FileSystemFacade.FileSystem.DirectoryExists(path)) { throw new ArgumentException("Output path does not exist."); }

            var timestamp = DateTime.MinValue;
            var nodeToTile = new SparseArray();
            var wayToTiles = new OsmGeoIdToTileMap();
            var tileSet = new HashSet<uint>();
            var mode = OsmGeoType.Node;
            foreach (var osmGeo in source)
            {
                switch (mode)
                {
                    case OsmGeoType.Way when osmGeo.Type == OsmGeoType.Node:
                    case OsmGeoType.Relation when (osmGeo.Type == OsmGeoType.Node || osmGeo.Type == OsmGeoType.Way):
                        throw new InvalidDataException("Source stream has to be sorted.");
                }
                
                // update timestamp.
                if (osmGeo.TimeStamp.HasValue &&
                    osmGeo.TimeStamp > timestamp)
                {
                    timestamp = osmGeo.TimeStamp.Value;
                }

                mode = osmGeo.Type;

                if (mode == OsmGeoType.Node)
                {
                    if (!(osmGeo is Node node)) throw new InvalidDataException("Could not cast node to node.");
                    if (node.Id == null) throw new InvalidDataException("Cannot store nodes without an id.");
                    if (!node.Latitude.HasValue || !node.Longitude.HasValue) throw new InvalidDataException("Cannot store nodes without a location.");

                    var tile = Tile.FromWorld(node.Longitude.Value, node.Latitude.Value, zoom);
                    var localId = Tile.ToLocalId(tile.x, tile.y, zoom);

                    nodeToTile.EnsureMinimumSize(node.Id.Value);
                    nodeToTile[node.Id.Value] = localId;
                    
                    WriteTo(path, (tile.x, tile.y, zoom), node);
                }
                else if(osmGeo is Way way)
                {
                    if (way.Nodes == null) continue;
                    if (way.Id == null) throw new InvalidDataException("Cannot store ways without an id.");
                    
                    tileSet.Clear();
                    foreach (var n in way.Nodes)
                    {
                        if (nodeToTile.Length <= n) continue;
                        var tile = nodeToTile[n];
                        if (tile == 0) continue;
                        
                        tileSet.Add(tile);
                    }

                    wayToTiles.Add(way.Id.Value, tileSet);

                    foreach (var tileId in tileSet)
                    {
                        var tile = Tile.FromLocalId(zoom, tileId);
                        
                        WriteTo(path, (tile.x, tile.y, zoom), way);
                    }
                }
                else if(osmGeo is Relation relation)
                {
                    if (relation.Members == null) continue;
                    if (relation.Id == null) throw new InvalidDataException("Cannot store relations without an id.");
                    
                    tileSet.Clear();
                    foreach (var member in relation.Members)
                    {
                        switch (member.Type)
                        {
                            case OsmGeoType.Node:
                                if (nodeToTile.Length <= member.Id) continue;
                                var tile = nodeToTile[member.Id];
                                if (tile == 0) continue;
                        
                                tileSet.Add(tile);
                                break;
                            case OsmGeoType.Way:
                                foreach (var t in wayToTiles.Get(member.Id))
                                {
                                    tileSet.Add(t);
                                }
                                break;
                        }
                    }

                    foreach (var tileId in tileSet)
                    {
                        var tile = Tile.FromLocalId(zoom, tileId);
                        
                        WriteTo(path, (tile.x, tile.y, zoom), relation);
                    }
                }
            }
            
            FlushQueue(path);
            
            // convert all tiles to data tiles.
            foreach (var tile in OsmTiledDbOperations.GetTiles(path, zoom, "*.osm.bin"))
            {
                // build data tile.
                OsmDbTile dataTile;
                var osmBinFile = OsmTiledDbOperations.PathToTile(path, tile, ".osm.bin");
                using (var osmBinStream =
                    FileSystemFacade.FileSystem.OpenRead(osmBinFile))
                {
                    dataTile = await OsmDbTile.BuildFromOsmBinaryStream(osmBinStream);
                }
                
                // write data tile.
                var dataTileFile = OsmTiledDbOperations.PathToTile(path, tile);
                using (var stream = File.Open(dataTileFile, FileMode.Create))
                {
                    await dataTile.Serialize(stream);
                }
                
                // delete old file.
                FileSystemFacade.FileSystem.Delete(osmBinFile);
            }

            // save the meta-data.
            var dbMeta = new OsmTiledDbMeta
            {
                Base = null, // this is a full db.
                Type = OsmTiledDbType.Full,
                Zoom = zoom,
                Timestamp = timestamp
            };
            OsmTiledDbOperations.SaveDbMeta(path, dbMeta);
            
            return new OsmTiledDb(path);
        }

        private static readonly Queue<OsmGeo> Queue = new Queue<OsmGeo>();
        private static (uint x, uint y, uint zoom) _queueTile = (uint.MaxValue, uint.MaxValue, uint.MaxValue);

        private static void FlushQueue(string path)
        {
            if (_queueTile.x == uint.MaxValue) return;
            
            var pathToTile = OsmTiledDbOperations.PathToTile(path, _queueTile, ".osm.bin");
            var directory = FileSystemFacade.FileSystem.DirectoryForFile(pathToTile);
            if (!FileSystemFacade.FileSystem.DirectoryExists(directory))
            {
                FileSystemFacade.FileSystem.CreateDirectory(directory);
            }

            using var stream = FileSystemFacade.FileSystem.Open(pathToTile, FileMode.Append);
            while (Queue.Count > 0)
            {
                stream.Append(Queue.Dequeue());
            }

            _queueTile = (uint.MaxValue, uint.MaxValue, uint.MaxValue);
        }

        private static void WriteTo(string path, (uint x, uint y, uint zoom) tile, OsmGeo osmGeo)
        {
            if (tile != _queueTile)
            {
                FlushQueue(path);
            }

            Queue.Enqueue(osmGeo);
            _queueTile = tile;
        }
    }
}