using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using OsmSharp.Db.Tiled.Collections;
using OsmSharp.Db.Tiled.Indexes.TileMaps;
using OsmSharp.Db.Tiled.Tiles;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.OsmTiled.IO;
using OsmSharp.Db.Tiled.OsmTiled.Tiles;
using OsmSharp.IO.Binary;
using OsmSharp.Logging;
using Serilog;

namespace OsmSharp.Db.Tiled.OsmTiled.Build
{
    /// <summary>
    /// Builds an OSM tiled db from an OSM stream.
    /// </summary>
    internal static class OsmTiledDbBuilder
    {
        private static readonly LRUCache<(uint x, uint y, uint zoom), Stream> StreamCache = new LRUCache<(uint x, uint y, uint zoom), Stream>(128);
        
        /// <summary>
        /// Builds a new database and write the structure to the given path.
        /// </summary>
        /// <param name="source">The source stream.</param>
        /// <param name="path">The path to store the db at.</param>
        /// <param name="zoom">The zoom.</param>
        public static async Task<OsmTiledDb> Build(this IEnumerable<OsmGeo> source, string path, uint zoom = 14)
        {
            if (source == null) { throw new ArgumentNullException(nameof(source)); }
            if (path == null) { throw new ArgumentNullException(nameof(path)); }
            if (!FileSystemFacade.FileSystem.DirectoryExists(path)) { throw new ArgumentException("Output path does not exist."); }

            StreamCache.OnRemove = (stream) =>
            {
                stream.Flush();
                stream.Dispose();
            };

            var timestamp = DateTime.MinValue;
            var nodeToTile = new TileMap();
            var wayToTiles = new TilesMap();
            var relationToTiles = new TilesMap();
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
                    
                    relationToTiles.Add(relation.Id.Value, tileSet);

                    foreach (var tileId in tileSet)
                    {
                        var tile = Tile.FromLocalId(zoom, tileId);
                        
                        WriteTo(path, (tile.x, tile.y, zoom), relation);
                    }
                }
            }
            
            StreamCache.Clear();
            
            // save tile maps and create tiles.
            Task.WaitAll(Task.Run(() => WriteIndex(path, nodeToTile)), 
                Task.Run(() => WriteIndex(path, OsmGeoType.Way, wayToTiles)), 
                Task.Run(() => WriteIndex(path, OsmGeoType.Relation, relationToTiles)),
                CreateTiles(path, zoom));

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

        private static async Task CreateTiles(string path, uint zoom)
        {
            Logger.Log(nameof(OsmTiledDbBuilder), TraceEventType.Verbose,
                $"Converting tile streams to data tiles...");
            var c = 0;
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
                using (var stream = FileSystemFacade.FileSystem.Open(dataTileFile, FileMode.Create))
                {
                    await dataTile.Serialize(stream);
                }

                // delete old file.
                FileSystemFacade.FileSystem.Delete(osmBinFile);
                c++;

                if (c % 1000 == 0)
                {
                    Logger.Log(nameof(OsmTiledDbBuilder), TraceEventType.Verbose,
                        $"Converted {c} tiles.");
                }
            }
            Logger.Log(nameof(OsmTiledDbBuilder), TraceEventType.Verbose,
                $"All {c} tiles converted.");
        }

        private static void WriteIndex(string path, TileMap tileMap)
        {
            var indexFile = OsmTiledDbOperations.PathToIndex(path, OsmGeoType.Node);
            if (FileSystemFacade.FileSystem.DirectoryExists(
                FileSystemFacade.FileSystem.DirectoryForFile(indexFile)))
            {
                FileSystemFacade.FileSystem.CreateDirectory(FileSystemFacade.FileSystem.DirectoryForFile(indexFile));
            }

            Logger.Log(nameof(OsmTiledDbBuilder), TraceEventType.Verbose,
                $"Writing Node tile map...");
            using var stream = FileSystemFacade.FileSystem.Open(indexFile, FileMode.Create);
            tileMap.Serialize(stream);
            Logger.Log(nameof(OsmTiledDbBuilder), TraceEventType.Verbose,
                $"Node tile map written.");
        }

        private static void WriteIndex(string path, OsmGeoType type, TilesMap tilesMap)
        {
            var indexFile = OsmTiledDbOperations.PathToIndex(path, type);
            if (FileSystemFacade.FileSystem.DirectoryExists(
                FileSystemFacade.FileSystem.DirectoryForFile(indexFile)))
            {
                FileSystemFacade.FileSystem.CreateDirectory(FileSystemFacade.FileSystem.DirectoryForFile(indexFile));
            }

            Logger.Log(nameof(OsmTiledDbBuilder), TraceEventType.Verbose,
                $"Writing {type} tile map...");
            using var stream = FileSystemFacade.FileSystem.Open(indexFile, FileMode.Create);
            tilesMap.Serialize(stream);
            Logger.Log(nameof(OsmTiledDbBuilder), TraceEventType.Verbose,
                $"{type} tile map written.");
        }

        private static void WriteTo(string path, (uint x, uint y, uint zoom) tile, OsmGeo osmGeo)
        {
            var pathToTile = OsmTiledDbOperations.PathToTile(path, tile, ".osm.bin");
            if (!StreamCache.TryGet(tile, out var tileStream))
            {
                var directory = FileSystemFacade.FileSystem.DirectoryForFile(pathToTile);
                if (!FileSystemFacade.FileSystem.DirectoryExists(directory))
                {
                    FileSystemFacade.FileSystem.CreateDirectory(directory);
                }
                tileStream = FileSystemFacade.FileSystem.Open(pathToTile, FileMode.Append);
                StreamCache.Add(tile, tileStream);
            }

            tileStream.Append(osmGeo);
        }
    }
}