using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using OsmSharp.Db.Tiled.Collections;
using OsmSharp.Db.Tiled.Indexes.TileMaps;
using OsmSharp.Db.Tiled.Tiles;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.OsmTiled.IO;
using OsmSharp.Logging;
using TraceEventType = OsmSharp.Logging.TraceEventType;

namespace OsmSharp.Db.Tiled.OsmTiled.Build
{
    /// <summary>
    /// Builds an OSM tiled db from an OSM stream.
    /// </summary>
    internal static class OsmTiledDbBuilder
    {
        private static readonly LRUCache<(uint x, uint y, uint zoom), Stream> StreamCache = new LRUCache<(uint x, uint y, uint zoom), Stream>(4);
        
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

            var timestamp = DateTime.MinValue;
            var nodeToTile = new TileMap();
            var wayToTiles = new TilesMap();
            var relationToTiles = new TilesMap();

            using var data = FileSystemFacade.FileSystem.Open(
                FileSystemFacade.FileSystem.Combine(path, "osm.db"), FileMode.Create);
            using var dataIndex = FileSystemFacade.FileSystem.Open(
                FileSystemFacade.FileSystem.Combine(path, "osm.db.idx"), FileMode.Create);
            var tiledStream = new OsmTiledLinkedStream(data);
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

                    tiledStream.Append(localId, node);
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
                    
                    tiledStream.Append(tileSet, way);
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
                    
                    tiledStream.Append(tileSet, relation);
                }
            }
            
            // save tile maps and create tiles.
            Task.WaitAll(Task.Run(() => WriteIndex(path, nodeToTile)), 
                Task.Run(() => WriteIndex(path, OsmGeoType.Way, wayToTiles)), 
                Task.Run(() => WriteIndex(path, OsmGeoType.Relation, relationToTiles)),
                Task.Run(() =>
                {
                    tiledStream.Reverse();
                    tiledStream.SerializeIndex(dataIndex);
                }));

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
    }
}