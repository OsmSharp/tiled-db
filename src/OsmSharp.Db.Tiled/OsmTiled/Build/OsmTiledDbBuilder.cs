using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using OsmSharp.Db.Tiled.Indexes.TileMaps;
using OsmSharp.Db.Tiled.Tiles;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.OsmTiled.IO;

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
        /// <param name="settings">The settings.</param>
        public static async Task Build(this IEnumerable<OsmGeo> source, string path, uint zoom = 14,
            OsmTiledDbBuildSettings? settings = null)
        {
            if (source == null) { throw new ArgumentNullException(nameof(source)); }
            if (path == null) { throw new ArgumentNullException(nameof(path)); }
            if (!FileSystemFacade.FileSystem.DirectoryExists(path)) { throw new ArgumentException("Output path does not exist."); }
            
            settings ??= new OsmTiledDbBuildSettings();

            var buffer = new byte[1024];
            
            var timestamp = DateTime.MinValue;
            var nodeToTile = new TileMap();
            var wayToTiles = new TilesMap();
            var relationToTiles = new TilesMap();

            using var dataBase = FileSystemFacade.FileSystem.Open(
                OsmTiledDbOperations.PathToData(path), FileMode.Create);
            var data = new HugeBufferedStream(dataBase);
            using var dataTilesIndex = FileSystemFacade.FileSystem.Open(
                OsmTiledDbOperations.PathToTileIndex(path), FileMode.Create);
            using var dataIdIndex = FileSystemFacade.FileSystem.Open(
                OsmTiledDbOperations.PathToIdIndex(path), FileMode.Create);
            
            var tiledStream = new OsmTiledLinkedStream(data);
            var idIndex = new OsmTiledIndex(dataIdIndex);
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

                    Prepare(node, settings);
                    var location = tiledStream.Append(localId, node, buffer);
                    idIndex.Append(new OsmGeoKey(node), location);
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
                    
                    Prepare(way, settings);
                    var location = tiledStream.Append(tileSet, way, buffer);
                    idIndex.Append(new OsmGeoKey(way), location);
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
                    
                    Prepare(relation, settings);
                    var location = tiledStream.Append(tileSet, relation, buffer);
                    idIndex.Append(new OsmGeoKey(relation), location);
                }
            }
            
            // reverse indexed data and save tile index.
            data.Flush();
            tiledStream.SerializeIndex(dataTilesIndex);

            // save the meta-data.
            var dbMeta = new OsmTiledDbMeta
            {
                Base = null, // this is a full db.
                Type = OsmTiledDbType.Full,
                Zoom = zoom,
                Timestamp = timestamp
            };
            OsmTiledDbOperations.SaveDbMeta(path, dbMeta);
        }

        private static void Prepare(this OsmGeo osmGeo, OsmTiledDbBuildSettings settings)
        {
            if (!settings.IncludeChangeset) osmGeo.ChangeSetId = null;
            if (!settings.IncludeUsername) osmGeo.UserName = null;
            if (!settings.IncludeUserId) osmGeo.UserId = null;
            if (!settings.IncludeVisible) osmGeo.Visible = null;
        }
    }
}