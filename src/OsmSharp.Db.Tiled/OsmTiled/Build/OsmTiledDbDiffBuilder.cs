using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using OsmSharp.Changesets;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.OsmTiled.IO;
using OsmSharp.Db.Tiled.Tiles;

namespace OsmSharp.Db.Tiled.OsmTiled.Build
{
    /// <summary>
    /// Builds an OSM tiled db diff from an OSM stream.
    /// </summary>
    internal static class OsmTiledDbDiffBuilder
    {
        /// <summary>
        /// Builds a new database and write the structure to the given path.
        /// </summary>
        /// <param name="osmTiledDb">The tiled db.</param>
        /// <param name="changeset">The changeset stream.</param>
        /// <param name="path">The path to store the db at.</param>
        /// <param name="createId">A function to create ids for objects that are new.</param>
        /// <param name="settings">The settings.</param>
        public static async Task ApplyChangset(this OsmTiledDb osmTiledDb, OsmChange changeset, string path, 
            Func<OsmGeoKey, OsmGeoKey> createId = null, OsmTiledDbDiffBuildSettings settings = null)
        {
            var buffer = new byte[1024];
            var zoom = osmTiledDb.Zoom;
            
            // generate new ids and update them everywhere.
            if (createId != null) GenerateIds(changeset, createId);
            
            // collect all affected tiles and tile mutations.
            var affectedTiles = new HashSet<uint>();
            var newTilesIndex = new Dictionary<OsmGeoKey, IEnumerable<uint>>();
            var changedObjects = new SortedSet<OsmGeoKey>();
            var tileMutations = new Dictionary<uint, TileMutation>();

            async Task<IEnumerable<uint>> GetTilesFor(OsmGeoKey key)
            {
                // this key already exists, also query old tiles.
                var oldTiles = (await osmTiledDb.GetTiles(key.Type, key.Id, buffer)).Select(x => Tile.ToLocalId(x, osmTiledDb.Zoom));
                if (!newTilesIndex.TryGetValue(key, out var newTiles)) return oldTiles;

                var tilesSet = new HashSet<uint>(oldTiles);
                foreach (var newTile in newTiles)
                {
                    tilesSet.Add(newTile);
                }

                return tilesSet;
            }

            // process new objects, assign ids and collect affected tiles.
            var timestamp = DateTime.MinValue;
            if (changeset.Create != null)
            {
                foreach (var create in changeset.Create)
                {
                    if (create == null) continue;
                
                    // update timestamp.
                    if (create.TimeStamp.HasValue &&
                        create.TimeStamp > timestamp)
                    {
                        timestamp = create.TimeStamp.Value;
                    }
                    
                    // collect tiles per object.
                    var key = new OsmGeoKey(create);
                    changedObjects.Add(key);
                    var tileSet = new HashSet<uint>();
                    if (create is Node newNode)
                    {
                        if (newNode.Latitude == null || newNode.Longitude == null) throw new InvalidDataException("Cannot store node without a valid location.");
                        
                        var tile = Tile.FromWorld(newNode.Longitude.Value, newNode.Latitude.Value, zoom);
                        var localId = Tile.ToLocalId(tile.x, tile.y, zoom);

                        tileSet.Add(localId);
                    }
                    else if (create is Way way)
                    {
                        foreach (var n in way.Nodes)
                        {
                            var nodeTiles = await GetTilesFor(key);
                            foreach (var nodeTile in nodeTiles)
                            {
                                tileSet.Add(nodeTile);
                            }
                        }
                    }
                    else if (create is Relation relation)
                    {
                        foreach (var m in relation.Members)
                        {
                            var memberTiles = await GetTilesFor(key);
                            foreach (var nodeTile in memberTiles)
                            {
                                tileSet.Add(nodeTile);
                            }
                        }
                    }
                    
                    // save the tile mutations and affected tiles.
                    foreach (var tile in tileSet)
                    {
                        if (!tileMutations.TryGetValue(tile, out var tileMutation))
                        {
                            tileMutation = new TileMutation();
                            tileMutations[tile] = tileMutation;
                        }

                        tileMutation.Mutations[key] = create;
                        
                        affectedTiles.Add(tile);
                    }

                    // save the tiles, this key is new.
                    newTilesIndex[key] = tileSet;
                }
            }
            if (changeset.Delete != null)
            {
                foreach (var deleted in changeset.Delete)
                {
                    if (deleted == null) continue;
                
                    // update timestamp.
                    if (deleted.TimeStamp.HasValue &&
                        deleted.TimeStamp > timestamp)
                    {
                        timestamp = deleted.TimeStamp.Value;
                    }
                    
                    var key = new OsmGeoKey(deleted);
                    changedObjects.Add(key);
                    
                    // save the tile mutations and affected tiles.
                    var deletedTiles = (await osmTiledDb.GetTiles(key.Type, key.Id, buffer)).Select(x => Tile.ToLocalId(x, osmTiledDb.Zoom));
                    foreach (var tile in deletedTiles)
                    {
                        if (!tileMutations.TryGetValue(tile, out var tileMutation))
                        {
                            tileMutation = new TileMutation();
                            tileMutations[tile] = tileMutation;
                        }

                        tileMutation.Mutations[key] = null;
                        
                        affectedTiles.Add(tile);
                    }
                }
            }
            if (changeset.Modify != null)
            {
                foreach (var modify in changeset.Modify)
                {
                    if (modify == null) continue;
                
                    // update timestamp.
                    if (modify.TimeStamp.HasValue &&
                        modify.TimeStamp > timestamp)
                    {
                        timestamp = modify.TimeStamp.Value;
                    }
                    
                    // collect tiles per object.
                    var key = new OsmGeoKey(modify);  
                    changedObjects.Add(key);
                    
                    var tileSet = new HashSet<uint>();
                    if (modify is Node newNode)
                    {
                        if (newNode.Latitude == null || newNode.Longitude == null) throw new InvalidDataException("Cannot store node without a valid location.");
                        
                        var tile = Tile.FromWorld(newNode.Longitude.Value, newNode.Latitude.Value, zoom);
                        var localId = Tile.ToLocalId(tile.x, tile.y, zoom);

                        tileSet.Add(localId);
                    }
                    else if (modify is Way way)
                    {
                        foreach (var n in way.Nodes)
                        {
                            var nodeTiles = GetTilesFor(key);
                            foreach (var nodeTile in nodeTiles)
                            {
                                tileSet.Add(nodeTile);
                            }
                        }
                    }
                    else if (modify is Relation relation)
                    {
                        foreach (var m in relation.Members)
                        {
                            var memberTiles = GetTilesFor(key);
                            foreach (var nodeTile in memberTiles)
                            {
                                tileSet.Add(nodeTile);
                            }
                        }
                    }
                    
                    // remove objects that have moved between tiles.
                    var oldTiles = new HashSet<uint>(getTilesFor(key));
                    var identical = true;
                    foreach (var tile in oldTiles)
                    {
                        if (tileSet.Contains(tile)) continue;

                        identical = false;
                        if (!tileMutations.TryGetValue(tile, out var tileMutation))
                        {
                            tileMutation = new TileMutation();
                            tileMutations[tile] = tileMutation;
                        }

                        tileMutation.Mutations[key] = null;
                    }
                    
                    // add objects to tiles that need to have the modified data.
                    foreach (var tile in tileSet)
                    {
                        if (!tileMutations.TryGetValue(tile, out var tileMutation))
                        {
                            tileMutation = new TileMutation();
                            tileMutations[tile] = tileMutation;
                        }

                        tileMutation.Mutations[key] = modify;

                        if (!oldTiles.Contains(tile)) identical = false;
                    }
                        
                    // save the tiles, the set has changed.
                    if (!identical) newTilesIndex[key] = tileSet;
                }
            }

            using var data = FileSystemFacade.FileSystem.Open(
                OsmTiledDbOperations.PathToData(path), FileMode.Create);
            using var dataTilesIndex = FileSystemFacade.FileSystem.Open(
                OsmTiledDbOperations.PathToTileIndex(path), FileMode.Create);
            using var dataIdIndex = FileSystemFacade.FileSystem.Open(
                OsmTiledDbOperations.PathToIdIndex(path), FileMode.Create);
            
            var tiledStream = new OsmTiledLinkedStream(data);
            var idIndex = new OsmTiledIndex(dataIdIndex);

            foreach (var key in changedObjects)
            {
                // write objects.
                if (!newTilesIndex.TryGetValue(key, out var tiles))
                {
                    tiles = getTilesFor(key);
                }

                OsmGeo osmGeo = null;
                var isDeleted = false;
                foreach (var tile in tiles)
                {
                    if (!tileMutations.TryGetValue(tile, out var tileMutation)) continue;

                    if (!tileMutation.Mutations.TryGetValue(key, out var mutated)) continue;
                    
                    if (mutated == null)
                    {
                        if (key.Type == OsmGeoType.Node)
                        {
                            osmGeo = new Node()
                            {
                                Id = osmGeo.Id
                            };
                        }
                        else if (key.Type == OsmGeoType.Way)
                        {
                            osmGeo = new Way()
                            {
                                Id = osmGeo.Id
                            };
                        }
                        else if (key.Type == OsmGeoType.Relation)
                        {
                            osmGeo = new Way()
                            {
                                Id = osmGeo.Id
                            };
                        }
                    }
                    else
                    {
                        osmGeo = mutated;
                    }
                    break;
                }
                
                tiledStream.Append(tiles.ToArray(), osmGeo, buffer);
            }
            
            // reverse indexed data and save tile index.
            tiledStream.SerializeIndex(dataTilesIndex);

            // save the meta-data.
            var dbMeta = new OsmTiledDbMeta
            {
                Base = basePath, 
                Type = OsmTiledDbType.Diff,
                Zoom = zoom,
                Timestamp = timestamp
            };
            OsmTiledDbOperations.SaveDbMeta(path, dbMeta);
        }

        private static void GenerateIds(OsmChange changeset, Func<OsmGeoKey, OsmGeoKey> generateIds)
        {
            throw new NotImplementedException();
        }
        
        private class TileMutation
        {
            public SortedDictionary<OsmGeoKey, OsmGeo> Mutations { get; } = new SortedDictionary<OsmGeoKey, OsmGeo>();
        }
    }
}