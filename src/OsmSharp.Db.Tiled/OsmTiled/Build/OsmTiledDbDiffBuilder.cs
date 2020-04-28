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
        /// <param name="settings">The settings.</param>
        public static async Task ApplyChangSet(this OsmTiledDb osmTiledDb, OsmChange changeset, string path, 
            OsmTiledDbDiffBuildSettings? settings = null)
        {
            settings ??= new OsmTiledDbDiffBuildSettings();
            
            var zoom = osmTiledDb.Zoom;
            
            // collect all affected tiles and tile mutations.
            var modifications = new SortedDictionary<OsmGeoKey, (IEnumerable<uint>? tiles, OsmGeo? osmGeo)>();
            var modifiedTiles = new HashSet<uint>();

            IEnumerable<uint> GetTilesFor(OsmGeoKey key)
            {
                if (modifications == null) throw new InvalidOperationException();
                if (modifications.TryGetValue(key, out var modification))
                {
                    if (modification.tiles == null) return Enumerable.Empty<uint>();
                    return modification.tiles;
                }
                
                return (osmTiledDb.GetTiles(key.Type, key.Id)).Select(x => 
                    Tile.ToLocalId(x, osmTiledDb.Zoom)).ToList();
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
                    
                    Prepare(create, settings);
                    
                    // collect tiles per object.
                    var key = new OsmGeoKey(create);
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
                            var nodeTiles = GetTilesFor(key);
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
                            var memberTiles = GetTilesFor(key);
                            foreach (var nodeTile in memberTiles)
                            {
                                tileSet.Add(nodeTile);
                            }
                        }
                    }
                    
                    // save the tile mutations and affected tiles.
                    foreach (var tile in tileSet)
                    {
                        modifiedTiles.Add(tile);
                    }

                    // save the tiles, this key is new.
                    modifications[key] = (tileSet, create);
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
                    
                    // save the tile mutations and affected tiles.
                    var tileSet = GetTilesFor(key);
                    foreach (var tile in tileSet)
                    {
                        modifiedTiles.Add(tile);
                    }

                    modifications[key] = (null, null);
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
                    
                    Prepare(modify, settings);
                    
                    // collect tiles per object.
                    var key = new OsmGeoKey(modify);  
                    
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
                    var oldTiles = new HashSet<uint>((osmTiledDb.GetTiles(key.Type, key.Id))
                        .Select(x => Tile.ToLocalId(x, osmTiledDb.Zoom)));
                    foreach (var tile in oldTiles)
                    {
                        modifiedTiles.Add(tile);
                    }
                    
                    // add objects to tiles that need to have the modified data.
                    foreach (var tile in tileSet)
                    {
                        modifiedTiles.Add(tile);
                    }
                        
                    // save the tiles, the set has changed.
                    modifications[key] = (tileSet, modify);
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

            // loop over all tiles and their objects affected and apply the mutations.
            var buffer = new byte[1024];
            using var existingStream = osmTiledDb.Get(modifiedTiles
                .Select(x => Tile.FromLocalId(osmTiledDb.Zoom, x)).ToArray(), buffer)
                .Select<(OsmGeo osmGeo, IReadOnlyCollection<(uint x, uint y)> tiles), (IEnumerable<uint> tiles, OsmGeo osmGeo)>(x => ( 
                    x.tiles.Select(t => Tile.ToLocalId(t, osmTiledDb.Zoom)), x.osmGeo)).GetEnumerator();
            using var modifiedStream = modifications.GetEnumerator();
            var existingHasNext = existingStream.MoveNext();
            var modifiedHasNext = modifiedStream.MoveNext();

            while (existingHasNext || modifiedHasNext)
            {
                (IEnumerable<uint>? tiles, OsmGeo? osmGeo, OsmGeoKey key)? next = null;
                if (existingHasNext && modifiedHasNext)
                {
                    // compare and take first.
                    var existing = existingStream.Current;
                    var modified = modifiedStream.Current;
                    if (existing.osmGeo.Id == null) throw new InvalidDataException("Object found without an id.");
                    var existingId = OsmGeoCoder.Encode(modified.Key.Type, modified.Key.Id);
                    var modifiedId = OsmGeoCoder.Encode(existing.osmGeo.Type, existing.osmGeo.Id.Value);
                    if (existingId < modifiedId)
                    {
                        // move existing.
                        next = (existing.tiles.ToList(), existing.osmGeo, new OsmGeoKey(existing.osmGeo));
                        existingHasNext = existingStream.MoveNext();
                    }
                    else if (modifiedId < existingId)
                    {
                        // move modified.
                        next = (modified.Value.tiles?.ToList(), modified.Value.osmGeo, modified.Key);
                        modifiedHasNext = modifiedStream.MoveNext();
                    }
                    else
                    { // overwrite existing if equal.
                        // move modified.
                        next = (modified.Value.tiles?.ToList(), modified.Value.osmGeo, modified.Key);
                        modifiedHasNext = modifiedStream.MoveNext();
                        existingHasNext = modifiedStream.MoveNext();
                    }
                }
                else if (existingHasNext)
                {
                    // move existing.
                    var existing = existingStream.Current;
                    next = (existing.tiles.ToList(), existing.osmGeo, new OsmGeoKey(existing.osmGeo));
                    existingHasNext = existingStream.MoveNext();
                }
                else
                {
                    // move modified.
                    var modified = modifiedStream.Current;
                    next = (modified.Value.tiles?.ToList(), modified.Value.osmGeo, modified.Key);
                    modifiedHasNext = modifiedStream.MoveNext();
                }

                if (next == null) throw new InvalidDataException("Next object cannot be null.");
                if (next?.osmGeo == null || next?.tiles == null)
                { // this object was delete, add it as such to the index.
                    idIndex.Append(next.Value.key, -1);
                    continue;
                }
                
                // append to output.
                var tiles = next.Value.tiles.ToList();
                var location = tiledStream.Append(tiles, next.Value.osmGeo);
                idIndex.Append(new OsmGeoKey(next.Value.osmGeo), location);
            }
            
            // set empty tiles if any.
            foreach (var tile in modifiedTiles)
            {
                if (tiledStream.HasTile(tile)) continue;
                
                // tile was set as modified but it wasn't written to, it has to be empty.
                tiledStream.SetAsEmpty(tile);
            }

            // reverse indexed data and save tile index.
            tiledStream.SerializeIndex(dataTilesIndex);

            // save the meta-data.
            var dbMeta = new OsmTiledDbMeta
            {
                Base = osmTiledDb.Path, 
                Type = OsmTiledDbType.Diff,
                Zoom = zoom,
                Timestamp = timestamp
            };
            OsmTiledDbOperations.SaveDbMeta(path, dbMeta);
        }

        private static void Prepare(this OsmGeo osmGeo, OsmTiledDbDiffBuildSettings settings)
        {
            if (!settings.IncludeChangeset) osmGeo.ChangeSetId = null;
            if (!settings.IncludeUsername) osmGeo.UserName = null;
            if (!settings.IncludeUserId) osmGeo.UserId = null;
            if (!settings.IncludeVisible) osmGeo.Visible = null;
        }
    }
}