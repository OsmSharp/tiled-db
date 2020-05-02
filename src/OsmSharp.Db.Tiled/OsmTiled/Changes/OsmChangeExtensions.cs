using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using OsmSharp.Changesets;
using OsmSharp.Db.Tiled.Logging;
using OsmSharp.Db.Tiled.Tiles;

namespace OsmSharp.Db.Tiled.OsmTiled.Changes
{
    internal static class OsmChangeExtensions
    {
        public static (DateTime timestamp, IEnumerable<uint> tiles, IEnumerable<(OsmGeoKey key, IEnumerable<uint>? tiles, OsmGeo? osmGeo)> osmGeo)
            BuildTiledStream(this OsmChange changeset,
                uint zoom, Func<OsmGeoKey, IEnumerable<uint>> getTiles)
        {
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
                
                return getTiles(key).ToList();
            }
            
            // NOTE: the changeset is expected to 'squashed' already and in order (nodes, ways and relations sorted).
            // it's expected to be minimal; deleting an object that has been create in the same changeset will no be considered.
            
            var timestamp = DateTime.MinValue;
            
            // process all deletions.
            if (changeset.Delete != null)
            {
                var progress = Log.Default.ProgressRelative(getMessage: (p) => $"Deleting {p}%");
                for (var i = 0; i < changeset.Delete.Length; i++)
                {
                    progress.Progress(i, changeset.Delete.Length);
                    
                    var deleted = changeset.Delete[i];
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
                progress.Done();
            }

            // first process all node modification/creations.
            if (changeset.Create != null)
            {
                var progress = Log.Default.ProgressRelative(getMessage: (p) => $"Creating nodes {p}%");
                for (var i = 0; i < changeset.Create.Length; i++)
                {
                    progress.Progress(i, changeset.Create.Length);
                    
                    var create = changeset.Create[i];
                    if (create == null) continue;
                
                    // update timestamp.
                    if (create.TimeStamp.HasValue &&
                        create.TimeStamp > timestamp)
                    {
                        timestamp = create.TimeStamp.Value;
                    }
                    
                    // collect tiles per object.
                    var key = new OsmGeoKey(create);
                    var tileSet = new HashSet<uint>();
                    
                    if (!(create is Node newNode)) break;
                    
                    if (newNode.Latitude == null || newNode.Longitude == null) throw new InvalidDataException("Cannot store node without a valid location.");
                        
                    var tile = Tile.FromWorld(newNode.Longitude.Value, newNode.Latitude.Value, zoom);
                    var localId = Tile.ToLocalId(tile.x, tile.y, zoom);
                    tileSet.Add(localId);

                    // save the tile mutations and affected tiles.
                    modifiedTiles.Add(localId);

                    // save the tiles, this key is new.
                    modifications[key] = (tileSet, create);
                }
                progress.Done();
            }
            if (changeset.Modify != null)
            {
                var progress = Log.Default.ProgressRelative(getMessage: (p) => $"Modifying nodes {p}%");
                for (var i = 0; i < changeset.Modify.Length; i++)
                {
                    progress.Progress(i, changeset.Modify.Length);

                    var modify = changeset.Modify[i];
                    if (modify == null) continue;
                
                    // update timestamp.
                    if (modify.TimeStamp.HasValue &&
                        modify.TimeStamp > timestamp)
                    {
                        timestamp = modify.TimeStamp.Value;
                    }
                    
                    // collect tiles per object.
                    var key = new OsmGeoKey(modify);  
                    
                    var tileSet = new HashSet<uint>();
                    if (!(modify is Node newNode)) break;
                    
                    if (newNode.Latitude == null || newNode.Longitude == null) throw new InvalidDataException("Cannot store node without a valid location.");
                        
                    var tile = Tile.FromWorld(newNode.Longitude.Value, newNode.Latitude.Value, zoom);
                    var localId = Tile.ToLocalId(tile.x, tile.y, zoom);

                    tileSet.Add(localId);
                    
                    // remove objects that have moved between tiles.
                    var oldTiles = new HashSet<uint>(getTiles(key));
                    foreach (var oldTile in oldTiles)
                    {
                        modifiedTiles.Add(oldTile);
                    }
                    
                    // add objects to tiles that need to have the modified data.
                    modifiedTiles.Add(localId);
                        
                    // save the tiles, the set has changed.
                    modifications[key] = (tileSet, modify);
                }
                progress.Done();
            }
            
            // process all the rest.
            if (changeset.Create != null)
            {
                var progress = Log.Default.ProgressRelative(getMessage: (p) => $"Creating ways & relations {p}%");
                for (var i = 0; i < changeset.Create.Length; i++)
                {
                    progress.Progress(i, changeset.Create.Length);
                    
                    var create = changeset.Create[i];
                    if (create == null) continue;
                
                    // update timestamp.
                    if (create.TimeStamp.HasValue &&
                        create.TimeStamp > timestamp)
                    {
                        timestamp = create.TimeStamp.Value;
                    }
                    
                    // collect tiles per object.
                    var key = new OsmGeoKey(create);
                    var tileSet = new HashSet<uint>();
                    switch (create)
                    {
                        case Node newNode:
                            continue;
                        case Way way:
                        {
                            foreach (var n in way.Nodes)
                            {
                                var nodeTiles = GetTilesFor(new OsmGeoKey(OsmGeoType.Node, n));
                                foreach (var nodeTile in nodeTiles)
                                {
                                    tileSet.Add(nodeTile);
                                }
                            }

                            break;
                        }
                        case Relation relation:
                        {
                            foreach (var m in relation.Members)
                            {
                                var memberTiles = GetTilesFor(new OsmGeoKey(m.Type, m.Id));
                                foreach (var nodeTile in memberTiles)
                                {
                                    tileSet.Add(nodeTile);
                                }
                            }

                            break;
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
                progress.Done();
            }
            if (changeset.Modify != null)
            {
                var progress = Log.Default.ProgressRelative(getMessage: (p) => $"Modifying ways and relations {p}%");
                for (var i = 0; i < changeset.Modify.Length; i++)
                {
                    progress.Progress(i, changeset.Modify.Length);

                    var modify = changeset.Modify[i];
                    if (modify == null) continue;
                
                    // update timestamp.
                    if (modify.TimeStamp.HasValue &&
                        modify.TimeStamp > timestamp)
                    {
                        timestamp = modify.TimeStamp.Value;
                    }
                    
                    // collect tiles per object.
                    var key = new OsmGeoKey(modify);  
                    
                    var tileSet = new HashSet<uint>();
                    switch (modify)
                    {
                        case Node newNode:
                            continue;
                        case Way way:
                        {
                            foreach (var n in way.Nodes)
                            {
                                var nodeTiles = GetTilesFor(new OsmGeoKey(OsmGeoType.Node, n));
                                foreach (var nodeTile in nodeTiles)
                                {
                                    tileSet.Add(nodeTile);
                                }
                            }

                            break;
                        }
                        case Relation relation:
                        {
                            foreach (var m in relation.Members)
                            {
                                var memberTiles = GetTilesFor(new OsmGeoKey(m.Type, m.Id));
                                foreach (var nodeTile in memberTiles)
                                {
                                    tileSet.Add(nodeTile);
                                }
                            }

                            break;
                        }
                    }
                    
                    // remove objects that have moved between tiles.
                    var oldTiles = new HashSet<uint>(getTiles(key));
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
                progress.Done();
            }

            return (timestamp, modifiedTiles, modifications.Select(x => (x.Key, x.Value.tiles, x.Value.osmGeo)));
        }
    }
}