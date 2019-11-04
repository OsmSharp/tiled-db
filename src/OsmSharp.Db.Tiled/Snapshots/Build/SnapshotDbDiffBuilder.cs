using System;
using System.Collections.Generic;
using System.Linq;
using OsmSharp.Changesets;
using OsmSharp.Db.Tiled.Indexes;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.Snapshots.IO;
using OsmSharp.Db.Tiled.Tiles;
using OsmSharp.IO.Binary;
using OsmSharp.IO.PBF;
using Serilog;

namespace OsmSharp.Db.Tiled.Snapshots.Build
{
    /// <summary>
    /// Builds an OSM snapshot diff db from a previous snapshot db and an OSM change definition.
    /// </summary>
    internal static class SnapshotDbDiffBuilder
    {
        /// <summary>
        /// Builds a snapshot diff db.
        /// </summary>
        /// <param name="snapshotDb"></param>
        /// <param name="path"></param>
        /// <param name="changeset"></param>
        public static SnapshotDbDiff BuildDiff(this SnapshotDb snapshotDb, string path, OsmChange changeset)
        {
            // creates a new database diff representing the given changes.
            // create a target directory if one wasn't specified.
            if (string.IsNullOrWhiteSpace(path))
            {
                var epochs = DateTime.Now.ToUnixTime();
                path = FileSystemFacade.FileSystem.Combine(FileSystemFacade.FileSystem.ParentDirectory(path),
                    $"diff-{epochs}");
            }
            
            // make sure path exists.
            if (!FileSystemFacade.FileSystem.DirectoryExists(path))
            {
                FileSystemFacade.FileSystem.CreateDirectory(path);
            }

            var timestamp = snapshotDb.Timestamp;
            
            // execute the deletes.
            if (changeset.Delete != null &&
                changeset.Delete.Length > 0)
            {
                for (var d = 0; d < changeset.Delete.Length; d++)
                {
                    var delete = changeset.Delete[d];
                    if (delete.TimeStamp.HasValue && timestamp < delete.TimeStamp.Value)
                        timestamp = delete.TimeStamp.Value;
                    Delete(path, snapshotDb.Zoom, snapshotDb, delete.Type, delete.Id.Value);
                    if (changeset.Delete.Length > 1000 && d % 1000 == 0)
                    {
                        Log.Information($"Deleted {d+1}/{changeset.Delete.Length} objects.");
                    }
                }
                if (changeset.Delete.Length > 1000)
                {
                    Log.Information($"Deleted {changeset.Delete.Length}/{changeset.Delete.Length} objects.");
                }
            }
            
            // execute the creations.
            if (changeset.Create != null &&
                changeset.Create.Length > 0)
            {
                for (var c = 0; c < changeset.Create.Length; c++)
                {
                    var create = changeset.Create[c];
                    if (create.TimeStamp.HasValue && timestamp < create.TimeStamp.Value)
                        timestamp = create.TimeStamp.Value;
                    Create(path, snapshotDb.Zoom, snapshotDb, create);
                    if (changeset.Create.Length > 1000 && c % 1000 == 0)
                    {
                        Log.Information($"Created {c}/{changeset.Create.Length} objects.");
                    }
                }
                if (changeset.Create.Length > 1000)
                {
                    Log.Information($"Created {changeset.Create.Length}/{changeset.Create.Length} objects.");
                }
            }
            
            // execute the modifications.
            if (changeset.Modify != null &&
                changeset.Modify.Length > 0)
            {
                for (var m = 0; m < changeset.Modify.Length; m++)
                {
                    var modify = changeset.Modify[m];
                    if (modify.TimeStamp.HasValue && timestamp < modify.TimeStamp.Value)
                        timestamp = modify.TimeStamp.Value;
                    Modify(path, snapshotDb.Zoom, snapshotDb, changeset.Modify[m]);
                    if (changeset.Modify.Length > 1000 && m % 1000 == 0)
                    {
                        Log.Information($"Modified {m}/{changeset.Modify.Length} objects.");
                    }
                }
                if (changeset.Modify.Length > 1000)
                {
                    Log.Information($"Modified {changeset.Modify.Length}/{changeset.Modify.Length} objects.");
                }
            }
            
            // write meta data.
            var dbMeta = new SnapshotDbMeta()
            {
                Base = snapshotDb.Path,
                Type = SnapshotDbType.Diff,
                Zoom = snapshotDb.Zoom,
                Timestamp = timestamp
            };
            SnapshotDbOperations.SaveDbMeta(path, dbMeta);

            return new SnapshotDbDiff(path);
        }
        
        private static void Delete(string path, uint maxZoom, SnapshotDb snapshotDb, OsmGeoType type, long id)
        {
            //Log.Information($"Deleting: {type} - {id}");
            // TODO: deletions could cause another object to be removed from tiles.
            
            var tiles = GetTilesFor(path, maxZoom, snapshotDb, new[] {(type, id)});
            var dataTiles = tiles[tiles.Count - 1];

            // update the deleted index for each tile.
            foreach (var dataTile in dataTiles)
            {
                // load index.
                var deletedIndex = SnapshotDbOperations.LoadDeletedIndex(path, dataTile.tile, type) ?? new DeletedIndex();

                // add id.
                deletedIndex.Add(id);
                
                // save index.
                SnapshotDbOperations.SaveDeletedIndex(path, dataTile.tile, type, deletedIndex);
            }
        }
        
        private static void Create(string path, uint maxZoom, SnapshotDb snapshotDb, OsmGeo osmGeo)
        {
            //Log.Information($"Creating: {osmGeo}");
            // TODO: creating relations could cause loops in the relations and change tile membership.
            
            // determine the tile for this new object.
            IReadOnlyList<IEnumerable<(Tile tile, int mask)>> tiles = null;
            switch (osmGeo.Type)
            {
                case OsmGeoType.Node:
                    var node = osmGeo as Node;
                    tiles = GetTilesFor(path, maxZoom, node.Longitude.Value, node.Latitude.Value);
                    break;
                case OsmGeoType.Way:
                    var way = osmGeo as Way;
                    var nodes = new List<(OsmGeoType type, long id)>();
                    foreach (var n in way.Nodes)
                    {
                        nodes.Add((OsmGeoType.Node, n));
                    }
                    tiles = GetTilesFor(path, maxZoom, snapshotDb, nodes);
                    break;
                case OsmGeoType.Relation:
                    var relation = osmGeo as Relation;
                    var members = new List<(OsmGeoType type, long id)>();
                    foreach (var m in relation.Members)
                    {
                        members.Add((m.Type, m.Id));
                    }
                    tiles = GetTilesFor(path, maxZoom, snapshotDb, members);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
            
            // add the object to all the indexes/tiles.
            for (var l = 0; l < tiles.Count - 1; l++)
            {
                foreach (var (tile, mask) in tiles[l])
                {
                    var index = LoadIndex(path, tile, osmGeo.Type, true);
                    index.Add(osmGeo.Id.Value, mask);
                    SnapshotDbOperations.SaveIndex(path, tile, osmGeo.Type, index);
                }
            }
            
            // add the object to all the data tiles.
            foreach (var (tile, _) in tiles[tiles.Count - 1])
            {
                using (var stream = SnapshotDbOperations.OpenAppendStreamTile(path, osmGeo.Type, tile))
                {
                    switch (osmGeo.Type)
                    {
                        case OsmGeoType.Node:
                            stream.Append(osmGeo as Node);
                            break;
                        case OsmGeoType.Way:
                            stream.Append(osmGeo as Way);
                            break;
                        case OsmGeoType.Relation:
                            stream.Append(osmGeo as Relation);
                            break;
                    }
                }
            }
        }

        private static void Modify(string path, uint maxZoom, SnapshotDb snapshotDb, OsmGeo osmGeo)
        {
            // TODO: we are testing this, modification in our model is creation.
            Create(path, maxZoom, snapshotDb, osmGeo);
            return;
            
            //Log.Information($"Modifying: {osmGeo}");

            /*var recreate = new Dictionary<OsmGeoKey, OsmGeo>();
            
            if (osmGeo.Type == OsmGeoType.Node)
            {
                var oldTiles = GetTilesFor(path, maxZoom, snapshotDb, new[] {(osmGeo.Type, osmGeo.Id.Value)});

                if (!oldTiles[oldTiles.Count - 1].Any())
                { // no old tiles found, object didn't exist yet, this is possible with extracts.
                    //Log.Warning($"Modification converted into create: {osmGeo} not found");
                    Create(path, maxZoom, snapshotDb, osmGeo);
                    return;
                }

                var oldTile = oldTiles[oldTiles.Count - 1].First();
                var node = osmGeo as Node;
                var newTiles = GetTilesFor(path, maxZoom, node.Longitude.Value, node.Latitude.Value);
                var newTile = newTiles[newTiles.Count - 1].First();

                if (newTile.tile.LocalId != oldTile.tile.LocalId)
                { // node is NOT in the same tile, just recreate the node, it will be moved.
                    // trigger move of dependent objects.
                    foreach (var parent in GetParentsIn(path, maxZoom, snapshotDb, oldTile.tile, osmGeo.Type, osmGeo.Id.Value))
                    {
                        recreate[new OsmGeoKey(parent)] = parent;
                    }
                    
                    // recreate the node.
                    Create(path, maxZoom, snapshotDb, node);
                }
                else
                { // node is in the same tile, just add it to the local data tile, it will be used instead of base.
                    // add the node to the data tile.
                    using (var stream = SnapshotDbOperations.OpenAppendStreamTile(path, osmGeo.Type, newTile.tile))
                    {
                        stream.Append(osmGeo as Node);
                    }
                }
            }
            else if (osmGeo.Type == OsmGeoType.Way)
            {
                var way = osmGeo as Way;
                
                // build set of old tiles.
                var oldTiles = GetTilesFor(path, maxZoom, snapshotDb, new[] {(osmGeo.Type, osmGeo.Id.Value)});
                
                if (!oldTiles[oldTiles.Count - 1].Any())
                { // no old tiles found, object didn't exist yet, this is possible with extracts.
                    //Log.Warning($"Modification converted into create: {osmGeo} not found");
                    Create(path, maxZoom, snapshotDb, osmGeo);
                    return;
                }
                
                var oldTileSet = new HashSet<ulong>();
                foreach (var oldTile in oldTiles[oldTiles.Count - 1])
                {
                    oldTileSet.Add(oldTile.tile.LocalId);
                }
                
                // get new tiles and build set.
                var nodes = new List<(OsmGeoType type, long id)>();
                foreach (var n in way.Nodes)
                {
                    nodes.Add((OsmGeoType.Node, n));
                }
                var newTiles = GetTilesFor(path, maxZoom, snapshotDb, nodes);
                var newTileSet = new HashSet<ulong>();
                foreach (var newTile in newTiles[newTiles.Count - 1])
                {
                    newTileSet.Add(newTile.tile.LocalId);
                }
                
                // compare sets.
                if (!newTileSet.SetEquals(oldTileSet))
                { // way NOT in the same tiles
                    // trigger move of dependent objects.
                    foreach (var oldTile in oldTiles[oldTiles.Count - 1])
                    {
                        foreach (var parent in GetParentsIn(path, maxZoom, snapshotDb, oldTile.tile, osmGeo.Type, osmGeo.Id.Value))
                        {
                            recreate[new OsmGeoKey(parent)] = parent;
                        }
                    }
                    
                    // recreate the way.
                    Create(path, maxZoom, snapshotDb, way);
                }
                else
                { // way is exactly the same tiles, just write it to all of them.
                    foreach (var newTile in newTiles[newTiles.Count - 1])
                    {
                        // add the node to the data tile.
                        using (var stream = SnapshotDbOperations.OpenAppendStreamTile(path, osmGeo.Type, newTile.tile))
                        {
                            stream.Append(way);
                        }
                    }
                }
            }
            else
            {
                 var relation = osmGeo as Relation;
                
                // build set of old tiles.
                var oldTiles = GetTilesFor(path, maxZoom, snapshotDb, new[] {(osmGeo.Type, osmGeo.Id.Value)});
                
                if (!oldTiles[oldTiles.Count - 1].Any())
                { // no old tiles found, object didn't exist yet, this is possible with extracts.
                    //Log.Warning($"Modification converted into create: {osmGeo} not found");
                    Create(path, maxZoom, snapshotDb, osmGeo);
                    return;
                }
                
                var oldTileSet = new HashSet<ulong>();
                foreach (var oldTile in oldTiles[oldTiles.Count - 1])
                {
                    oldTileSet.Add(oldTile.tile.LocalId);
                }
                
                // get new tiles and build set.
                var members = new List<(OsmGeoType type, long id)>();
                foreach (var m in relation.Members)
                {
                    members.Add((m.Type, m.Id));
                }
                var newTiles = GetTilesFor(path, maxZoom, snapshotDb, members);
                var newTileSet = new HashSet<ulong>();
                foreach (var newTile in newTiles[newTiles.Count - 1])
                {
                    newTileSet.Add(newTile.tile.LocalId);
                }
                
                // compare sets.
                if (!newTileSet.SetEquals(oldTileSet))
                { // relation NOT in the same tiles
                    // trigger move of dependent objects.
                    foreach (var oldTile in oldTiles[oldTiles.Count - 1])
                    {
                        foreach (var parent in GetParentsIn(path, maxZoom, snapshotDb, oldTile.tile, osmGeo.Type, osmGeo.Id.Value))
                        {
                            recreate[new OsmGeoKey(parent)] = parent;
                        }
                    }
                    
                    // recreate the way.
                    Create(path, maxZoom, snapshotDb, relation);
                }
                else
                { // relation is exactly the same tiles, just write it to all of them.
                    foreach (var newTile in newTiles[newTiles.Count - 1])
                    {
                        // add the node to the data tile.
                        using (var stream = SnapshotDbOperations.OpenAppendStreamTile(path, osmGeo.Type, newTile.tile))
                        {
                            stream.Append(relation);
                        }
                    }
                }
            }

            // recreate all objects that could have been moved.
            foreach (var create in recreate.Values)
            {
                Create(path, maxZoom, snapshotDb, create);
            }*/
        }
        
        private static IReadOnlyList<IEnumerable<(Tile tile, int mask)>> GetTilesFor(string path, uint maxZoom, double longitude, double latitude)
        {
            var tilesPerZoom = new List<IEnumerable<(Tile tile, int mask)>>();
            var tile = new Tile(0, 0, 0);
            uint zoom = 0;
            while (zoom < maxZoom)
            {
                // determine next tile.
                var nextTile = Tiles.Tile.WorldToTileIndex(latitude, longitude, zoom + 2);
                
                // build mask and add previous tile.
                var mask = nextTile.BuildMask2();
                tilesPerZoom.Add(new[] {(tile, mask)});
                
                // move to next tile.
                tile = nextTile;
                zoom += 2;
            }
            
            // add the last tile with a don't-care mask.
            tilesPerZoom.Add(new [] { (Tile.WorldToTileIndex(latitude, longitude, maxZoom), -1) });

            return tilesPerZoom;
        }
        
        private static IReadOnlyList<IEnumerable<(Tile tile, int mask)>> GetTilesFor(string path, uint maxZoom,
            SnapshotDb snapshotDb, IEnumerable<(OsmGeoType type, long id)> objects)
        {
            var tilesPerZoom = new List<IEnumerable<(Tile tile, int mask)>>();
            
            // do zoom level '0'.
            var tile = new Tile(0, 0, 0);
            var mask = 0;
            Index nodeIndex = null;
            Index wayIndex = null;
            Index relationIndex = null;
            foreach (var (type, id) in objects)
            {
                switch (type)
                {
                    case OsmGeoType.Node:
                        if (nodeIndex == null) nodeIndex = LoadIndex(path, tile, type);
                        if (nodeIndex != null &&
                            nodeIndex.TryGetMask(id, out var nodeMask))
                        {
                            mask |= nodeMask;
                        }

                        break;
                    case OsmGeoType.Way:
                        if (wayIndex == null) wayIndex = LoadIndex(path, tile, type);
                        if (wayIndex != null &&
                            wayIndex.TryGetMask(id, out var wayMask))
                        {
                            mask |= wayMask;
                        }

                        break;
                    case OsmGeoType.Relation:
                        if (relationIndex == null) relationIndex = LoadIndex(path, tile, type);
                        if (relationIndex != null &&
                            relationIndex.TryGetMask(id, out var relationMask))
                        {
                            mask |= relationMask;
                        }

                        break;
                }
            }

            if (mask == 0)
            { // object doesn't exist here, try base.
                return snapshotDb.GetTilesFor(objects);
            }
            
            // add first tile.
            tilesPerZoom.Add(new []{ (tile, mask)});

            // split tiles per level.
            uint zoom = 0;
            while (zoom < maxZoom)
            {
                // move one level down and collect all tiles and masks.
                var tilesAtZoom = new List<(Tile tile, int mask)>();
                foreach (var (tileAbove, maskAbove) in tilesPerZoom[tilesPerZoom.Count - 1])
                { // go over all tiles in zoom.
                    foreach (var currentTile in tileAbove.SubTilesForMask2(maskAbove))
                    { // go over all tiles that have at least one object at zoom+2.
                        
                        // determine mask for the tile above over all objects.
                        mask = 0;
                        nodeIndex = null;
                        wayIndex = null;
                        relationIndex = null;
                        foreach (var (type, id) in objects)
                        {
                            switch (type)
                            {
                                case OsmGeoType.Node:
                                    if (nodeIndex == null) nodeIndex = LoadIndex(path, currentTile, type);
                                    if (nodeIndex != null &&
                                        nodeIndex.TryGetMask(id, out var nodeMask))
                                    {
                                        mask |= nodeMask;
                                    }
                                    break;
                                case OsmGeoType.Way:
                                    if (wayIndex == null) wayIndex = LoadIndex(path, currentTile, type);
                                    if (wayIndex != null &&
                                        wayIndex.TryGetMask(id, out var wayMask))
                                    {
                                        mask |= wayMask;
                                    }
                                    break;
                                case OsmGeoType.Relation:
                                    if (relationIndex == null) relationIndex = LoadIndex(path, currentTile, type);
                                    if (relationIndex != null &&
                                        relationIndex.TryGetMask(id, out var relationMask))
                                    {
                                        mask |= relationMask;
                                    }
                                    break;
                            }
                        }
                        
                        // log the current tile and its mask.
                        tilesAtZoom.Add((currentTile, mask));
                    }
                }
                
                // keep what was collected.
                tilesPerZoom.Add(tilesAtZoom);
                
                // move to the next level.
                zoom += 2;
            }

            return tilesPerZoom;
        }
        
        private static Index LoadIndex(string path, Tile tile, OsmGeoType type, bool create = false)
        {
            if (type == OsmGeoType.Node)
            {
                var index = SnapshotDbOperations.LoadIndex(path, tile, type);
                if (create && index == null)
                {
                    index = new Index();
                }
                return index;
            }
            else
            {
                var index = SnapshotDbOperations.LoadIndex(path, tile, type);
                if (create && index == null)
                {
                    index = new Index();
                }
                return index;
            }
        }  
        
        private static IEnumerable<OsmGeo> GetParentsIn(string path, uint maxZoom, SnapshotDb snapshotDb, Tile tile, OsmGeoType type, long id)
        {
            if (type == OsmGeoType.Node)
            { // check ways only.
                foreach (var osmGeo in GetTile(path, maxZoom, snapshotDb, tile, OsmGeoType.Way))
                {
                    var way = osmGeo as Way;
                    if (way?.Nodes != null && way.Nodes.Contains(id))
                    {
                        yield return osmGeo;
                    }
                }
            }
            else
            {
                foreach (var osmGeo in GetTile(path, maxZoom, snapshotDb, tile, OsmGeoType.Relation))
                {
                    var relation = osmGeo as Relation;
                    if (relation?.Members != null && relation.Members.Any(m => m.Id == id && m.Type == type))
                    {
                        yield return osmGeo;
                    }
                }
            }
        }    
        
        private static IEnumerable<OsmGeo> GetTile(string path, uint maxZoom, SnapshotDb snapshotDb, Tile tile, OsmGeoType type)
        {
            if (tile.Zoom != maxZoom) throw new ArgumentException("Tile doesn't have the correct zoom level.");
            
            // get the deleted index if any.
            var deletedIndex = SnapshotDbOperations.LoadDeletedIndex(path, tile, type);

            // enumerate all data in tile.
            using (var localData = SnapshotDbOperations.GetLocalTile(path, maxZoom, tile, type).GetEnumerator())
            using (var baseData = snapshotDb.GetTile(tile, type).GetEnumerator())
            {
                var localHasData = localData.MoveNext();
                var baseHasData = baseData.MoveNext();

                while (localHasData || baseHasData)
                {
                    if (localHasData && baseHasData)
                    {
                        var c = localData.Current.CompareByIdAndType(baseData.Current);
                        if (c == 0)
                        { // equal in id and type, skip over the one from base and don't do a thing.
                            baseHasData = baseData.MoveNext();
                        }
                        else if (c < 0)
                        { // the local is earliest.
                            yield return  localData.Current;
                            localHasData = localData.MoveNext();
                        }
                        else
                        { // the base is earliest.
                            if (deletedIndex == null ||
                                !deletedIndex.Contains(baseData.Current.Id.Value))
                            { // in base and not deleted, return it.
                                yield return baseData.Current;
                            }
                            baseHasData = baseData.MoveNext();
                        }
                    }
                    else if (localHasData)
                    {
                        yield return localData.Current;
                        localHasData = localData.MoveNext();
                    }
                    else
                    {
                        if (deletedIndex == null ||
                            !deletedIndex.Contains(baseData.Current.Id.Value))
                        { // in base and not deleted.
                            yield return baseData.Current;
                        }
                        baseHasData = baseData.MoveNext();
                    }
                }
            }
        }
    }
}