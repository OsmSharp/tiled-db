using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using OsmSharp.Changesets;
using OsmSharp.Db.Tiled.Indexes;
using OsmSharp.Db.Tiled.Indexes.InMemory;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.Snapshots.IO;
using OsmSharp.Db.Tiled.Tiles;
using OsmSharp.IO.Binary;
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
        /// <param name="snapshotDb">The snapshot db.</param>
        /// <param name="changeset">The changeset.</param>
        /// <param name="changesetTimestamp">The changeset timestamp, override the timestamp generated by the data.</param>
        /// <param name="path">The path to store the db at, if not given one will be generated at the same level as the given db.</param>
        public static SnapshotDbDiff BuildDiff(this SnapshotDb snapshotDb, OsmChange changeset, DateTime? changesetTimestamp = null, string path = null)
        {
            // creates a new database diff representing the given changes.
            // create a target directory if one wasn't specified.
            if (string.IsNullOrWhiteSpace(path))
            {
                path = FileSystemFacade.FileSystem.Combine(FileSystemFacade.FileSystem.ParentDirectory(snapshotDb.Path),
                    $"diff-{DateTime.Now.ToTimestampPath()}");
            }
            
            // make sure path exists.
            if (!FileSystemFacade.FileSystem.DirectoryExists(path))
            {
                FileSystemFacade.FileSystem.CreateDirectory(path);
            }

            var timestamp = snapshotDb.Timestamp;
            var indexCache = new MemoryIndexCache();
            var tileStreams = new Dictionary<(ulong, OsmGeoType), Stream>();
            
            // execute the deletes.
            if (changeset.Delete != null &&
                changeset.Delete.Length > 0)
            {
                for (var d = 0; d < changeset.Delete.Length; d++)
                {
                    var delete = changeset.Delete[d];
                    if (delete.TimeStamp.HasValue && timestamp < delete.TimeStamp.Value)
                        timestamp = delete.TimeStamp.Value;
                    Delete(indexCache, path, snapshotDb.Zoom, snapshotDb, delete.Type, delete.Id.Value);
                    if (changeset.Delete.Length > 1000 && d % 1000 == 0)
                    {
                        Log.Verbose($"Deleted {d+1}/{changeset.Delete.Length} objects.");
                    }
                }
            }
            
            // execute the creations.
            var progress = 5000;
            if (changeset.Create != null &&
                changeset.Create.Length > 0)
            {
                // update timestamp.
                for (var c = 0; c < changeset.Create.Length; c++)
                {
                    var create = changeset.Create[c];
                    if (create.TimeStamp.HasValue && timestamp < create.TimeStamp.Value)
                        timestamp = create.TimeStamp.Value;
                }

                var last = 0;
                for (var c = 0; c < changeset.Create.Length;)
                {
                    var count = Create(indexCache, tileStreams, path, snapshotDb.Zoom, snapshotDb, changeset.Create, c);
                    c += count;
                    if (c - last > progress)
                    {
                        Log.Verbose($"Created {c}/{changeset.Create.Length} objects.");
                        last = c;
                    }
                }
            }

            // execute the creations.
            if (changeset.Modify != null &&
                changeset.Modify.Length > 0)
            {
                // update timestamp.
                for (var c = 0; c < changeset.Modify.Length; c++)
                {
                    var modification = changeset.Modify[c];
                    if (modification.TimeStamp.HasValue && timestamp < modification.TimeStamp.Value)
                        timestamp = modification.TimeStamp.Value;
                }

                var last = 0;
                for (var c = 0; c < changeset.Modify.Length; )
                {
                    var count = Create(indexCache, tileStreams, path, snapshotDb.Zoom, snapshotDb, changeset.Modify, c);
                    c += count;
                    if (c - last > progress)
                    {
                        Log.Verbose($"Modified {c}/{changeset.Modify.Length} objects.");
                        last = c;
                    }
                }
            }
            
            // save all indexes.
            foreach (var index in indexCache.GetAll())
            {
                if (index.index == null) continue;
                
                SnapshotDbOperations.SaveIndex(path, index.tile, index.type, index.index.ToIndex());
            }
            
            foreach (var tileStream in tileStreams.Values)
            {
                tileStream.Flush();
                tileStream.Dispose();
            }
            
            // use the given timestamp if any.
            if (changesetTimestamp.HasValue)
            {
                timestamp = changesetTimestamp.Value;
            }
            
            // write meta data.
            var dbMeta = new SnapshotDbMeta
            {
                Base = snapshotDb.Path,
                Type = SnapshotDbType.Diff,
                Zoom = snapshotDb.Zoom,
                Timestamp = timestamp
            };
            SnapshotDbOperations.SaveDbMeta(path, dbMeta);

            return new SnapshotDbDiff(path);
        }
        
        private static void Delete(MemoryIndexCache indexCache, string path, uint maxZoom, SnapshotDb snapshotDb, OsmGeoType type, long id)
        {
            // TODO: deletions could cause another object to be removed from tiles.
            
            var tiles = GetTilesFor(indexCache, path, maxZoom, snapshotDb, new[] {(type, id)});
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

        private static int Create(MemoryIndexCache indexCache, IDictionary<(ulong, OsmGeoType), Stream> tileStreams, string path, uint maxZoom, SnapshotDb snapshotDb, OsmGeo[] osmGeos, int i)
        {
            var osmGeo = osmGeos[i];
            if (osmGeo.Type == OsmGeoType.Node)
            {
                // attempt to create multiple nodes at a time.
                var node = osmGeo as Node;
                var tile = Tile.WorldToTileIndex(node.Latitude.Value, node.Longitude.Value, maxZoom);
                var nodes = new List<Node> {node};
                var start = i;
                while (true)
                {
                    i++;
                    if (i >= osmGeos.Length) break;
                    osmGeo = osmGeos[i];

                    if (osmGeo.Type != OsmGeoType.Node) break;
                    node = osmGeo as Node;
                    
                    var nextTile = Tile.WorldToTileIndex(node.Latitude.Value, node.Longitude.Value, maxZoom);
                    if (nextTile.LocalId != tile.LocalId) break;

                    nodes.Add(node);
                }

                // update all at once.
                Create(indexCache, tileStreams, path, maxZoom, snapshotDb, nodes);

                return i - start;
            }

            // create one at a time.
            // TODO: figure out if we can also update this.
            Create(indexCache, tileStreams, path, maxZoom, snapshotDb, osmGeo);
            return 1;
        }

        private static void Create(MemoryIndexCache indexCache, IDictionary<(ulong, OsmGeoType), Stream> tileStreams, string path, uint maxZoom, SnapshotDb snapshotDb, List<Node> nodes)
        {
            // determine the tile for this new object.
            var tiles = GetTilesFor(indexCache, path, maxZoom, nodes[0].Longitude.Value, nodes[0].Latitude.Value);
            
            // add the object to all the indexes/tiles.
            for (var l = 0; l < tiles.Count - 1; l++)
            {
                foreach (var (tile, mask) in tiles[l])
                {
                    var index = LoadIndex(indexCache, path, tile, OsmGeoType.Node, true);
                    foreach (var node in nodes)
                    {
                        index.Add(node.Id.Value, mask);
                    }
                }
            }
            
            // add the object to all the data tiles.
            foreach (var (tile, _) in tiles[tiles.Count - 1])
            {
                if (!tileStreams.TryGetValue((tile.LocalId, OsmGeoType.Node), out var stream))
                {
                    stream = SnapshotDbOperations.CreateTile(path, OsmGeoType.Node, tile);
                    tileStreams[(tile.LocalId, OsmGeoType.Node)] = stream;
                }

                foreach (var node in nodes)
                {
                    stream.Append(node);
                }
            }
        }

        private static void Create(MemoryIndexCache indexCache, IDictionary<(ulong, OsmGeoType), Stream> tileStreams, string path, uint maxZoom, SnapshotDb snapshotDb, OsmGeo osmGeo)
        {
            // TODO: creating relations could cause loops in the relations and change tile membership.
            
            // determine the tile for this new object.
            IReadOnlyList<IEnumerable<(Tile tile, int mask)>> tiles = null;
            switch (osmGeo.Type)
            {
                case OsmGeoType.Node:
                    var node = osmGeo as Node;
                    tiles = GetTilesFor(indexCache, path, maxZoom, node.Longitude.Value, node.Latitude.Value);
                    break;
                case OsmGeoType.Way:
                    var way = osmGeo as Way;
                    var nodes = new List<(OsmGeoType type, long id)>();
                    foreach (var n in way.Nodes)
                    {
                        nodes.Add((OsmGeoType.Node, n));
                    }
                    tiles = GetTilesFor(indexCache, path, maxZoom, snapshotDb, nodes);
                    break;
                case OsmGeoType.Relation:
                    var relation = osmGeo as Relation;
                    var members = new List<(OsmGeoType type, long id)>();
                    foreach (var m in relation.Members)
                    {
                        members.Add((m.Type, m.Id));
                    }
                    tiles = GetTilesFor(indexCache, path, maxZoom, snapshotDb, members);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
            
            // add the object to all the indexes/tiles.
            for (var l = 0; l < tiles.Count - 1; l++)
            {
                foreach (var (tile, mask) in tiles[l])
                {
                    var index = LoadIndex(indexCache, path, tile, osmGeo.Type, true);
                    index.Add(osmGeo.Id.Value, mask);
                    //SnapshotDbOperations.SaveIndex(path, tile, osmGeo.Type, index);
                }
            }
            
            // add the object to all the data tiles.
            foreach (var (tile, _) in tiles[tiles.Count - 1])
            {
                if (!tileStreams.TryGetValue((tile.LocalId, osmGeo.Type), out var stream))
                {
                    stream = SnapshotDbOperations.CreateTile(path, osmGeo.Type, tile);
                    tileStreams[(tile.LocalId, osmGeo.Type)] = stream;
                }

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

//        private static void Modify(string path, uint maxZoom, SnapshotDb snapshotDb, OsmGeo osmGeo)
//        {
//            // TODO: we are testing this, modification in our model is creation.
//            Create(path, maxZoom, snapshotDb, osmGeo);
//            return;
//            
//            //Log.Information($"Modifying: {osmGeo}");
//
//            /*var recreate = new Dictionary<OsmGeoKey, OsmGeo>();
//            
//            if (osmGeo.Type == OsmGeoType.Node)
//            {
//                var oldTiles = GetTilesFor(path, maxZoom, snapshotDb, new[] {(osmGeo.Type, osmGeo.Id.Value)});
//
//                if (!oldTiles[oldTiles.Count - 1].Any())
//                { // no old tiles found, object didn't exist yet, this is possible with extracts.
//                    //Log.Warning($"Modification converted into create: {osmGeo} not found");
//                    Create(path, maxZoom, snapshotDb, osmGeo);
//                    return;
//                }
//
//                var oldTile = oldTiles[oldTiles.Count - 1].First();
//                var node = osmGeo as Node;
//                var newTiles = GetTilesFor(path, maxZoom, node.Longitude.Value, node.Latitude.Value);
//                var newTile = newTiles[newTiles.Count - 1].First();
//
//                if (newTile.tile.LocalId != oldTile.tile.LocalId)
//                { // node is NOT in the same tile, just recreate the node, it will be moved.
//                    // trigger move of dependent objects.
//                    foreach (var parent in GetParentsIn(path, maxZoom, snapshotDb, oldTile.tile, osmGeo.Type, osmGeo.Id.Value))
//                    {
//                        recreate[new OsmGeoKey(parent)] = parent;
//                    }
//                    
//                    // recreate the node.
//                    Create(path, maxZoom, snapshotDb, node);
//                }
//                else
//                { // node is in the same tile, just add it to the local data tile, it will be used instead of base.
//                    // add the node to the data tile.
//                    using (var stream = SnapshotDbOperations.OpenAppendStreamTile(path, osmGeo.Type, newTile.tile))
//                    {
//                        stream.Append(osmGeo as Node);
//                    }
//                }
//            }
//            else if (osmGeo.Type == OsmGeoType.Way)
//            {
//                var way = osmGeo as Way;
//                
//                // build set of old tiles.
//                var oldTiles = GetTilesFor(path, maxZoom, snapshotDb, new[] {(osmGeo.Type, osmGeo.Id.Value)});
//                
//                if (!oldTiles[oldTiles.Count - 1].Any())
//                { // no old tiles found, object didn't exist yet, this is possible with extracts.
//                    //Log.Warning($"Modification converted into create: {osmGeo} not found");
//                    Create(path, maxZoom, snapshotDb, osmGeo);
//                    return;
//                }
//                
//                var oldTileSet = new HashSet<ulong>();
//                foreach (var oldTile in oldTiles[oldTiles.Count - 1])
//                {
//                    oldTileSet.Add(oldTile.tile.LocalId);
//                }
//                
//                // get new tiles and build set.
//                var nodes = new List<(OsmGeoType type, long id)>();
//                foreach (var n in way.Nodes)
//                {
//                    nodes.Add((OsmGeoType.Node, n));
//                }
//                var newTiles = GetTilesFor(path, maxZoom, snapshotDb, nodes);
//                var newTileSet = new HashSet<ulong>();
//                foreach (var newTile in newTiles[newTiles.Count - 1])
//                {
//                    newTileSet.Add(newTile.tile.LocalId);
//                }
//                
//                // compare sets.
//                if (!newTileSet.SetEquals(oldTileSet))
//                { // way NOT in the same tiles
//                    // trigger move of dependent objects.
//                    foreach (var oldTile in oldTiles[oldTiles.Count - 1])
//                    {
//                        foreach (var parent in GetParentsIn(path, maxZoom, snapshotDb, oldTile.tile, osmGeo.Type, osmGeo.Id.Value))
//                        {
//                            recreate[new OsmGeoKey(parent)] = parent;
//                        }
//                    }
//                    
//                    // recreate the way.
//                    Create(path, maxZoom, snapshotDb, way);
//                }
//                else
//                { // way is exactly the same tiles, just write it to all of them.
//                    foreach (var newTile in newTiles[newTiles.Count - 1])
//                    {
//                        // add the node to the data tile.
//                        using (var stream = SnapshotDbOperations.OpenAppendStreamTile(path, osmGeo.Type, newTile.tile))
//                        {
//                            stream.Append(way);
//                        }
//                    }
//                }
//            }
//            else
//            {
//                 var relation = osmGeo as Relation;
//                
//                // build set of old tiles.
//                var oldTiles = GetTilesFor(path, maxZoom, snapshotDb, new[] {(osmGeo.Type, osmGeo.Id.Value)});
//                
//                if (!oldTiles[oldTiles.Count - 1].Any())
//                { // no old tiles found, object didn't exist yet, this is possible with extracts.
//                    //Log.Warning($"Modification converted into create: {osmGeo} not found");
//                    Create(path, maxZoom, snapshotDb, osmGeo);
//                    return;
//                }
//                
//                var oldTileSet = new HashSet<ulong>();
//                foreach (var oldTile in oldTiles[oldTiles.Count - 1])
//                {
//                    oldTileSet.Add(oldTile.tile.LocalId);
//                }
//                
//                // get new tiles and build set.
//                var members = new List<(OsmGeoType type, long id)>();
//                foreach (var m in relation.Members)
//                {
//                    members.Add((m.Type, m.Id));
//                }
//                var newTiles = GetTilesFor(path, maxZoom, snapshotDb, members);
//                var newTileSet = new HashSet<ulong>();
//                foreach (var newTile in newTiles[newTiles.Count - 1])
//                {
//                    newTileSet.Add(newTile.tile.LocalId);
//                }
//                
//                // compare sets.
//                if (!newTileSet.SetEquals(oldTileSet))
//                { // relation NOT in the same tiles
//                    // trigger move of dependent objects.
//                    foreach (var oldTile in oldTiles[oldTiles.Count - 1])
//                    {
//                        foreach (var parent in GetParentsIn(path, maxZoom, snapshotDb, oldTile.tile, osmGeo.Type, osmGeo.Id.Value))
//                        {
//                            recreate[new OsmGeoKey(parent)] = parent;
//                        }
//                    }
//                    
//                    // recreate the way.
//                    Create(path, maxZoom, snapshotDb, relation);
//                }
//                else
//                { // relation is exactly the same tiles, just write it to all of them.
//                    foreach (var newTile in newTiles[newTiles.Count - 1])
//                    {
//                        // add the node to the data tile.
//                        using (var stream = SnapshotDbOperations.OpenAppendStreamTile(path, osmGeo.Type, newTile.tile))
//                        {
//                            stream.Append(relation);
//                        }
//                    }
//                }
//            }
//
//            // recreate all objects that could have been moved.
//            foreach (var create in recreate.Values)
//            {
//                Create(path, maxZoom, snapshotDb, create);
//            }*/
//        }
        
        private static IReadOnlyList<IEnumerable<(Tile tile, int mask)>> GetTilesFor(MemoryIndexCache indexCache, string path, uint maxZoom, double longitude, double latitude)
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
        
        private static IReadOnlyList<IEnumerable<(Tile tile, int mask)>> GetTilesFor(MemoryIndexCache indexCache, string path, uint maxZoom,
            SnapshotDb snapshotDb, IEnumerable<(OsmGeoType type, long id)> objects)
        {
            var tilesPerZoom = new List<IEnumerable<(Tile tile, int mask)>>();
            
            // do zoom level '0'.
            var tile = new Tile(0, 0, 0);
            var mask = 0;
            MemoryIndex nodeIndex = null;
            MemoryIndex wayIndex = null;
            MemoryIndex relationIndex = null;
            foreach (var (type, id) in objects)
            {
                switch (type)
                {
                    case OsmGeoType.Node:
                        if (nodeIndex == null) nodeIndex = LoadIndex(indexCache, path, tile, type);
                        if (nodeIndex != null &&
                            nodeIndex.TryGetMask(id, out var nodeMask))
                        {
                            mask |= nodeMask;
                        }

                        break;
                    case OsmGeoType.Way:
                        if (wayIndex == null) wayIndex = LoadIndex(indexCache, path, tile, type);
                        if (wayIndex != null &&
                            wayIndex.TryGetMask(id, out var wayMask))
                        {
                            mask |= wayMask;
                        }

                        break;
                    case OsmGeoType.Relation:
                        if (relationIndex == null) relationIndex = LoadIndex(indexCache, path, tile, type);
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
                                    if (nodeIndex == null) nodeIndex = LoadIndex(indexCache, path, currentTile, type);
                                    if (nodeIndex != null &&
                                        nodeIndex.TryGetMask(id, out var nodeMask))
                                    {
                                        mask |= nodeMask;
                                    }
                                    break;
                                case OsmGeoType.Way:
                                    if (wayIndex == null) wayIndex = LoadIndex(indexCache, path, currentTile, type);
                                    if (wayIndex != null &&
                                        wayIndex.TryGetMask(id, out var wayMask))
                                    {
                                        mask |= wayMask;
                                    }
                                    break;
                                case OsmGeoType.Relation:
                                    if (relationIndex == null) relationIndex = LoadIndex(indexCache, path, currentTile, type);
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

        private static MemoryIndex LoadIndex(MemoryIndexCache indexCache, string path, Tile tile, OsmGeoType type,
            bool create = false)
        {
            if (indexCache.TryGet(tile, type, out var index))
            {
                if (index != null) return index;
                if (!create) return null;
            }

            var loadedIndex = SnapshotDbOperations.LoadIndex(path, tile, type);
            if (loadedIndex != null)
            {
                index = MemoryIndex.FromIndex(loadedIndex);
            }
            if (create && index == null)
            {
                index = new MemoryIndex();
            }

            indexCache.AddOrUpdate(tile, type, index);
            
            return index;
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
            using (var baseData = snapshotDb.GetTile(tile.X, tile.Y, type).GetEnumerator())
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