using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using OsmSharp.Changesets;
using OsmSharp.Db.Tiled.Changesets;
using OsmSharp.Db.Tiled.Indexes;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.Tiles;
using OsmSharp.IO.Binary;

namespace OsmSharp.Db.Tiled
{
    /// <summary>
    /// A database diff.
    /// </summary>
    /// <remarks>
    /// A diff contains only diff data.
    /// </remarks>
    public class DatabaseDiff : DatabaseBase, IDatabaseView
    {
        private readonly IDatabaseView _baseView;

        /// <summary>
        /// Creates a new database diff view.
        /// </summary>
        /// <param name="baseView">The base view.</param>
        /// <param name="path">The path.</param>
        /// <param name="meta">The meta data.</param>
        /// <param name="mapped">The mapped flag.</param>
        public DatabaseDiff(IDatabaseView baseView, string path, DatabaseMeta meta, bool mapped = true)
            : base(path, meta, mapped)
        {
            _baseView = baseView;
        }

        /// <inheritdoc/>
        public override OsmGeo Get(OsmGeoType type, long id)
        {
            var local = this.GetLocal(type, id);
            if (local != null)
            {
                return local;
            }
            return this._baseView.Get(type, id);
        }
        
        /// <inheritdoc/>
        public override IEnumerable<OsmGeo> GetTile(Tile tile, OsmGeoType type)
        {
            if (tile.Zoom != this.Zoom) throw new ArgumentException("Tile doesn't have the correct zoom level.");
            
            // get the deleted index if any.
            var deletedIndexPath = DatabaseCommon.PathToDeletedIndex(this.Path, type, tile);
            DeletedIndex deletedIndex = null;
            if (FileSystemFacade.FileSystem.Exists(deletedIndexPath))
            {
                using (var stream = FileSystemFacade.FileSystem.OpenRead(deletedIndexPath))
                {
                    deletedIndex = DeletedIndex.Deserialize(stream);
                }
            }

            using (var localData = this.GetLocalTile(tile, type).GetEnumerator())
            using (var baseData = this._baseView.GetTile(tile, type).GetEnumerator())
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

        /// <summary>
        /// Gets the tiles for the given objects.
        /// </summary>
        /// <param name="objects">The objects.</param>
        /// <returns>All tiles with the given object.</returns>
        internal override IReadOnlyList<IEnumerable<(Tile tile, int mask)>> GetTilesFor(IEnumerable<(OsmGeoType type, long id)> objects)
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
                        if (nodeIndex == null) nodeIndex = LoadIndex(type, tile);
                        if (nodeIndex != null &&
                            nodeIndex.TryGetMask(id, out var nodeMask))
                        {
                            mask |= nodeMask;
                        }

                        break;
                    case OsmGeoType.Way:
                        if (wayIndex == null) wayIndex = LoadIndex(type, tile);
                        if (wayIndex != null &&
                            wayIndex.TryGetMask(id, out var wayMask))
                        {
                            mask |= wayMask;
                        }

                        break;
                    case OsmGeoType.Relation:
                        if (relationIndex == null) relationIndex = LoadIndex(type, tile);
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
                return (this._baseView as DatabaseBase).GetTilesFor(objects);
            }
            
            // add first tile.
            tilesPerZoom.Add(new []{ (tile, mask)});

            // split tiles per level.
            uint zoom = 0;
            while (zoom < this.Zoom)
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
                                    if (nodeIndex == null) nodeIndex = LoadIndex(type, currentTile);
                                    if (nodeIndex != null &&
                                        nodeIndex.TryGetMask(id, out var nodeMask))
                                    {
                                        mask |= nodeMask;
                                    }
                                    break;
                                case OsmGeoType.Way:
                                    if (wayIndex == null) wayIndex = LoadIndex(type, currentTile);
                                    if (wayIndex != null &&
                                        wayIndex.TryGetMask(id, out var wayMask))
                                    {
                                        mask |= wayMask;
                                    }
                                    break;
                                case OsmGeoType.Relation:
                                    if (relationIndex == null) relationIndex = LoadIndex(type, currentTile);
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

        /// <summary>
        /// Marks the given object as deleted.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <param name="id">The id.</param>
        internal void Delete(OsmGeoType type, long id)
        {
            // TODO: deletions could cause another object to be removed from tiles.
            
            var tiles = this.GetTilesFor(new[] {(type, id)});
            var dataTiles = tiles[tiles.Count - 1];

            foreach (var dataTile in dataTiles)
            {
                var deletedIndexStream = DatabaseCommon.OpenAppendStreamDeletedIndex(this.Path, type, dataTile.tile);
                deletedIndexStream.AppendToDeletedIndex(id);
            }
        }

        /// <summary>
        /// Creates the given object.
        /// </summary>
        /// <param name="osmGeo">The object to create.</param>
        internal void Create(OsmGeo osmGeo)
        {
            // TODO: creating relations could cause loops in the relations and change tile membership.
            
            // determine the tile for this new object.
            IReadOnlyList<IEnumerable<(Tile tile, int mask)>> tiles = null;
            switch (osmGeo.Type)
            {
                case OsmGeoType.Node:
                    var node = osmGeo as Node;
                    tiles = this.GetTilesFor(node.Longitude.Value, node.Latitude.Value);
                    break;
                case OsmGeoType.Way:
                    var way = osmGeo as Way;
                    var nodes = new List<(OsmGeoType type, long id)>();
                    foreach (var n in way.Nodes)
                    {
                        nodes.Add((OsmGeoType.Node, n));
                    }
                    tiles = this.GetTilesFor(nodes);
                    break;
                case OsmGeoType.Relation:
                    var relation = osmGeo as Relation;
                    var members = new List<(OsmGeoType type, long id)>();
                    foreach (var m in relation.Members)
                    {
                        members.Add((m.Type, m.Id));
                    }
                    tiles = this.GetTilesFor(members);
                    break;
            }
            
            // add the object to all the indexes/tiles.
            for (var l = 0; l < tiles.Count - 1; l++)
            {
                foreach (var (tile, mask) in tiles[l])
                {
                    using (var stream = DatabaseCommon.OpenAppendStreamIndex(this.Path, osmGeo.Type, tile))
                    {
                        stream.AppendToIndex(osmGeo.Id.Value, mask);
                    }
                }
            }
            
            // add the object to all the data tiles.
            foreach (var (tile, _) in tiles[tiles.Count - 1])
            {
                using (var stream = DatabaseCommon.OpenAppendStreamTile(this.Path, osmGeo.Type, tile))
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

        /// <summary>
        /// Creates the given object.
        /// </summary>
        /// <param name="osmGeo">The object to modify.</param>
        internal void Modify(OsmGeo osmGeo)
        {
            var recreate = new Dictionary<OsmGeoKey, OsmGeo>();
            
            if (osmGeo.Type == OsmGeoType.Node)
            {
                var oldTiles = this.GetTilesFor(new[] {(osmGeo.Type, osmGeo.Id.Value)});
                
                var node = osmGeo as Node;

                var oldTile = oldTiles[oldTiles.Count - 1].First();
                var newTiles = this.GetTilesFor(node.Longitude.Value, node.Latitude.Value);
                var newTile = newTiles[newTiles.Count - 1].First();

                if (newTile.tile.LocalId != oldTile.tile.LocalId)
                { // node is NOT in the same tile, just recreate the node, it will be moved.
                    // trigger move of dependent objects.
                    foreach (var parent in this.GetParentsIn(oldTile.tile, osmGeo.Type, osmGeo.Id.Value))
                    {
                        recreate[new OsmGeoKey(parent)] = parent;
                    }
                    
                    // recreate the node.
                    this.Create(node);
                }
                else
                { // node is in the same tile, just add it to the local data tile, it will be used instead of base.
                    // add the node to the data tile.
                    using (var stream = DatabaseCommon.OpenAppendStreamTile(this.Path, osmGeo.Type, newTile.tile))
                    {
                        stream.Append(osmGeo as Node);
                    }
                }
            }
            else if (osmGeo.Type == OsmGeoType.Way)
            {
                var way = osmGeo as Way;
                
                // build set of old tiles.
                var oldTiles = this.GetTilesFor(new[] {(osmGeo.Type, osmGeo.Id.Value)});
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
                var newTiles = this.GetTilesFor(nodes);
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
                        foreach (var parent in this.GetParentsIn(oldTile.tile, osmGeo.Type, osmGeo.Id.Value))
                        {
                            recreate[new OsmGeoKey(parent)] = parent;
                        }
                    }
                    
                    // recreate the way.
                    this.Create(way);
                }
                else
                { // way is exactly the same tiles, just write it to all of them.
                    foreach (var newTile in newTiles[newTiles.Count - 1])
                    {
                        // add the node to the data tile.
                        using (var stream = DatabaseCommon.OpenAppendStreamTile(this.Path, osmGeo.Type, newTile.tile))
                        {
                            stream.Append(osmGeo as Node);
                        }
                    }
                }
            }
            else
            {
                 var relation = osmGeo as Relation;
                
                // build set of old tiles.
                var oldTiles = this.GetTilesFor(new[] {(osmGeo.Type, osmGeo.Id.Value)});
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
                var newTiles = this.GetTilesFor(members);
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
                        foreach (var parent in this.GetParentsIn(oldTile.tile, osmGeo.Type, osmGeo.Id.Value))
                        {
                            recreate[new OsmGeoKey(parent)] = parent;
                        }
                    }
                    
                    // recreate the way.
                    this.Create(relation);
                }
                else
                { // relation is exactly the same tiles, just write it to all of them.
                    foreach (var newTile in newTiles[newTiles.Count - 1])
                    {
                        // add the node to the data tile.
                        using (var stream = DatabaseCommon.OpenAppendStreamTile(this.Path, osmGeo.Type, newTile.tile))
                        {
                            stream.Append(osmGeo as Node);
                        }
                    }
                }
            }

            // recreate all objects that could have been moved.
            foreach (var create in recreate.Values)
            {
                this.Create(create);
            }
        }

        /// <summary>
        /// Gets the parents of the given object in the given tile.
        /// </summary>
        /// <param name="tile">The tile.</param>
        /// <param name="type">The type.</param>
        /// <param name="id">The id.</param>
        /// <returns>The objects with the given object as member/node.</returns>
        private IEnumerable<OsmGeo> GetParentsIn(Tile tile, OsmGeoType type, long id)
        {
            if (type == OsmGeoType.Node)
            { // check ways only.
                foreach (var osmGeo in this.GetTile(tile, OsmGeoType.Way))
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
                foreach (var osmGeo in this.GetTile(tile, OsmGeoType.Relation))
                {
                    var relation = osmGeo as Relation;
                    if (relation?.Members != null && relation.Members.Any(m => m.Id == id && m.Type == type))
                    {
                        yield return osmGeo;
                    }
                }
            }
        }
    }
}