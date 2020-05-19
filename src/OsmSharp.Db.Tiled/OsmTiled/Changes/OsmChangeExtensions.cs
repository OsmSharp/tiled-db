using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using OsmSharp.Changesets;
using OsmSharp.Db.Tiled.Collections;
using OsmSharp.Db.Tiled.Indexes.TileMaps;
using OsmSharp.Db.Tiled.Logging;
using OsmSharp.Db.Tiled.OsmTiled.IO;
using OsmSharp.Db.Tiled.Tiles;

namespace OsmSharp.Db.Tiled.OsmTiled.Changes
{
    internal static class OsmChangeExtensions
    {
        internal static int Count(this OsmChange changeSet)
        {
            var count = changeSet.Delete?.Length ?? 0;
            count += changeSet.Create?.Length ?? 0;
            count += changeSet.Modify?.Length ?? 0;
            return count;
        }
        
        internal static IEnumerable<(OsmGeo osmGeo, ChangeType type)> Changes(this OsmChange changeSet)
        {
            // collect all changes and sort.
            if (changeSet.Delete != null)
            {
                for (var i = 0; i < changeSet.Delete.Length; i++)
                {
                    yield return (changeSet.Delete[i], ChangeType.Delete);
                }
            }
            if (changeSet.Create != null)
            {
                for (var i = 0; i < changeSet.Create.Length; i++)
                {
                    yield return (changeSet.Create[i], ChangeType.Create);
                }
            }
            if (changeSet.Modify != null)
            {
                for (var i = 0; i < changeSet.Modify.Length; i++)
                {
                    yield return (changeSet.Modify[i], ChangeType.Modify);
                }
            }
        }

        internal static IEnumerable<(OsmGeo osmGeo, ChangeType type)> SortChanges(this OsmChange changeSet)
        {
            // collect all changes and sort.
            var changes = new List<(int i, ChangeType type)>();
            if (changeSet.Delete != null)
            {
                for (var i = 0; i < changeSet.Delete.Length; i++)
                {
                    changes.Add((i, ChangeType.Delete));
                }
            }
            if (changeSet.Create != null)
            {
                for (var i = 0; i < changeSet.Create.Length; i++)
                {
                    changes.Add((i, ChangeType.Create));
                }
            }
            if (changeSet.Modify != null)
            {
                for (var i = 0; i < changeSet.Modify.Length; i++)
                {
                    changes.Add((i, ChangeType.Modify));
                }
            }

            OsmGeo GetFromChangeSet(int i, ChangeType type)
            {
                switch (type)
                {
                    case ChangeType.Create:
                        return changeSet.Create[i];
                    case ChangeType.Modify:
                        return changeSet.Modify[i];
                    case ChangeType.Delete:
                        return changeSet.Delete[i];
                }
                throw new ArgumentException("Cannot find data.");
            }
            
            // sort changes.
            changes.Sort((o1, o2) =>
            {
                var osmGeo1 = GetFromChangeSet(o1.i, o1.type);
                var osmGeo2 = GetFromChangeSet(o2.i, o2.type);

                return (new OsmGeoKey(osmGeo1)).CompareTo(new OsmGeoKey(osmGeo2));
            });

            return changes.Select(c => (GetFromChangeSet(c.i, c.type), c.type));
        }

        internal static IEnumerable<OsmGeo> GetObjectsToUpdate(this IEnumerable<OsmGeo> data, HashSet<OsmGeoKey> objectsRemovedFromTile)
        {
            foreach (var osmGeo in data)
            {
                if (osmGeo is Way way &&
                    way.Nodes != null)
                {
                    foreach (var n in way.Nodes)
                    {
                        if (!objectsRemovedFromTile.Contains(new OsmGeoKey(OsmGeoType.Node, n))) continue;
                        yield return osmGeo;
                        break;
                    }
                }
                else if (osmGeo is Relation relation &&
                         relation.Members != null)
                {
                    foreach (var m in relation.Members)
                    {
                        if (!objectsRemovedFromTile.Contains(new OsmGeoKey(m.Type, m.Id))) continue;
                        yield return osmGeo;
                        break;
                    }
                }
            }
        }
        
        internal static IEnumerable<(OsmGeo osmGeo, IEnumerable<(uint x, uint y)> tiles)> 
            BuildTiledDiffStream(this OsmChange changeset, uint zoom, OsmTiledDbBase osmTiledDb, byte[] buffer)
        {
            // NOTE: the changeset is expected to 'squashed' already and in order (nodes, ways and relations sorted).
            // it's expected to be minimal; deleting an object that has been create in the same changeset will no be considered.
            
            var tilesWithRemovals = new HashSet<(uint x, uint y)>();
            var objectsRemovedFromTile = new HashSet<OsmGeoKey>();
            
            // manage a tile cache.
            var baseTilesCache = new Dictionary<OsmGeoKey, IEnumerable<(uint x, uint y)>>();
            IEnumerable<(uint x, uint y)> GetBaseTiles(OsmGeoKey key)
            {
                if (baseTilesCache.TryGetValue(key, out var baseTiles))
                {
                    foreach (var tile in baseTiles)
                    {
                        yield return tile;
                    }
                    yield break;
                }

                baseTiles = osmTiledDb.GetTilesFor(key).ToList();
                baseTilesCache[key] = baseTiles;
                foreach (var tile in baseTiles)
                {
                    yield return tile;
                }
            }
            
            // manage modified data.
            var modifiedTiles = new Dictionary<OsmGeoKey, IEnumerable<(uint x, uint y)>>();
            IEnumerable<(uint x, uint y)> GetModifiedTiles(OsmGeoKey key)
            {
                if (key.Type == OsmGeoType.Node && modifiedTiles.TryGetValue(key, out var baseTiles))
                {
                    return baseTiles;
                }

                return GetBaseTiles(key);
            }
            
            // iterate over all changes a first pass and collect if any objects have moved tiles
            // cache all tile lists.
            var progress = Log.Default.ProgressRelative(getMessage: (p) => $"Collecting tiles and movements... {p}%");
            var count = changeset.Count();
            var i = 0;
            var changes = changeset.SortChanges();
            foreach (var (osmGeo, type) in changes)
            {
                progress.Progress(i, count);
                i++;
                
                // bookkeeping.
                if (osmGeo.Id == null) throw new InvalidDataException("Cannot store object without a valid id.");
                if (osmGeo.Version == null) throw new InvalidDataException("Cannot apply changes for an object without a valid version.");

                // get all the tiles for each object.
                // make sure all tiles are in the global modified tiles set.
                // - get the old tile for modified nodes.
                var key = new OsmGeoKey(osmGeo);
                
                // get the new tiles.
                var newTiles = osmGeo.GetTile(zoom, GetModifiedTiles);
                
                // handle removals.
                var modified = false;
                if (type == ChangeType.Modify)
                {                 
                    var removes = new HashSet<(uint x, uint y)>(GetBaseTiles(key));
                    if (type != ChangeType.Delete) removes.ExceptWith(newTiles);

                    if (removes.Count > 0)
                    {
                        tilesWithRemovals.UnionWith(removes);
                        objectsRemovedFromTile.Add(key);
                        modified = true;
                    }
                }
                
                // handle new data.
                if (type == ChangeType.Delete) continue;
                    
                // update modified tiles if create or modify.
                if (modified || type == ChangeType.Create) modifiedTiles[key] = newTiles;
            }
            
            // collect all recreations from the tiles that have removals.
            var sortedChanges = changes;
            if (tilesWithRemovals.Count > 0)
            {
                Log.Default.Verbose($"Found tiles with removals, getting {tilesWithRemovals.Count} tiles to query {objectsRemovedFromTile.Count} objects.");
                var dataFromTiles = osmTiledDb.Get(tilesWithRemovals).Select(x => x.osmGeo)
                    .GetObjectsToUpdate(objectsRemovedFromTile).Select<OsmGeo, (OsmGeo osmGeo, ChangeType type)>(x => (x, ChangeType.Modify));
                sortedChanges = sortedChanges.MergeWhenSorted(dataFromTiles, (x, y) => x.osmGeo.CompareByIdAndType(y.osmGeo));
            }
                
            progress = Log.Default.ProgressRelative(getMessage: (p) => $"Generating tiled changes... {p}%");
            i = 0;
            foreach (var (osmGeo, type) in sortedChanges)
            {
                progress.Progress(i, count);
                i++;
                
                // bookkeeping.
                if (osmGeo.Id == null) throw new InvalidDataException("Cannot store object without a valid id.");
                if (osmGeo.Version == null) throw new InvalidDataException("Cannot apply changes for an object without a valid version.");

                // get all the tiles for each object.
                // make sure all tiles are in the global modified tiles set.
                // - get the old tile for modified nodes.
                var key = new OsmGeoKey(osmGeo);
                
                // get the new tiles.
                var newTiles = osmGeo.GetTile(zoom, GetModifiedTiles);
                
                // handle removals.
                var modified = false;
                if (type == ChangeType.Delete ||
                    type == ChangeType.Modify)
                {                 
                    var removes = new HashSet<(uint x, uint y)>(GetBaseTiles(key));
                    if (type != ChangeType.Delete) removes.ExceptWith(newTiles);

                    if (removes.Count > 0)
                    {
                        tilesWithRemovals.UnionWith(removes);
                        objectsRemovedFromTile.Add(key);
                        modified = true;
                        yield return (osmGeo.CloneAsDeleted(), removes);
                    }
                }
                
                // handle new data.
                if (type == ChangeType.Delete) continue;
                yield return (osmGeo, newTiles);
                    
                // update modified tiles if create or modify.
                if (modified || type == ChangeType.Create) modifiedTiles[key] = newTiles;
            }
        }

        internal static IEnumerable<(OsmGeo osmGeo, IEnumerable<(uint x, uint y)> tiles)> ApplyTiledDiffStream(
            this IEnumerable<(OsmGeo osmGeo, IEnumerable<(uint x, uint y)> tiles)> data,
            IEnumerable<(OsmGeo osmGeo, IEnumerable<(uint x, uint y)>? tiles)> diff)
        {
            using var existingStream = data.GetEnumerator();
            using var modifiedStream = diff.GetEnumerator();
            
            var existingHasNext = existingStream.MoveNext();
            var modifiedHasNext = modifiedStream.MoveNext();

            var i = 0L;
            var progress = Log.Default.ProgressAbsolute(TraceEventType.Verbose,
                (p) => $"Processed {p}...", 1000000);
            var removedTiles = new HashSet<(uint x, uint t)>();
            while (existingHasNext || modifiedHasNext)
            {
                progress.Progress(i);
                i++;

                if (existingHasNext && modifiedHasNext)
                {
                    // compare and take first.
                    var existing = existingStream.Current;
                    var modified = modifiedStream.Current;
                    if (existing.osmGeo.Id == null) throw new InvalidDataException("Object found without an id.");
                    if (modified.osmGeo.Id == null) throw new InvalidDataException("Object found without an id.");
                    var modifiedId = OsmGeoCoder.Encode(modified.osmGeo.Type, modified.osmGeo.Id.Value);
                    var existingId = OsmGeoCoder.Encode(existing.osmGeo.Type, existing.osmGeo.Id.Value);
                    if (existingId < modifiedId)
                    {
                        // move existing.
                        var next = existing;
                        yield return (next.osmGeo, next.tiles ?? Enumerable.Empty<(uint x, uint y)>());
                        existingHasNext = existingStream.MoveNext();
                    }
                    else if (modifiedId < existingId)
                    {
                        // move modified.
                        var next = modified;
                        yield return (next.osmGeo, next.tiles ?? Enumerable.Empty<(uint x, uint y)>());
                        modifiedHasNext = modifiedStream.MoveNext();
                    }
                    else
                    {
                        // overwrite existing.
                        var next = modified;

                        // work out removed tiles if any, if different return a deleted node with the removed tiles.
                        removedTiles.Clear();
                        if (existing.tiles != null) removedTiles.UnionWith(existing.tiles);
                        removedTiles.ExceptWith(modified.tiles);
                        if (removedTiles.Count > 0)
                        {
                             yield return (next.osmGeo.CloneAsDeleted(), removedTiles);
                        }

                        // move both.
                        yield return (next.osmGeo, next.tiles ?? Enumerable.Empty<(uint x, uint y)>());
                        modifiedHasNext = modifiedStream.MoveNext();
                        existingHasNext = existingStream.MoveNext();
                    }
                }
                else if (existingHasNext)
                {
                    // move existing.
                    var next = existingStream.Current;
                    yield return (next.osmGeo, next.tiles ?? Enumerable.Empty<(uint x, uint y)>());
                    existingHasNext = existingStream.MoveNext();
                }
                else
                {
                    // move modified.
                    var next = modifiedStream.Current;
                    yield return (next.osmGeo, next.tiles ?? Enumerable.Empty<(uint x, uint y)>());
                    modifiedHasNext = modifiedStream.MoveNext();
                }
            }
        }

        internal static IEnumerable<(OsmGeo osmGeo, IEnumerable<(uint x, uint y)>? tiles)> MergeTiledDiffStream(
            this IEnumerable<IEnumerable<(OsmGeo osmGeo, IEnumerable<(uint x, uint y)>? tiles)>> diffs)
        {
            IEnumerable<(OsmGeo osmGeo, IEnumerable<(uint x, uint y)>? tiles)> first = 
                Enumerable.Empty<(OsmGeo osmGeo, IEnumerable<(uint x, uint y)>? tiles)>();
            foreach (var diff in diffs)
            {
                first = first.MergeTiledDiffStream(diff);
            }

            return first;
        }

        internal static IEnumerable<(OsmGeo osmGeo, IEnumerable<(uint x, uint y)>? tiles)> MergeTiledDiffStream(
            this IEnumerable<(OsmGeo osmGeo, IEnumerable<(uint x, uint y)>? tiles)> diff1,
            IEnumerable<(OsmGeo osmGeo, IEnumerable<(uint x, uint y)>? tiles)> diff2)
        {
            throw new NotImplementedException();
        }
        
        internal static IEnumerable<(OsmGeo osmGeo, IEnumerable<uint>? tiles)> MergeTiledDiffStream(
            this IEnumerable<IEnumerable<(OsmGeo osmGeo, IEnumerable<uint>? tiles)>> diffs)
        {
            IEnumerable<(OsmGeo osmGeo, IEnumerable<uint>? tiles)> first = 
                Enumerable.Empty<(OsmGeo osmGeo, IEnumerable<uint>? tiles)>();
            foreach (var diff in diffs)
            {
                first = first.MergeTiledDiffStream(diff);
            }

            return first;
        }

        internal static IEnumerable<(OsmGeo osmGeo, IEnumerable<uint>? tiles)> MergeTiledDiffStream(
            this IEnumerable<(OsmGeo osmGeo, IEnumerable<uint>? tiles)> diff1,
            IEnumerable<(OsmGeo osmGeo, IEnumerable<uint>? tiles)> diff2)
        {
            using var baseEnumerator = diff1.GetEnumerator();
            using var thisEnumerator = diff2.GetEnumerator();
            var baseHasNext = baseEnumerator.MoveNext();
            var thisHasNext = thisEnumerator.MoveNext();

            while (baseHasNext || thisHasNext)
            {
                if (baseHasNext && thisHasNext)
                {
                    var baseKey = new OsmGeoKey(baseEnumerator.Current.osmGeo);
                    var thisKey = new OsmGeoKey(thisEnumerator.Current.osmGeo);

                    if (baseKey < thisKey)
                    {
                        yield return baseEnumerator.Current;
                        baseHasNext = baseEnumerator.MoveNext();
                    }
                    else if (thisKey < baseKey)
                    {
                        yield return thisEnumerator.Current;
                        thisHasNext = thisEnumerator.MoveNext();
                    }
                    else
                    {
                        yield return thisEnumerator.Current;
                        baseHasNext = baseEnumerator.MoveNext();
                        thisHasNext = thisEnumerator.MoveNext();
                    }
                }
                else if (baseHasNext)
                {
                    yield return baseEnumerator.Current;
                    baseHasNext = baseEnumerator.MoveNext();
                }
                else
                {
                    yield return thisEnumerator.Current;
                    thisHasNext = thisEnumerator.MoveNext();
                }
            }
        }
    }
}