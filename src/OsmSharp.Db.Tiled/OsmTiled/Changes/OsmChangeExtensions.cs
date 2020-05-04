using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using OsmSharp.Changesets;
using OsmSharp.Db.Tiled.Logging;
using OsmSharp.Db.Tiled.OsmTiled.IO;

namespace OsmSharp.Db.Tiled.OsmTiled.Changes
{
    internal static class OsmChangeExtensions
    {
        internal static IEnumerable<(OsmGeo osmGeo, ChangeType type)> SortChanges(this OsmChange changeSet, out int count)
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

            count = changes.Count;
            return changes.Select(c => (GetFromChangeSet(c.i, c.type), c.type));
        }
        
        internal static IEnumerable<(OsmGeo osmGeo, IEnumerable<(uint x, uint y)> tiles)> 
            BuildTiledDiffStream(this OsmChange changeset, uint zoom, Func<OsmGeoKey, IEnumerable<(uint x, uint y)>> getTiles)
        {
            // NOTE: the changeset is expected to 'squashed' already and in order (nodes, ways and relations sorted).
            // it's expected to be minimal; deleting an object that has been create in the same changeset will no be considered.
            
            // manage a tile cache.
            var baseTilesCache = new Dictionary<OsmGeoKey, IEnumerable<(uint x, uint y)>>();
            IEnumerable<(uint x, uint y)> GetBaseTiles(OsmGeoKey key)
            {
                if (baseTilesCache.TryGetValue(key, out var baseTiles)) return baseTiles;

                baseTiles = getTiles(key);
                baseTilesCache[key] = baseTiles;
                return baseTiles;
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
            
            // iterate over all changes and return the result.
            var progress = Log.Default.ProgressRelative(getMessage: (p) => $"Processing {p}%");
            var changes = changeset.SortChanges(out var count);
            var i = 0;
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
                if (type == ChangeType.Delete ||
                    type == ChangeType.Modify)
                {                 
                    var removes = new HashSet<(uint x, uint y)>(getTiles(key));
                    if (type != ChangeType.Delete) removes.ExceptWith(newTiles);

                    if (removes.Count > 0)
                    {
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
    }
}