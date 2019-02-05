using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using OsmSharp.Db.Tiled.Indexes;
using OsmSharp.Db.Tiled.Tiles;
using OsmSharp.IO.Binary;
using OsmSharp.Streams;
using Serilog;

namespace OsmSharp.Db.Tiled.Build
{
    /// <summary>
    /// The relation processor.
    /// </summary>
    internal static class RelationProcessor
    {
        /// <summary>
        /// Processes the relations in the given stream. Assumed the current stream position already contains a relation.
        /// </summary>
        /// <param name="source">The source stream.</param>
        /// <param name="path">The based path of the db.</param>
        /// <param name="maxZoom">The maximum zoom.</param>
        /// <param name="tile">The tile being split.</param>
        /// <param name="nodeIndex">The node index.</param>
        /// <param name="wayIndex">The way index.</param>
        /// <param name="compressed">A flag to allow compression of target files.</param>
        /// <param name="saveLocally">Save the data locally when the mask is '0' and the object cannot be place in any tile.</param>
        /// <returns>The indexed node id's with a masked zoom.</returns>
        public static Index Process(OsmStreamSource source, string path, uint maxZoom, Tile tile,
            Index nodeIndex, Index wayIndex, bool compressed = false, bool saveLocally = false)
        {
            // split relations.
            var subTiles = new Dictionary<ulong, Stream>();
            foreach (var subTile in tile.GetSubtilesAt(tile.Zoom + 2))
            {
                subTiles.Add(subTile.LocalId, null);
            }
            
            // determine the relation tile based on ways/nodes members only.
            var relations = !source.CanReset ? new List<Relation>() : null;
            var unstableSet = new Dictionary<long, long[]>();
            var stableSet = new Dictionary<long, int>();
            do
            {
                var current = source.Current();
                if (current.Type != OsmGeoType.Relation)
                { // make sure the current object is a relation.
                    if (!source.MoveNext(true, true, false))
                    {
                        break;
                    }
                    continue;
                }

                // calculate tile.
                var r = (current as Relation);
                if (r?.Members == null)
                {
                    continue;
                }
                
                // keep a copy if source stream cannot be reset.
                relations?.Add(r);

                var mask = 0;
                var relationMembers = new List<long>();
                foreach (var member in r.Members)
                {
                    if (member == null)
                    {
                        Log.Warning($"Member of relation {r.Id} is null!");
                        continue;
                    }
                    switch (member.Type)
                    {
                        case OsmGeoType.Node:
                            if (nodeIndex !=null &&
                                nodeIndex.TryGetMask(member.Id, out var nodeMask))
                            {
                                mask |= nodeMask;
                            }
                            break;
                        case OsmGeoType.Way:
                            if (wayIndex != null &&
                                wayIndex.TryGetMask(member.Id, out var wayMask))
                            {
                                mask |= wayMask;
                            }
                            break;
                        case OsmGeoType.Relation:
                            relationMembers.Add(member.Id);
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }

                if (relationMembers.Count == 0)
                { // not relation members, this is an easy one.
                    stableSet[r.Id.Value] = mask;
                }
                else
                { // has relation members, this is an unstable one.
                    unstableSet[r.Id.Value] = relationMembers.ToArray();
                }
            } while (source.MoveNext());
            
            // keeps relations that are determined to be equivalent to another one for mask. 
            // this means that they are either directly or indirectly each others members.
            var equivalencies = new List<HashSet<long>>(); 
            
            // determine tiles for unstable relations.
            while (unstableSet.Count > 0)
            {
                var newUnstableSet = new Dictionary<long, long[]>();
                while (unstableSet.Count > 0)
                {
                    var relationInfo = unstableSet.First();
                    var mask = Stabilize(unstableSet, stableSet, equivalencies, relationInfo.Key);
                    if (mask >= 0) continue;
                    
                    newUnstableSet.Add(relationInfo.Key, relationInfo.Value);
                    unstableSet.Remove(relationInfo.Key);
                }
                
                unstableSet = newUnstableSet;
            }
            
            // reset source if possible, otherwise use relations list.
            if (relations == null)
            {
                source.Reset();
            }
            else
            {
                source = new OsmEnumerableStreamSource(relations);
            }
            
            // build the relations index.
            var relationIndex = new Index();
            if (!source.MoveNext(true, true, false))
            {
                return relationIndex;
            }
            do
            {
                var current = source.Current();
                if (current.Type != OsmGeoType.Relation)
                {
                    break;
                }

                // calculate tile.
                var r = (current as Relation);
                if (r?.Members == null)
                {
                    continue;
                }

                if (!stableSet.TryGetValue(r.Id.Value, out var mask))
                { // make not found, this should be impossible.
                    Log.Warning($"Mask for relation {r} could not be found!");
                    continue;
                }

                if (saveLocally && mask == 0)
                { // no place for object in sub tile, place locally if requested.
                    using (var stream = DatabaseCommon.CreateLocalTileObject(path, tile, r, compressed))
                    {
                        stream.Append(r);
                    }
                    continue;
                }

                // add relation to output(s).
                foreach(var relationTile in tile.SubTilesForMask2(mask))
                {
                    // is tile a sub tile.
                    if (!subTiles.TryGetValue(relationTile.LocalId, out var stream))
                    {
                        continue;
                    }


                    // initialize stream if needed.
                    if (stream == null)
                    {
                        stream = DatabaseCommon.CreateTile(path, OsmGeoType.Relation, relationTile, compressed);
                        subTiles[relationTile.LocalId] = stream;
                    }

                    // write way.
                    stream.Append(r);
                }
                
                // add way to index.
                relationIndex.Add(r.Id.Value, mask);
            } while (source.MoveNext());

            // flush/dispose all subtile streams.
            foreach (var subtile in subTiles)
            {
                if (subtile.Value == null) continue;
                subtile.Value.Flush();
                subtile.Value.Dispose();
            }

            return relationIndex;
        }

        /// <summary>
        /// Tries to stabilize the given relation.
        /// </summary>
        /// <param name="unstableSet">The set of unstable relations.</param>
        /// <param name="stableSet">The set of stable relations.</param>
        /// <param name="equivalencies">The list of equivalencies.</param>
        /// <param name="relation">The relation to check.</param>
        /// <param name="path">The path to the current relation.</param>
        /// <returns>A mask if stable.</returns>
        private static int Stabilize(Dictionary<long, long[]> unstableSet, Dictionary<long, int> stableSet, 
            List<HashSet<long>> equivalencies, long relation, HashSet<long> path = null)
        {
            var children = unstableSet[relation];
            
            if (path == null)
            { // create the path if it doesn't exist yet.
                path = new HashSet<long>();
            }
            else if (path.Contains(relation))
            { // this is an equivalency
                // build the and check for overlaps with others.
                var equivalency = path;
                for (var i = 0; i < equivalencies.Count; i++)
                {
                    var equivalentSet = equivalencies[i];
                    if (!equivalentSet.Overlaps(equivalency)) continue;
                    
                    equivalency.UnionWith(equivalentSet);
                    equivalencies.RemoveAt(i);
                    i--;
                }

                // check if the set is stable now.
                var equivalencyMask = StabilizeSet(unstableSet, stableSet, equivalency);
                if (equivalencyMask >= 0) return equivalencyMask; 
                
                // could not determine mask, keep equivalency for later.
                equivalencies.Add(equivalency);
                return -1;
            }
            
            // update path.
            path.Add(relation);

            // go over all children and if they are stable.
            var mask = 0;
            foreach (var child in children)
            {
                if (unstableSet.ContainsKey(child))
                { // there is an unstable child.
                    var childMask = Stabilize(unstableSet, stableSet, equivalencies, child, path);
                    if (childMask < 0)
                    { // could not stabilize child, there is a feedback to a 'higher' level.
                        return -1;
                    }
                    mask |= childMask;
                }
                else
                { // child is either stable or not in tile.
                    if (!stableSet.TryGetValue(child, out var stableMask)) continue; // not in this tile.
                
                    // add this child to the mask.
                    mask |= stableMask;
                }
            }

            // mask could be determined, switch sets.
            unstableSet.Remove(relation);
            stableSet[relation] = mask;
            
            return mask;
        }

        /// <summary>
        /// Checks if an equivalency is stable. An equivalency is stable if all of it's children are:
        /// - Part of the same equivalence.
        /// - Stable.
        /// </summary>
        /// <param name="unstableSet"></param>
        /// <param name="stableSet"></param>
        /// <param name="equivalency"></param>
        /// <returns></returns>
        private static int StabilizeSet(Dictionary<long, long[]> unstableSet, Dictionary<long, int> stableSet, 
            HashSet<long> equivalency)
        {
            var mask = 0;
            foreach (var e in equivalency)
            {
                if (stableSet.TryGetValue(e, out var eMask))
                { // this is a stable member, add mask.
                    mask |= eMask;
                    continue;
                }
                
                // this is an unstable member
                // check if its members are stable or a member of the equivalency.
                if (!unstableSet.TryGetValue(e, out var children)) continue;
                foreach (var child in children)
                {
                    if (stableSet.TryGetValue(child, out var childMask))
                    { // build mask, a stable child.
                        mask |= mask;
                    }
                        
                    if (!equivalency.Contains(child))
                    { // a child is unstable and not in the equivalency.
                        return -1;
                    }
                }
            }

            return mask;
        }
    }
}