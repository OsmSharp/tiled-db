//using System.Collections.Generic;
//using System.IO;
//using OsmSharp.Db.Tiled.Indexes;
//using OsmSharp.Db.Tiled.Tiles;
//using OsmSharp.IO.Binary;
//using OsmSharp.Streams;
//using Serilog;

//namespace OsmSharp.Db.Tiled.Build
//{
//    /// <summary>
//    /// The relation processor.
//    /// </summary>
//    static class RelationProcessor
//    {
//        /// <summary>
//        /// Processes the relations in the given stream. Assumed the current stream position already contains a relation.
//        /// </summary>
//        /// <param name="source">The source stream.</param>
//        /// <param name="path">The based path of the db.</param>
//        /// <param name="maxZoom">The maximum zoom.</param>
//        /// <param name="tile">The tile being split.</param>
//        /// <param name="nodeIndex">The node index.</param>
//        /// <param name="wayIndex">The way index.</param>
//        /// <returns>The indexed node id's with a masked zoom.</returns>
//        public static Index Process(OsmStreamSource source, string path, uint maxZoom, Tile tile,
//            Index nodeIndex, Index wayIndex)
//        {
//            // keep data about relations.
//            var data = new SortedDictionary<long, Status>();

//            // index relations and try to resolve a first time.
//            do
//            {
//                var current = source.Current();
//                if (current.Type != OsmGeoType.Relation)
//                {
//                    break;
//                }

//                // calculate tile.
//                var r = (current as Relation);
//                if (r.Members == null)
//                {
//                    Log.Warning("Encounted relation {r} without any members, this won't be proccessed for now.",
//                        r);
//                    continue;
//                }

//                // try to determine the mask.
//                var mask = Resolve(data, nodeIndex, wayIndex, r.Id.Value);
//                if (mask.HasValue)
//                {
//                    data[r.Id.Value] = new Status()
//                    {
//                        Mask = mask.Value
//                    };
//                }
//                else
//                {
//                    var members = new IdType[r.Members.Length];
//                    for (var i = 0; i < r.Members.Length; i++)
//                    {
//                        members[i] = new IdType()
//                        {
//                            Id = r.Members[i].Id,
//                            Type = r.Members[i].Type
//                        };
//                    }

//                    data[r.Id.Value] = new Status()
//                    {
//                        Members = members
//                    };
//                }
//            } while (source.MoveNext());
            
//            // build subtiles.
//            var subtiles = new Dictionary<ulong, Stream>();
//            foreach (var subTile in tile.GetSubtilesAt(tile.Zoom + 2))
//            {
//                subtiles.Add(subTile.LocalId, null);
//            }

//            // resolve the relations and write to stream.
//            var relationIndex = new Index();
//            foreach (var relationPair in data)
//            {
//                var mask = Resolve(data, nodeIndex, wayIndex, relationPair.Key);
//                if (!mask.HasValue)
//                {
//                    Log.Warning("Could not determine mask for relation {r}, this won't be proccessed for now.",
//                        relationPair.Key);
//                    continue;
//                }

//                // add relation to output(s).
//                foreach (var relationTile in tile.SubTilesForMask2(mask))
//                {
//                    // is tile a subtile.
//                    Stream stream;
//                    if (!subtiles.TryGetValue(relationTile.LocalId, out stream))
//                    {
//                        continue;
//                    }

//                    // initialize stream if needed.
//                    if (stream == null)
//                    {
//                        var file = Path.Combine(path, relationTile.Zoom.ToInvariantString(), relationTile.X.ToInvariantString(),
//                            relationTile.Y.ToInvariantString() + ".relations.osm.bin");
//                        var fileInfo = new FileInfo(file);
//                        if (!fileInfo.Directory.Exists)
//                        {
//                            fileInfo.Directory.Create();
//                        }
//                        stream = File.Open(file, FileMode.Create);
//                        stream = new LZ4.LZ4Stream(stream, LZ4.LZ4StreamMode.Compress);

//                        subtiles[relationTile.LocalId] = stream;
//                    }

//                    // write.
//                    BinarySerializer.Append(stream, r);
//                }

//                // add relation to index.
//                relationIndex.Add(r.Id.Value, mask);
//            }

//            // flush/dispose all subtile streams.
//            foreach (var subtile in subtiles)
//            {
//                if (subtile.Value != null)
//                {
//                    subtile.Value.Flush();
//                    subtile.Value.Dispose();
//                }
//            }

//            return relationIndex;
//        }

//        private static int? Resolve(SortedDictionary<long, Status> data, Index nodesIndex, Index waysIndex, Relation r, 
//            HashSet<long> parents = null)
//        {
//            if (r.Members == null)
//            {
//                return null;
//            }

//            int mask = 0;
//            HashSet<long> newParents = null;
//            for (var i = 0; i < r.Members.Length; i++)
//            {
//                var member = r.Members[i];

//                int childMask = 0;
//                switch (member.Type)
//                {
//                    case OsmGeoType.Node:
//                        if (!nodesIndex.TryGetMask(member.Id, out childMask))
//                        {
//                            childMask = 0;
//                        }
//                        break;
//                    case OsmGeoType.Way:
//                        if (!waysIndex.TryGetMask(member.Id, out childMask))
//                        {
//                            childMask = 0;
//                        }
//                        break;
//                    case OsmGeoType.Relation:
//                        if (parents.Contains(member.Id) ||
//                            member.Id == r.Id.Value)
//                        {
//                            continue;
//                        }
//                        if (newParents != null)
//                        {
//                            newParents = new HashSet<long>();
//                            newParents.Add(r.Id.Value);
//                        }
//                        var memberMask = Resolve(data, nodesIndex, waysIndex, member.Id,
//                            newParents);
//                        if (memberMask.HasValue)
//                        {
//                            childMask = memberMask.Value;
//                        }
//                        else
//                        {
//                            return null;
//                        }
//                        break;
//                }

//                mask |= childMask;
//            }
            
//            return mask;
//        }

//        private static int? Resolve(SortedDictionary<long, Status> data, Index nodesIndex, Index waysIndex, long id, 
//            HashSet<long> parents = null)
//        {
//            Status status;
//            if (!data.TryGetValue(id, out status))
//            {
//                return 0;
//            }

//            if (status.Mask.HasValue)
//            {
//                return status.Mask;
//            }

//            if (status.Members == null)
//            {
//                return 0;
//            }

//            HashSet<long> newParents = null;
//            int mask = 0;
//            for (var i = 0; i < status.Members.Length; i++)
//            {
//                var member = status.Members[i];

//                int childMask = 0;
//                switch (member.Type)
//                {
//                    case OsmGeoType.Node:
//                        if (!nodesIndex.TryGetMask(member.Id, out childMask))
//                        {
//                            childMask = 0;
//                        }
//                        break;
//                    case OsmGeoType.Way:
//                        if (!waysIndex.TryGetMask(member.Id, out childMask))
//                        {
//                            childMask = 0;
//                        }
//                        break;
//                    case OsmGeoType.Relation:
//                        if (parents.Contains(member.Id) ||
//                            member.Id == r.Id.Value)
//                        {
//                            continue;
//                        }
//                        if (newParents != null)
//                        {
//                            if (parents != null)
//                            {
//                                newParents = new HashSet<long>(parents);
//                            }
//                            else
//                            {
//                                newParents = new HashSet<long>();
//                            }
//                            newParents.Add(id);
//                        }
//                        var memberMask = Resolve(data, nodesIndex, waysIndex, member.Id,
//                            newParents);
//                        if (memberMask.HasValue)
//                        {
//                            childMask = memberMask.Value;
//                        }
//                        break;
//                }

//                mask |= childMask;
//            }

//            data[id] = new Status()
//            {
//                Mask = mask
//            };
//            return mask;
//        }

//        private struct IdType
//        {
//            public long Id { get; set; }

//            public OsmGeoType Type { get; set; }
//        }

//        private class Status
//        {
//            public int? Mask { get; set; }

//            public IdType[] Members { get; set; }
//        }
//    }
//}