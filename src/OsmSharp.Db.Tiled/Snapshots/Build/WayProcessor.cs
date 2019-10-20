using System;
using System.Collections.Generic;
using OsmSharp.Db.Tiled.Indexes;
using OsmSharp.Db.Tiled.Tiles;
using OsmSharp.IO.Binary;
using OsmSharp.Streams;
using System.IO;
using OsmSharp.Db.Tiled.Snapshots.IO;

namespace OsmSharp.Db.Tiled.Snapshots.Build
{
    /// <summary>
    /// The way processor.
    /// </summary>
    internal static class WayProcessor
    {
        /// <summary>
        /// Processes the ways in the given stream until the first on-way object is reached. Assumed the current stream position already contains a way.
        /// </summary>
        /// <param name="source">The source stream.</param>
        /// <param name="path">The based path of the db.</param>
        /// <param name="maxZoom">The maximum zoom.</param>
        /// <param name="tile">The tile being split.</param>
        /// <param name="nodeIndex">The node index.</param>
        /// <returns>The indexed node id's with a masked zoom.</returns>
        public static (Index index, bool hasNext, DateTime timestamp) Process(OsmStreamSource source, string path, uint maxZoom, Tile tile,
            Index nodeIndex)
        { 
            // split ways.
            var subTiles = new Dictionary<ulong, Stream>();
            foreach (var subTile in tile.GetSubtilesAt(tile.Zoom + 2))
            {
                subTiles.Add(subTile.LocalId, null);
            }

            // build the ways index.
            var wayIndex = new Index();
            var hasNext = false;
            var timestamp = DateTime.MinValue;
            do
            {
                var current = source.Current();
                if (current.Type != OsmGeoType.Way)
                {
                    hasNext = true;
                    break;
                }
                
                // update timestamp.
                if (current.TimeStamp.HasValue &&
                    current.TimeStamp > timestamp)
                {
                    timestamp = current.TimeStamp.Value;
                }

                // calculate tile.
                if (!(current is Way w))
                {
                    throw new InvalidDataException($"A way was found with type way but not could not be cast to a {nameof(Way)}.");
                }
                if (!w.Id.HasValue)
                {
                    throw new InvalidDataException($"A way was found without an valid ID.");
                }
                if (w.Nodes == null)
                {
                    // TODO: log a warning or report somewhere on this way.
                    continue;
                }

                var mask = 0;
                foreach (var node in w.Nodes)
                {
                    if (nodeIndex.TryGetMask(node, out var nodeMask))
                    {
                        mask |= nodeMask;
                    }
                }

                // add way to output(s).
                foreach(var wayTile in tile.SubTilesForMask2(mask))
                {
                    // is tile a sub tile.
                    if (!subTiles.TryGetValue(wayTile.LocalId, out var stream))
                    {
                        continue;
                    }

                    // initialize stream if needed.
                    if (stream == null)
                    {
                        stream = SnapshotDbOperations.CreateTile(path, OsmGeoType.Way, wayTile);
                        subTiles[wayTile.LocalId] = stream;
                    }

                    // write way.
                    stream.Append(w);
                }
                
                // add way to index.
                wayIndex.Add(w.Id.Value, mask);
            } while (source.MoveNext());

            // flush/dispose all sub tile streams.
            foreach (var subTile in subTiles)
            {
                if (subTile.Value == null) continue;
                subTile.Value.Flush();
                subTile.Value.Dispose();
            }

            return (wayIndex, hasNext, timestamp);
        }
    }
}