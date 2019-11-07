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
    /// The node processor.
    /// </summary>
    internal static class NodeProcessor
    {
        /// <summary>
        /// Processes the nodes in the given stream until the first on-node object is reached.
        /// </summary>
        /// <param name="source">The source stream.</param>
        /// <param name="path">The based path of the db.</param>
        /// <param name="maxZoom">The maximum zoom.</param>
        /// <param name="tile">The tile being split.</param>
        /// <returns>The indexed node id's with a masked zoom, a list of non-empty tiles, a boolean to indicate next data and the latest timestamp.</returns>
        public static (Index index, List<Tile> nonEmptyTiles, bool hasNext, DateTime timestamp) Process(OsmStreamSource source, string path, uint maxZoom, Tile tile)
        {            
            // build the set of possible sub tiles.
            var subTiles = new Dictionary<ulong, Stream>();
            foreach (var subTile in tile.GetSubtilesAt(tile.Zoom + 2))
            {
                subTiles.Add(subTile.LocalId, null);
            }

            // go over all nodes.
            var nodeIndex = new Index();
            var timestamp = DateTime.MinValue;
            var hasNext = false;
            while (source.MoveNext())
            {
                var current = source.Current();
                if (current.Type != OsmGeoType.Node)
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
                var n = (current as Node);
                var nodeTile = Tiles.Tile.WorldToTileIndex(n.Latitude.Value, n.Longitude.Value, tile.Zoom + 2);

                // is tile a subtile.
                if (!subTiles.TryGetValue(nodeTile.LocalId, out var stream))
                {
                    continue;
                }

                // initialize stream if needed.
                if (stream == null)
                {
                    stream = SnapshotDbOperations.CreateTile(path, OsmGeoType.Node, nodeTile);
                    subTiles[nodeTile.LocalId] = stream;
                }

                // write node.
                stream.Append(n);

                // add node to index.
                nodeIndex.Add(n.Id.Value, nodeTile.BuildMask2());
            }

            // flush/dispose all sub tile streams.
            // keep all non-empty tiles.
            var nonEmptyTiles = new List<Tile>();
            foreach (var subTile in subTiles)
            {
                if (subTile.Value == null) continue;
                subTile.Value.Flush();
                subTile.Value.Dispose();

                if (tile.Zoom + 2 < maxZoom)
                {
                    nonEmptyTiles.Add(Tile.FromLocalId(tile.Zoom + 2, subTile.Key));
                }
            }

            return (nodeIndex, nonEmptyTiles, hasNext, timestamp);
        }
    }
}