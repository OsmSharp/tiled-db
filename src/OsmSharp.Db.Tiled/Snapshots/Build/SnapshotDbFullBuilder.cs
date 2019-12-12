using System;
using System.Collections.Generic;
using OsmSharp.Db.Tiled.Indexes;
using OsmSharp.Db.Tiled.Tiles;
using OsmSharp.Streams;
using Serilog;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.Snapshots.IO;

namespace OsmSharp.Db.Tiled.Snapshots.Build
{
    /// <summary>
    /// Builds an OSM snapshot db from an OSM stream.
    /// </summary>
    internal static class SnapshotDbFullBuilder
    {
        /// <summary>
        /// Builds a new database and write the structure to the given path.
        /// </summary>
        /// <param name="source">The source stream.</param>
        /// <param name="path">The path to store the db at.</param>
        /// <param name="maxZoom">The maximum zoom.</param>
        public static SnapshotDbFull Build(this OsmStreamSource source, string path, uint maxZoom = 12)
        {
            if (source == null) { throw new ArgumentNullException(nameof(source)); }
            if (path == null) { throw new ArgumentNullException(nameof(path)); }
            if (!source.CanReset) { throw new ArgumentException("Source cannot be reset."); }
            if (!FileSystemFacade.FileSystem.DirectoryExists(path)) { throw new ArgumentException("Output path does not exist."); }
            
            // first build the data structure on disk.
            Log.Logger.Information("Building for tile {0}/{1}/{2}...", 0, 0, 0);
            var (tiles, timestamp) = BuildInitial(source, path, maxZoom, new Tile(0, 0, 0));
            while (true)
            {
                var newTiles = new List<Tile>();

                var tiles1 = tiles;
                System.Threading.Tasks.Parallel.For(0, tiles.Count, (t) =>
                {
                    var subTile = tiles1[t];
                    Log.Logger.Information($"Building for tile ({t + 1}/{tiles1.Count}):{subTile.Zoom}/{subTile.X}/{subTile.Y}...");
                    var subTiles = Build(path, maxZoom, subTile);

                    lock (newTiles)
                    {
                        newTiles.AddRange(subTiles);
                    }
                });

                if (newTiles.Count == 0)
                {
                    break;
                }

                tiles = newTiles;
            }

            // save the meta-data.
            var dbMeta = new SnapshotDbMeta
            {
                Base = null, // this is a full db.
                Type = SnapshotDbType.Full,
                Zoom = maxZoom,
                Timestamp = timestamp
            };
            SnapshotDbOperations.SaveDbMeta(path, dbMeta);
            
            return new SnapshotDbFull(path);
        }

        private static (List<Tile> tiles, DateTime timestamp) BuildInitial(OsmStreamSource source, string path, uint maxZoom, Tile tile)
        {
            // split nodes and return nodes index and non-empty tiles.
            var (nodeIndex, nonEmptyTiles, hasNext, timestamp) = NodeProcessor.Process(source, path, maxZoom, tile);

            // split ways using the node index and return the way index.
            Index wayIndex = null;
            DateTime localTimestamp;
            if (hasNext)
            {
                (wayIndex, hasNext, localTimestamp) = WayProcessor.Process(source, path, maxZoom, tile, nodeIndex);
                if (localTimestamp > timestamp) timestamp = localTimestamp;
            }

            // split relations using the node and way index and return the relation index.
            Index relationIndex = null;
            if (hasNext)
            {
                (relationIndex, localTimestamp) =
                    RelationProcessor.Process(source, path, maxZoom, tile, nodeIndex, wayIndex, true);
                if (localTimestamp > timestamp) timestamp = localTimestamp;
            }

            // write the indices to disk.
            nodeIndex.Write(SnapshotDbOperations.PathToIndex(path, OsmGeoType.Node, tile));
            wayIndex?.Write(SnapshotDbOperations.PathToIndex(path, OsmGeoType.Way, tile));
            relationIndex?.Write(SnapshotDbOperations.PathToIndex(path, OsmGeoType.Relation, tile));
            
//            var actions = new List<Action>
//            {
//                () => nodeIndex.Write(SnapshotDbOperations.PathToIndex(path, OsmGeoType.Node, tile)),
//                () => wayIndex?.Write(SnapshotDbOperations.PathToIndex(path, OsmGeoType.Way, tile)),
//                () => relationIndex?.Write(SnapshotDbOperations.PathToIndex(path, OsmGeoType.Relation, tile))
//            };
//            System.Threading.Tasks.Parallel.ForEach(actions, (a) => a());
            
            return (nonEmptyTiles, timestamp);
        }
        
        private static IEnumerable<Tile> Build(string path, uint maxZoom, Tile tile)
        {
            // split nodes and return index and non-empty tiles.
            List<Tile> nonEmptyTiles = null;
            Index nodeIndex = null;
            
            var nodeFile = SnapshotDbOperations.PathToTile(path, OsmGeoType.Node, tile);
            if (!FileSystemFacade.FileSystem.Exists(nodeFile))
            {
                Log.Logger.Warning("Tile {0}/{1}/{2} not found: {3}", tile.Zoom, tile.X, tile.Y,
                    nodeFile);
                return new List<Tile>();
            }
            using (var nodeStream = SnapshotDbOperations.LoadTile(path, OsmGeoType.Node, tile))
            {
                var nodeSource = new OsmSharp.Streams.BinaryOsmStreamSource(nodeStream);

                // split nodes and return nodes index and non-empty tiles.
                (nodeIndex, nonEmptyTiles, _, _) = NodeProcessor.Process(nodeSource, path, maxZoom, tile);
            }

            // build the ways index.
            Index wayIndex = null;
            var wayFile = SnapshotDbOperations.PathToTile(path, OsmGeoType.Way, tile);
            if (FileSystemFacade.FileSystem.Exists(wayFile))
            {
                using (var wayStream = SnapshotDbOperations.LoadTile(path, OsmGeoType.Way, tile))
                {
                    var waySource = new OsmSharp.Streams.BinaryOsmStreamSource(wayStream);
                    if (waySource.MoveNext())
                    {
                        (wayIndex, _, _) = WayProcessor.Process(waySource, path, maxZoom, tile, nodeIndex);
                    }
                }
            }  

            // build the relations index.
            Index relationIndex = null;
            var relationFile = SnapshotDbOperations.PathToTile(path, OsmGeoType.Relation, tile);
            if (FileSystemFacade.FileSystem.Exists(relationFile))
            {
                using (var relationStream = SnapshotDbOperations.LoadTile(path, OsmGeoType.Relation, tile))
                {
                    var relationSource = new OsmSharp.Streams.BinaryOsmStreamSource(relationStream);
                    if (relationSource.MoveNext())
                    {
                        (relationIndex, _) = RelationProcessor.Process(relationSource, path, maxZoom, tile, nodeIndex, wayIndex);
                    }
                }
            }

            // write the indexes to disk.
            var actions = new List<Action>
            {
                () => nodeIndex.Write(SnapshotDbOperations.PathToIndex(path, OsmGeoType.Node, tile)),
                () => wayIndex?.Write(SnapshotDbOperations.PathToIndex(path, OsmGeoType.Way, tile)),
                () => relationIndex?.Write(SnapshotDbOperations.PathToIndex(path, OsmGeoType.Relation, tile))
            };
            System.Threading.Tasks.Parallel.ForEach(actions, (a) => a());

            if (FileSystemFacade.FileSystem.Exists(nodeFile))
            {
                FileSystemFacade.FileSystem.Delete(nodeFile);
            }
            if (FileSystemFacade.FileSystem.Exists(wayFile))
            {
                FileSystemFacade.FileSystem.Delete(wayFile);
            }
            if (FileSystemFacade.FileSystem.Exists(relationFile))
            {
                FileSystemFacade.FileSystem.Delete(relationFile);
            }

            return nonEmptyTiles;
        }
    }
}