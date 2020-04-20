//using System;
//using OsmSharp.Db.Tiled.Indexes;
//using OsmSharp.Db.Tiled.IO;
//using OsmSharp.Db.Tiled.Snapshots.IO;
//using OsmSharp.IO.Binary;
//
//namespace OsmSharp.Db.Tiled.Snapshots.Build
//{
//    /// <summary>
//    /// Builds an OSM snapshot db snapshot from another snapshot db.
//    /// </summary>
//    internal static class SnapshotDbSnapshotBuilder
//    {
//        /// <summary>
//        /// Builds a new database and write the structure to the given path.
//        /// </summary>
//        /// <param name="snapshotDb">The snapshot db.</param>
//        /// <param name="path">The path to store the db at.</param>
//        public static SnapshotDb Build(this SnapshotDb snapshotDb, string path = null)
//        {
//            if (snapshotDb is SnapshotDbFull) { throw new ArgumentException("Cannot build a snapshot from a full db."); }
//            if (!(snapshotDb is SnapshotDbDiff snapshotDbDiff)) { throw new ArgumentException("Cannot build a snapshot from a full db."); }
//            
//            // creates a new database diff representing the given changes.
//            // create a target directory if one wasn't specified.
//            if (string.IsNullOrWhiteSpace(path))
//            {
//                path = FileSystemFacade.FileSystem.Combine(FileSystemFacade.FileSystem.ParentDirectory(snapshotDb.Path),
//                    $"diff-{DateTime.Now.ToTimestampPath()}-snapshot");
//            }
//            
//            // make sure path exists.
//            if (!FileSystemFacade.FileSystem.DirectoryExists(path))
//            {
//                FileSystemFacade.FileSystem.CreateDirectory(path);
//            }
//
//            // loop over all tiles and consolidate all the tiles that have been modified.
//            foreach (var tile in snapshotDb.GetChangedTilesSinceLatestDiff())
//            {
//                var tileData = snapshotDb.GetTile(tile.X, tile.Y, OsmGeoType.Node);
//                using (var tileStream = SnapshotDbOperations.CreateTile(path, OsmGeoType.Node, tile))
//                {
//                    foreach (var osmGeo in tileData)
//                    {
//                        tileStream.Append(osmGeo as Node);
//                    }
//                }
//                
//                tileData = snapshotDb.GetTile(tile.X, tile.Y, OsmGeoType.Way);
//                if (tileData != null)
//                {
//                    using (var tileStream = SnapshotDbOperations.CreateTile(path, OsmGeoType.Way, tile))
//                    {
//                        foreach (var osmGeo in tileData)
//                        {
//                            tileStream.Append(osmGeo as Way);
//                        }
//                    }
//                }
//
//                tileData = snapshotDb.GetTile(tile.X, tile.Y, OsmGeoType.Relation);
//                if (tileData != null)
//                {
//                    using (var tileStream = SnapshotDbOperations.CreateTile(path, OsmGeoType.Relation, tile))
//                    {
//                        foreach (var osmGeo in tileData)
//                        {
//                            tileStream.Append(osmGeo as Relation);
//                        }
//                    }
//                }
//            }
//            
//            // per zoom split:
//            // - merge indexes:
//            //   - use the latest data for a certain id.
//            //   - we don't care about deleted entries, leave them in.
//
//            for (uint zoom = 0; zoom < snapshotDb.Zoom; zoom += 2)
//            {
//                foreach (var tile in snapshotDb.GetIndexesForZoom(zoom))
//                {
//                    var index = new OsmGeoKeyIndex();
//                    var indexData = snapshotDb.GetSortedIndexData(tile);
//                    foreach (var (type, id, mask) in indexData)
//                    {
//                        index.Add(type, id, mask);
//                    }
//                    
//                    SnapshotDbOperations.SaveIndex(path, tile, index);
//                }
//            }
//
//            // save the meta-data.
//            var dbMeta = new SnapshotDbMeta
//            {
//                Base = snapshotDb.GetLatestNonDiff().Path,
//                Type = SnapshotDbType.Snapshot,
//                Zoom = snapshotDb.Zoom,
//                Timestamp = snapshotDb.Timestamp
//            };
//            SnapshotDbOperations.SaveDbMeta(path, dbMeta);
//            
//            return new SnapshotDbFull(path);
//        }
//    }
//}