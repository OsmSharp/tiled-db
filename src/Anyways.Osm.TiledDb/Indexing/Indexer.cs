using Anyways.Osm.TiledDb.Collections;
using OsmSharp.IO.Binary;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Anyways.Osm.TiledDb.Indexing
{
    /// <summary>
    /// Contains functionality related to the indexing.
    /// </summary>
    public static class Indexer
    {
        /// <summary>
        /// Builds an index for the tiles in the given path.
        /// </summary>
        public static void Build(string basePath)
        {
            // STEP1: build an index per tile.
            // make sure to delete all existing index files first.
            foreach (var file in Directory.EnumerateFiles(basePath, "*.idx"))
            {
                File.Delete(file);
            }
            var i = 0;
            Parallel.ForEach(Directory.EnumerateFiles(basePath, "*.osm.bin"), (file) =>
            //foreach (var file in Directory.EnumerateFiles(basePath, "*.osm.bin"))
            {
                var nodeIndex = new OneToOneIdMap();
                var wayIndex = new OneToManyIdMap();
                var relationIndex = new OneToManyIdMap();

                var fileName = Path.GetFileNameWithoutExtension(file);
                var dotIdx = fileName.IndexOf('.');
                if (dotIdx > 0)
                {
                    fileName = fileName.Substring(0, dotIdx);
                }
                ulong tileId;
                if (!ulong.TryParse(fileName, out tileId))
                { // oeps, filename is no a tileid, what's happening here!
                    OsmSharp.Logging.Logger.Log("Indexer.Build", OsmSharp.Logging.TraceEventType.Warning,
                        "A file that could be a tile detected but tile id could not be parsed: {0}.", file);
                    return;
                }

                // read data and build index.
                using (var stream = File.OpenRead(file))
                {
                    var source = new BinaryOsmStreamSource(stream);
                    foreach (var osmGeo in source)
                    {
                        switch (osmGeo.Type)
                        {
                            case OsmSharp.OsmGeoType.Node:
                                nodeIndex.Add(osmGeo.Id.Value, tileId);
                                break;
                            case OsmSharp.OsmGeoType.Way:
                                wayIndex.Add(osmGeo.Id.Value, tileId);
                                break;
                            case OsmSharp.OsmGeoType.Relation:
                                relationIndex.Add(osmGeo.Id.Value, tileId);
                                break;
                        }
                    }
                }

                // write indices to disk.
                using (var stream = File.Open(file + ".node.idx", FileMode.Create))
                {
                    nodeIndex.Serialize(stream);
                }
                using (var stream = File.Open(file + ".way.idx", FileMode.Create))
                {
                    wayIndex.Serialize(stream);
                }
                using (var stream = File.Open(file + ".relation.idx", FileMode.Create))
                {
                    relationIndex.Serialize(stream);
                }

                i++;
                OsmSharp.Logging.Logger.Log("Indexer.Build", OsmSharp.Logging.TraceEventType.Information,
                    "Indexed file # {1}: {0}.", file, i);
            });

            // STEP2: merge all indexes together.
            // STEP2.1: merge all node indexes together.
            var indexFiles = new List<string>(Directory.EnumerateFiles(basePath, "*.node.idx"));
            var maxMergeCount = 256;
            while (indexFiles.Count > 1)
            {
                OsmSharp.Logging.Logger.Log("Indexer.Build", OsmSharp.Logging.TraceEventType.Information,
                    "Merging index, {0} files left...", indexFiles.Count);

                var mergeCount = Math.Min(maxMergeCount, indexFiles.Count);
                var maps = new OneToOneIdMap[mergeCount];
                for (var m = 0; m < mergeCount; m++)
                {
                    using (var stream = File.OpenRead(indexFiles[m]))
                    {
                        maps[m] = OneToOneIdMap.Deserialize(stream);
                    }
                }

                //while (maps.Count > 1)
                //{
                //    var mergedMap = OneToOneIdMap.Merge(maps[0], maps[1]);
                //    maps.RemoveAt(0);
                //    maps.RemoveAt(0);
                //    maps.Add(mergedMap);
                //}

                var map = OneToOneIdMap.Merge(maps);
                var mapFileName = Path.Combine(basePath, Guid.NewGuid().ToString() + ".node.idx");
                using (var stream = File.Open(mapFileName, FileMode.Create))
                {
                    map.Serialize(stream);
                }
                for (var m = 0; m < mergeCount; m++)
                {
                    File.Delete(indexFiles[m]);
                }
                for (var m = 0; m < mergeCount; m++)
                {
                    indexFiles.RemoveAt(0);
                }
                indexFiles.Add(mapFileName);
            }
            var indexFile = Path.Combine(basePath, "nodes.idx");
            File.Delete(indexFile);
            File.Move(indexFiles[0], indexFile);
            // STEP2.2: merge all way indexes.
            indexFiles = new List<string>(Directory.EnumerateFiles(basePath, "*.way.idx"));
            while (indexFiles.Count > 1)
            {
                OsmSharp.Logging.Logger.Log("Indexer.Build", OsmSharp.Logging.TraceEventType.Information,
                    "Merging index, {0} files left...", indexFiles.Count);

                var maps = new List<OneToManyIdMap>();
                var mergeCount = Math.Min(maxMergeCount, indexFiles.Count);
                for (var m = 0; m < mergeCount; m++)
                {
                    using (var stream = File.OpenRead(indexFiles[m]))
                    {
                        maps.Add(OneToManyIdMap.Deserialize(stream));
                    }
                }

                while (maps.Count > 1)
                {
                    var mergedMap = OneToManyIdMap.Merge(maps[0], maps[1]);
                    maps.RemoveAt(0);
                    maps.RemoveAt(0);
                    maps.Add(mergedMap);
                }

                var map = maps[0];
                var mapFileName = Path.Combine(basePath, Guid.NewGuid().ToString() + ".way.idx");
                using (var stream = File.Open(mapFileName, FileMode.Create))
                {
                    map.Serialize(stream);
                }
                for (var m = 0; m < mergeCount; m++)
                {
                    File.Delete(indexFiles[m]);
                }
                for (var m = 0; m < mergeCount; m++)
                {
                    indexFiles.RemoveAt(0);
                }
                indexFiles.Add(mapFileName);
            }
            indexFile = Path.Combine(basePath, "ways.idx");
            File.Delete(indexFile);
            File.Move(indexFiles[0], indexFile);
            // STEP2.2: merge all relation indexes.
            indexFiles = new List<string>(Directory.EnumerateFiles(basePath, "*.relation.idx"));
            while (indexFiles.Count > 1)
            {
                OsmSharp.Logging.Logger.Log("Indexer.Build", OsmSharp.Logging.TraceEventType.Information,
                    "Merging index, {0} files left...", indexFiles.Count);

                var maps = new List<OneToManyIdMap>();
                var mergeCount = Math.Min(maxMergeCount, indexFiles.Count);
                for (var m = 0; m < mergeCount; m++)
                {
                    using (var stream = File.OpenRead(indexFiles[m]))
                    {
                        maps.Add(OneToManyIdMap.Deserialize(stream));
                    }
                }

                while (maps.Count > 1)
                {
                    var mergedMap = OneToManyIdMap.Merge(maps[0], maps[1]);
                    maps.RemoveAt(0);
                    maps.RemoveAt(0);
                    maps.Add(mergedMap);
                }

                var map = maps[0];
                var mapFileName = Path.Combine(basePath, Guid.NewGuid().ToString() + ".relation.idx");
                using (var stream = File.Open(mapFileName, FileMode.Create))
                {
                    map.Serialize(stream);
                }
                for (var m = 0; m < mergeCount; m++)
                {
                    File.Delete(indexFiles[m]);
                }
                for (var m = 0; m < mergeCount; m++)
                {
                    indexFiles.RemoveAt(0);
                }
                indexFiles.Add(mapFileName);
            }
            indexFile = Path.Combine(basePath, "relations.idx");
            File.Delete(indexFile);
            File.Move(indexFiles[0], indexFile);
        }
    }
}
