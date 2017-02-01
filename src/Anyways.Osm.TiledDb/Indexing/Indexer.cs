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
            while (indexFiles.Count > 1)
            {
                OsmSharp.Logging.Logger.Log("Indexer.Build", OsmSharp.Logging.TraceEventType.Information,
                    "Merging index, {0} files left...", indexFiles.Count);

                OneToOneIdMap map1 = null;
                OneToOneIdMap map2 = null;
                using (var stream1 = File.OpenRead(indexFiles[0]))
                using (var stream2 = File.OpenRead(indexFiles[1]))
                {
                    map1 = OneToOneIdMap.Deserialize(stream1);
                    map2 = OneToOneIdMap.Deserialize(stream2);
                }

                var map = OneToOneIdMap.Merge(map1, map2);
                var mapFileName = Path.Combine(basePath, Guid.NewGuid().ToString() + ".node.idx");
                using (var stream = File.Open(mapFileName, FileMode.Create))
                {
                    map.Serialize(stream);
                }
                File.Delete(indexFiles[0]);
                File.Delete(indexFiles[1]);
                indexFiles.RemoveAt(0);
                indexFiles.RemoveAt(0);
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

                OneToManyIdMap map1 = null;
                OneToManyIdMap map2 = null;
                using (var stream1 = File.OpenRead(indexFiles[0]))
                using (var stream2 = File.OpenRead(indexFiles[1]))
                {
                    map1 = OneToManyIdMap.Deserialize(stream1);
                    map2 = OneToManyIdMap.Deserialize(stream2);
                }

                var map = OneToManyIdMap.Merge(map1, map2);
                var mapFileName = Path.Combine(basePath, Guid.NewGuid().ToString() + ".way.idx");
                using (var stream = File.Open(mapFileName, FileMode.Create))
                {
                    map.Serialize(stream);
                }
                File.Delete(indexFiles[0]);
                File.Delete(indexFiles[1]);
                indexFiles.RemoveAt(0);
                indexFiles.RemoveAt(0);
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

                OneToManyIdMap map1 = null;
                OneToManyIdMap map2 = null;
                using (var stream1 = File.OpenRead(indexFiles[0]))
                using (var stream2 = File.OpenRead(indexFiles[1]))
                {
                    map1 = OneToManyIdMap.Deserialize(stream1);
                    map2 = OneToManyIdMap.Deserialize(stream2);
                }

                var map = OneToManyIdMap.Merge(map1, map2);
                var mapFileName = Path.Combine(basePath, Guid.NewGuid().ToString() + ".relation.idx");
                using (var stream = File.Open(mapFileName, FileMode.Create))
                {
                    map.Serialize(stream);
                }
                File.Delete(indexFiles[0]);
                File.Delete(indexFiles[1]);
                indexFiles.RemoveAt(0);
                indexFiles.RemoveAt(0);
                indexFiles.Add(mapFileName);
            }
            indexFile = Path.Combine(basePath, "relations.idx");
            File.Delete(indexFile);
            File.Move(indexFiles[0], indexFile);
        }
    }
}
