using Anyways.Osm.TiledDb.Collections;
using Anyways.Osm.TiledDb.IO.Binary;
using Anyways.Osm.TiledDb.IO.PBF;
using Anyways.Osm.TiledDb.Tiles;
using OsmSharp;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Anyways.Osm.TiledDb.Splitter
{
    /// <summary>
    /// Contains functionality to split an OSM data stream into tiles.
    /// </summary>
    public static class Split
    {
        private static int DIFF = 4;

        /// <summary>
        /// Splits the data in the given source into tiles at the given zoom level recursively.
        /// </summary>
        public static void RunRecursive(IEnumerable<OsmGeo> source, int zoom, string outputPath)
        {
            RunRecursive(source, zoom, new Tile(0, 0, 0), outputPath);
        }

        /// <summary>
        /// Splits the data in the given source into tiles at the given zoom level recursively.
        /// </summary>
        public static void RunRecursive(IEnumerable<OsmGeo> source, int zoom, Tile tile, string outputPath)
        {
            var tilesToInclude = new HashSet<ulong>();
            var nextZoom = tile.Zoom + DIFF;
            if (nextZoom > zoom)
            {
                nextZoom = zoom;
            }
            foreach(var subTile in tile.GetSubTiles(nextZoom))
            {
                tilesToInclude.Add(subTile.Id);
            }
            var tileOutputPath = Path.Combine(outputPath, nextZoom.ToInvariantString());
            if (!Directory.Exists(tileOutputPath))
            {
                Directory.CreateDirectory(tileOutputPath);
            }

            OsmSharp.Logging.Logger.Log("Split", OsmSharp.Logging.TraceEventType.Information, "Splitting tile {0} into {1}...", tile.ToInvariantString(), nextZoom);
            var tileFiles = Run(source, nextZoom, tileOutputPath, tilesToInclude);

            if (nextZoom == zoom)
            {
                return;
            }

            foreach(var tileFile in tileFiles)
            {
                using (var stream = File.OpenRead(tileFile.Value))
                {
                    var binarySource = new BinaryOsmStreamSource(stream);
                    RunRecursive(binarySource, zoom, new Tile(tileFile.Key), outputPath);
                }
            }
        }

        /// <summary>
        /// Splits the data in the given source into tiles at the given zoom level. Includes only the tiles in the includes list if any.
        /// </summary>
        public static Dictionary<ulong, string> Run(IEnumerable<OsmGeo> source, int zoom, string outputPath, HashSet<ulong> tilesToInclude = null)
        {
            var nodes = new IdMap();
            var ways = new IdMap();
            var relations = new IdMap();
            var tileFiles = new Dictionary<ulong, string>();

            var tiles = new ulong[256];
            var count = 0;

            foreach (var osmGeo in source)
            {
                if (osmGeo.Type == OsmSharp.OsmGeoType.Node)
                {
                    var node = (osmGeo as Node);
                    var tile = Tiles.Tile.CreateAroundLocation(
                        node.Latitude.Value, node.Longitude.Value, zoom).Id;

                    if (tilesToInclude == null ||
                        tilesToInclude.Contains(tile))
                    {
                        nodes.Add(node.Id.Value, tile);
                    }
                }
                else if (osmGeo.Type == OsmGeoType.Way)
                {
                    var way = (osmGeo as Way);

                    if (way.Nodes != null)
                    {
                        for (var i = 0; i < way.Nodes.Length; i++)
                        {
                            count = nodes.Get(way.Nodes[i], ref tiles);
                            for (var t = 0; t < count; t++)
                            {
                                if (tilesToInclude == null ||
                                    tilesToInclude.Contains(tiles[t]))
                                {
                                    ways.Add(way.Id.Value, tiles[t]);
                                }
                            }
                        }
                    }
                }
                else if (osmGeo.Type == OsmGeoType.Relation)
                {
                    var relation = (osmGeo as Relation);

                    if (relation.Members != null)
                    {
                        for (var i = 0; i < relation.Members.Length; i++)
                        {
                            var member = relation.Members[i];
                            switch (member.Type)
                            {
                                case OsmGeoType.Node:
                                    count = nodes.Get(relation.Members[i].Id, ref tiles);
                                    break;
                                case OsmGeoType.Way:
                                    count = ways.Get(relation.Members[i].Id, ref tiles);
                                    break;
                                case OsmGeoType.Relation:
                                    break;
                            }
                            for (var t = 0; t < count; t++)
                            {
                                if (tilesToInclude == null ||
                                    tilesToInclude.Contains(tiles[t]))
                                {
                                    relations.Add(relation.Id.Value, tiles[t]);
                                }
                            }
                        }
                    }
                }
            }

            var streamCache = new LRUCache<ulong, Stream>(1024);
            streamCache.OnRemove += (s) =>
            {
                s.Flush();
                s.Dispose();
            };
            var output = new DirectoryInfo(outputPath);
            if (!output.Exists)
            {
                output.Create();
            }
            foreach (var osmGeo in source)
            {
                switch (osmGeo.Type)
                {
                    case OsmGeoType.Node:
                        count = nodes.Get(osmGeo.Id.Value, ref tiles);
                        break;
                    case OsmGeoType.Way:
                        count = ways.Get(osmGeo.Id.Value, ref tiles);
                        break;
                    case OsmGeoType.Relation:
                        count = relations.Get(osmGeo.Id.Value, ref tiles);
                        break;
                }

                for (var i = 0; i < count; i++)
                {
                    var tile = tiles[i];

                    Stream stream;
                    if (!streamCache.TryGet(tile, out stream))
                    {
                        var path = Path.Combine(output.FullName, tile.ToString() + ".osm.bin");
                        tileFiles[tile] = path;
                        stream = File.Open(path, FileMode.Append);
                        streamCache.Add(tile, stream);
                    }

                    switch (osmGeo.Type)
                    {
                        case OsmGeoType.Node:
                            stream.Append(osmGeo as Node);
                            break;
                        case OsmGeoType.Way:
                            stream.Append(osmGeo as Way);
                            break;
                        case OsmGeoType.Relation:
                            stream.Append(osmGeo as Relation);
                            break;
                    }
                }
            }

            foreach (var writerAndStream in streamCache)
            {
                writerAndStream.Value.Flush();
                writerAndStream.Value.Dispose();
            }

            return tileFiles;
        }

        /// <summary>
        /// Compresses all tiled files in the given path.
        /// </summary>
        public static void CompressAll(string path)
        {
            var files = Directory.EnumerateFiles(path, "*.osm.bin");
            foreach(var file in files)
            {
                var zippedFile = file + ".zip";
                using (var source = File.OpenRead(file))
                using (var target = new System.IO.Compression.GZipStream(File.Open(zippedFile, FileMode.Create), System.IO.Compression.CompressionLevel.Fastest))
                {
                    source.CopyTo(target);
                }
            }
        }
    }
}
