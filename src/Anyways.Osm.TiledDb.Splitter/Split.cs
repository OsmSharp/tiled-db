using Anyways.Osm.TiledDb.Collections;
using Anyways.Osm.TiledDb.IO.PBF;
using OsmSharp;
using OsmSharp.Streams;
using System.IO;
using System.Linq;

namespace Anyways.Osm.TiledDb.Splitter
{
    static class Split
    {
        public static void Run(OsmStreamSource source, int zoom, string outputPath)
        {
            var nodes = new IdMap();
            var ways = new IdMap();
            var relations = new IdMap();

            var tiles = new ulong[256];
            var count = 0;

            foreach (var osmGeo in source)
            {
                if (osmGeo.Type == OsmSharp.OsmGeoType.Node)
                {
                    var node = (osmGeo as Node);
                    var tile = Tiles.Tile.CreateAroundLocation(
                        node.Latitude.Value, node.Longitude.Value, zoom).Id;

                    nodes.Add(node.Id.Value, tile);
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
                                ways.Add(way.Id.Value, tiles[t]);
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
                                relations.Add(relation.Id.Value, tiles[t]);
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
        }
    }
}
