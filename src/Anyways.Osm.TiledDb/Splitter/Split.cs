using Anyways.Osm.TiledDb.Collections;
using Anyways.Osm.TiledDb.Indexing;
using Anyways.Osm.TiledDb.IO.Binary;
using Anyways.Osm.TiledDb.IO.PBF;
using Anyways.Osm.TiledDb.Tiles;
using OsmSharp;
using OsmSharp.Streams;
using Reminiscence.Arrays;
using System;
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
        private static int DIFF = 3;

        /// <summary>
        /// Splits the data in the given source into tiles at the given zoom level recursively.
        /// </summary>
        public static void RunRecursive(OsmStreamSource source, int zoom, string outputPath)
        {
            RunRecursive(source, zoom, new Tile(0, 0, 0), outputPath);
        }

        /// <summary>
        /// Splits the data in the given source into tiles at the given zoom level recursively.
        /// </summary>
        public static void RunRecursive(OsmStreamSource source, int zoom, Tile tile, string outputPath)
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

            var existingTiles = Directory.EnumerateFiles(tileOutputPath, "*.osm.bin");
            foreach(var existingTile in existingTiles)
            {
                File.Delete(existingTile);
            }

            OsmSharp.Logging.Logger.Log("Split", OsmSharp.Logging.TraceEventType.Information, "Splitting tile {2} - {0} into {1}...", tile.ToInvariantString(), nextZoom, tile.Id);
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
                    var filteredBinarySource = new OsmSharp.Streams.Filters.OsmStreamFilterProgress();
                    filteredBinarySource.RegisterSource(binarySource);
                    RunRecursive(filteredBinarySource, zoom, new Tile(tileFile.Key), outputPath);
                }
            }
        }

        /// <summary>
        /// Splits the data in the given source into tiles at the given zoom level. Includes only the tiles in the includes list if any.
        /// </summary>
        public static Dictionary<ulong, string> Run(OsmStreamSource source, int zoom, string outputPath, HashSet<ulong> tilesToInclude)
        {
            var nodes = new OneToOneIdMap();
            var ways = new OneToManyIdMap();
            //var relations = new IdMap();

            var tileFiles = new Dictionary<ulong, string>();

            //var undeterminableRelations = new HashSet<long>(); // holds id's of relations that cannot be placed in a tile ever.
            //var determinedRelations = new HashSet<long>(); // holds id's of relations, where all members are either undeterminable or also fully determined.
            //var relationIds = new HashSet<long>(); // holds the id's of all relations.

            // first loop.
            var count = 0;
            foreach (var osmGeo in source)
            {
                if (osmGeo.Type == OsmGeoType.Node)
                {
                    var node = (osmGeo as Node);

                    var tile = Tiles.Tile.CreateAroundLocation(
                        node.Latitude.Value, node.Longitude.Value, zoom).Id;

                    if (tilesToInclude.Contains(tile))
                    {
                        nodes.Add(node.Id.Value, tile);
                    }
                }
                else if (osmGeo.Type == OsmGeoType.Way)
                {
                    var way = (osmGeo as Way);

                    if (way.Nodes != null)
                    {
                        var tiles = new HashSet<ulong>();
                        for (var i = 0; i < way.Nodes.Length; i++)
                        {
                            var tile = nodes.Get(way.Nodes[i]);
                            if (tile != ulong.MaxValue)
                            {
                                tiles.Add(tile);
                            }
                        }
                        ways.Add(way.Id.Value, tiles.ToArray());
                    }
                }
                else if (osmGeo.Type == OsmGeoType.Relation)
                {
                    //var relation = (osmGeo as Relation);

                    //relationIds.Add(relation.Id.Value);

                    //var hasRelationMember = false;
                    //var hasPositiveCount = false;
                    //if (relation.Members != null)
                    //{
                    //    for (var i = 0; i < relation.Members.Length; i++)
                    //    {
                    //        var member = relation.Members[i];
                    //        switch (member.Type)
                    //        {
                    //            case OsmGeoType.Node:
                    //                count = 1;
                    //                var tile = nodes.Get(relation.Members[i].Id);
                    //                byte tileId;
                    //                if (!tilesDictionary.TryGetValue(tile, out tileId))
                    //                {
                    //                    count = 0;
                    //                }
                    //                else
                    //                {
                    //                    tiles[0] = tileId;
                    //                }
                    //                break;
                    //            case OsmGeoType.Way:
                    //                count = ways.Get(relation.Members[i].Id, ref tiles);
                    //                break;
                    //            case OsmGeoType.Relation:
                    //                hasRelationMember = true;
                    //                break;
                    //        }
                    //        if (count > 0)
                    //        { // when one of the members was found, this relation is never undeterminable.
                    //            hasPositiveCount = true;
                    //        }
                    //        for (var t = 0; t < count; t++)
                    //        {
                    //            //if (tilesToInclude == null ||
                    //            //    tilesToInclude.Contains(tiles[t]))
                    //            //{
                    //                relations.Add(relation.Id.Value, tiles[t]);
                    //            //}
                    //        }
                    //    }
                    //}
                    //if (!hasRelationMember)
                    //{ // we can only say something at the point when there are no relation members.
                    //    if (hasPositiveCount)
                    //    { // all members are nodes and ways and at least one was found, things can't get better.
                    //        determinedRelations.Add(relation.Id.Value);
                    //    }
                    //    else
                    //    { // all members are nodes and ways but none of them were found, things can't get any worse.
                    //        undeterminableRelations.Add(relation.Id.Value);
                    //    }
                    //}
                }
            }

            // TODO: detect and prevent circular references.
            //// relation loops.
            //var unknownDetected = true;
            //while (unknownDetected)
            //{
            //    source.Reset();
            //    unknownDetected = false;
            //    while (source.MoveNext(true, true, false))
            //    {
            //        var relation = source.Current() as Relation;
            //        if (!undeterminableRelations.Contains(relation.Id.Value) &&
            //            !determinedRelations.Contains(relation.Id.Value))
            //        {
            //            if (relation.Members != null)
            //            {
            //                var hasUndeterminedMember = false;
            //                for (var i = 0; i < relation.Members.Length; i++)
            //                {
            //                    count = 0;

            //                    var member = relation.Members[i];
            //                    var memberId = relation.Members[i].Id;
            //                    switch (member.Type)
            //                    {
            //                        case OsmGeoType.Relation:
            //                            if (undeterminableRelations.Contains(memberId))
            //                            { // relation is undetermined, nothing we can do.

            //                            }
            //                            else if (determinedRelations.Contains(memberId))
            //                            { // relation is determined, check where it is.
            //                                count = relations.Get(relation.Members[i].Id, ref tiles);
            //                            }
            //                            else if (!relationIds.Contains(memberId))
            //                            { // relation is not in the source, nothing we can do.

            //                            }
            //                            else
            //                            { // member is not determined yet, we need another loop.
            //                                unknownDetected = true;
            //                                hasUndeterminedMember = true;
            //                            }
            //                            break;
            //                    }
            //                    for (var t = 0; t < count; t++)
            //                    {
            //                        if (tilesToInclude == null ||
            //                            tilesToInclude.Contains(tiles[t]))
            //                        {
            //                            relations.Add(relation.Id.Value, tiles[t]);
            //                        }
            //                    }
            //                }
            //                var hasPosition = relations.Get(relation.Id.Value, ref tiles) > 0;
            //                if (!hasUndeterminedMember)
            //                { // with no undetermined members, a descision has to be made.
            //                    if (hasPosition)
            //                    { // there are no undetermined members but there are positions so relation is determined.
            //                        determinedRelations.Add(relation.Id.Value);
            //                    }
            //                    else
            //                    { // there are no undetermined members but there are no positions so relation is undetermined.
            //                        undeterminableRelations.Add(relation.Id.Value);
            //                    }
            //                }
            //            }
            //        }
            //    }
            //}


            //// iterate once more and split into tiles.
            //var outputTileIds = new ulong[1024];
            //count = 0;
            //var streamCache = new LRUCache<ulong, Stream>(1024);
            //streamCache.OnRemove += (s) =>
            //{
            //    s.Flush();
            //    s.Dispose();
            //};
            //var output = new DirectoryInfo(outputPath);
            //if (!output.Exists)
            //{
            //    output.Create();
            //}
            //foreach (var osmGeo in source)
            //{
            //    switch (osmGeo.Type)
            //    {
            //        case OsmGeoType.Node:
            //            count = 1;
            //            outputTileIds[0] = nodes.Get(osmGeo.Id.Value);
            //            if (outputTileIds[0] == ulong.MaxValue)
            //            {
            //                count = 0;
            //            }
            //            break;
            //        case OsmGeoType.Way:
            //            count = ways.Get(osmGeo.Id.Value, ref tileIds);
            //            break;
            //        case OsmGeoType.Relation:
            //            count = relations.Get(osmGeo.Id.Value, ref tileIds);
            //            break;
            //    }

            //    for (var i = 0; i < count; i++)
            //    {
            //        var tile = tilesToInclude[tileIds[i]];

            //        Stream stream;
            //        if (!streamCache.TryGet(tile, out stream))
            //        {
            //            var path = Path.Combine(output.FullName, tile.ToString() + ".osm.bin");
            //            tileFiles[tile] = path;
            //            stream = File.Open(path, FileMode.Append);
            //            streamCache.Add(tile, stream);
            //        }

            //        switch (osmGeo.Type)
            //        {
            //            case OsmGeoType.Node:
            //                stream.Append(osmGeo as Node);
            //                break;
            //            case OsmGeoType.Way:
            //                stream.Append(osmGeo as Way);
            //                break;
            //            case OsmGeoType.Relation:
            //                stream.Append(osmGeo as Relation);
            //                break;
            //        }
            //    }
            //}

            //foreach (var writerAndStream in streamCache)
            //{
            //    writerAndStream.Value.Flush();
            //    writerAndStream.Value.Dispose();
            //}

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
