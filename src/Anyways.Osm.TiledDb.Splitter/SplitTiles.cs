using Anyways.Osm.TiledDb.IO.Binary;
using Anyways.Osm.TiledDb.Tiles;
using System;
using System.Collections.Generic;
using System.IO;

namespace Anyways.Osm.TiledDb.Splitter
{
    static class SplitTiles
    {
        public static void RunSplitTiles(string inputPath, string outputPath, int zoom)
        {
            var files = new DirectoryInfo(inputPath);
            foreach (var file in files.GetFiles("*.osm.bin"))
            {
                SplitTiles.RunSplitTile(file.FullName, outputPath, zoom);
            }
        }

        public static void RunSplitTile(string inputFile, string outputPath, int zoom)
        {
            var tileId = ulong.Parse((new FileInfo(inputFile)).Name.GetNameUntilFirstDot());

            using (var inputFileStream = File.OpenRead(inputFile))
            {
                var source = new BinaryOsmStreamSource(inputFileStream);

                var progress = new OsmSharp.Streams.Filters.OsmStreamFilterProgress();
                progress.RegisterSource(source);

                var tile = new Tile(tileId);
                
                if (tile.Zoom >= zoom)
                {
                    OsmSharp.Logging.Logger.Log("SplitTiles", OsmSharp.Logging.TraceEventType.Critical,
                        "Cannot split a tile at zoom {0} for tiles at zoom {1}.", tile.Zoom, zoom);
                    return;
                }

                var tileRange = tile.GetSubTiles(zoom);
                var tilesToInclude = new HashSet<ulong>();
                foreach(var tileToInclude in tileRange)
                {
                    tilesToInclude.Add(tileToInclude.Id);
                }

                Split.Run(progress, zoom, outputPath, tilesToInclude);
            }
        }
    }
}
