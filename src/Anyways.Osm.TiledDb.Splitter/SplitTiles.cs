using Anyways.Osm.TiledDb.IO.Binary;
using System.IO;

namespace Anyways.Osm.TiledDb.Splitter
{
    static class SplitTiles
    {
        public static void RunSplitTiles(string inputPath, string outputPath, int zoom)
        {
            var files = new DirectoryInfo(inputPath);
            foreach(var file in files.GetFiles("*.osm.bin"))
            {
                ulong tileId;
                if (ulong.TryParse(file.Name, out tileId))
                {
                    SplitTiles.RunSplitTile(file.FullName, outputPath, zoom);
                }
            }
        }

        public static void RunSplitTile(string inputFile, string outputPath, int zoom)
        {
            var tileId = ulong.Parse((new FileInfo(inputFile)).Name);

            using (var inputFileStream = File.OpenRead(inputFile))
            {
                var source = new BinaryOsmStreamSource(inputFileStream);

                var progress = new OsmSharp.Streams.Filters.OsmStreamFilterProgress();
                progress.RegisterSource(source);

                Split.Run(progress, zoom, outputPath);
            }
        }
    }
}
