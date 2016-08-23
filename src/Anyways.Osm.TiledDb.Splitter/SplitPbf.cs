using OsmSharp.Streams;
using System.IO;

namespace Anyways.Osm.TiledDb.Splitter
{
    static class SplitPbf
    {
        public static void Run(string inputFile, string outputPath, int zoom)
        {
            var source = new OsmSharp.Streams.Filters.OsmStreamFilterProgress();
            source.RegisterSource(new PBFOsmStreamSource(File.OpenRead(inputFile)));

            Split.Run(source, zoom, outputPath);
        }
    }
}
