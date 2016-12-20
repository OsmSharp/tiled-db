using OsmSharp.Streams;
using System.IO;

namespace Anyways.Osm.TiledDb.Tests.Functional
{
    public class Program
    {
        public static void Main(string[] args)
        {            
            // enable logging.
            OsmSharp.Logging.Logger.LogAction = (o, level, message, parameters) =>
            {
                System.Console.WriteLine(string.Format("[{0}] {1} - {2}", o, level, message));
            };
            
            var source = new OsmSharp.Streams.Filters.OsmStreamFilterProgress();
            source.RegisterSource(new PBFOsmStreamSource(File.OpenRead(@"C:\work\data\OSM\europe-latest.osm.pbf")));

            Anyways.Osm.TiledDb.Splitter.Split.RunRecursive(source, 9, @"C:\work\anyways\data\tiled-db-tests\output\");

            //Anyways.Osm.TiledDb.Splitter.Split.CompressAll(@"C:\work\anyways\data\tiled-db-tests\output\10\");
        }
    }
}
