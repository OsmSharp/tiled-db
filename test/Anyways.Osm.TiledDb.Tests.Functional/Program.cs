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
            source.RegisterSource(new PBFOsmStreamSource(File.OpenRead(@"C:\work\anyways\data\tiled-db-tests\belgium-highways.osm.pbf")));

            //var target = new BinaryOsmStreamTarget(File.OpenWrite(@"C:\work\anyways\tiled-osm-db\belgium-highways.osm.bin"));
            //target.RegisterSource(source);
            //target.Pull();

            //Anyways.Osm.TiledDb.Splitter.Split.RunRecursive(source, 12, @"C:\work\anyways\data\tiled-db-tests\output\");

            Anyways.Osm.TiledDb.Splitter.Split.CompressAll(@"C:\work\anyways\data\tiled-db-tests\output\12\");
        }
    }
}
