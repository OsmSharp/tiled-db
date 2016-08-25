using Anyways.Osm.TiledDb.Collections;
using Anyways.Osm.TiledDb.IO.PBF;
using OsmSharp;
using OsmSharp.Streams;
using System.IO;
using System.Linq;

namespace Anyways.Osm.TiledDb.Splitter
{
    public class Program
    {
        public static void Main(string[] args)
        {
            /*
             * Split pbf into tiles:
             * --split-pbf zoom=4 C:\work\anyways\data\tiled-db-tests\belgium-highways.osm.pbf C:\work\anyways\data\tiled-db-tests\4
             * 
             * Split tiles into subtiles:
             * --split-tiles zoom=5 C:\work\anyways\data\tiled-db-tests\4 C:\work\anyways\data\tiled-db-tests\5
             * 
             */

            // enable logging.
            OsmSharp.Logging.Logger.LogAction = (o, level, message, parameters) =>
            {
                System.Console.WriteLine(string.Format("[{0}] {1} - {2}", o, level, message));
            };

            if (args[0] == "--split-pbf")
            {
                var zoomArgs = args[1].Split('=');
                var zoom = int.Parse(zoomArgs[1]);

                var inputFile = args[2];
                var outputPath = args[3];

                SplitPbf.Run(inputFile, outputPath, zoom);
            }
            if (args[0] == "--split-pbf-recursive")
            {
                var zoomArgs = args[1].Split('=');
                var zoom = int.Parse(zoomArgs[1]);

                var inputFile = args[2];
                var outputPath = args[3];

                SplitPbf.RunRecursive(inputFile, outputPath, zoom);
            }
            else if (args[0] == "--split-tiles")
            {
                var zoomArgs = args[1].Split('=');
                var zoom = int.Parse(zoomArgs[1]);

                var inputFile = args[2];
                var outputPath = args[3];

                SplitTiles.RunSplitTiles(inputFile, outputPath, zoom);
            }
            else if (args[0] == "--to-osm-xml")
            {
                var inputFile = args[1];
                var outputPath = args[2];

                Convert.ToOsmXml(inputFile, outputPath);
            }
        }
    }
}
