//using OsmSharp.Streams;
//using System.IO;

//namespace Anyways.Osm.TiledDb.Splitter
//{
//    static class SplitPbf
//    {
//        public static void Run(string inputFile, string outputPath, int zoom)
//        {
//            var source = new OsmSharp.Streams.Filters.OsmStreamFilterProgress();
//            source.RegisterSource(new PBFOsmStreamSource(File.OpenRead(inputFile)));

//            Split.Run(source, zoom, outputPath);
//        }

//        public static void RunRecursive(string inputFile, string outputPath, int zoom)
//        {
//            var diff = 4;
//            var initialZoom = 6;

//            var zoomOutputPath = Path.Combine(outputPath + @"\" + initialZoom.ToString());
//            if (!Directory.Exists(zoomOutputPath))
//            {
//                Directory.CreateDirectory(zoomOutputPath);
//            }

//            SplitPbf.Run(inputFile, zoomOutputPath, initialZoom);

//            initialZoom += diff;
//            var newZoomOutputPath = Path.Combine(outputPath + @"\" + initialZoom.ToString());
//            while (initialZoom < zoom)
//            {
//                newZoomOutputPath = Path.Combine(outputPath + @"\" + initialZoom.ToString());
//                if (!Directory.Exists(newZoomOutputPath))
//                {
//                    Directory.CreateDirectory(newZoomOutputPath);
//                }

//                SplitTiles.RunSplitTiles(zoomOutputPath, newZoomOutputPath, initialZoom);
//                zoomOutputPath = newZoomOutputPath;
//                initialZoom += diff;
//            }

//            newZoomOutputPath = Path.Combine(outputPath + @"\" + zoom.ToString());
//            if (!Directory.Exists(newZoomOutputPath))
//            {
//                Directory.CreateDirectory(newZoomOutputPath);
//            }

//            SplitTiles.RunSplitTiles(zoomOutputPath, newZoomOutputPath, zoom);
//        }
//    }
//}
