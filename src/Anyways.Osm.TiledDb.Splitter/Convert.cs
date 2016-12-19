//using System.IO;

//namespace Anyways.Osm.TiledDb.Splitter
//{
//    public static class Convert
//    {
//        public static void ToOsmXml(string inputPath, string outputPath)
//        {
//            var files = new DirectoryInfo(inputPath);
//            foreach (var file in files.GetFiles("*.osm.bin"))
//            {
//                var outputFile = Path.Combine(outputPath, file.Name.GetNameUntilFirstDot() + ".osm");

//                using (var sourceStream = file.OpenRead())
//                using (var targetStream = File.OpenWrite(outputFile))
//                {
//                    var source = new IO.Binary.BinaryOsmStreamSource(sourceStream);
//                    var target = new OsmSharp.Streams.XmlOsmStreamTarget(targetStream);
//                    target.RegisterSource(source);
//                    target.Pull();
//                }
//            }
//        }
//    }
//}
