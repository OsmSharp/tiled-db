//using System.IO;
//using OsmSharp.Db.Tiled.IO;
//
//namespace OsmSharp.Db.Tiled.Indexes
//{
//    internal static class OsmGeoKeyIndexExtensions
//    {
//        public static void Write(this OsmGeoKeyIndex index, string file)
//        {
//            var directory = FileSystemFacade.FileSystem.DirectoryForFile(file);
//            if (!FileSystemFacade.FileSystem.DirectoryExists(directory))
//            {
//                FileSystemFacade.FileSystem.CreateDirectory(directory);
//            }
//            
//            using var stream = FileSystemFacade.FileSystem.Open(file, FileMode.Create);
//            index.Serialize(stream);
//        }
//    }
//}