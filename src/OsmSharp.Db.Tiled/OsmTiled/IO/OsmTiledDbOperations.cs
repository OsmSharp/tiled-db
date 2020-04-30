using System;
using OsmSharp.Db.Tiled.IO;
using System.IO;
using Newtonsoft.Json;

namespace OsmSharp.Db.Tiled.OsmTiled.IO
{
    internal static class OsmTiledDbOperations
    {
        public static void SaveDbMeta(string path, OsmTiledDbMeta dbMeta)
        {
            var dbMetaPath = PathToMeta(path);
            using var stream = FileSystemFacade.FileSystem.Open(dbMetaPath, FileMode.Create);
            using var streamWriter = new StreamWriter(stream);
            JsonSerializer.CreateDefault().Serialize(streamWriter, dbMeta);
        }
        
        public static OsmTiledDbBase LoadDb(string path)
        {
            var meta = OsmTiledDbOperations.LoadDbMeta(path);
            switch (meta.Type)
            {
                case OsmTiledDbType.Snapshot:
                    return new OsmTiledDbSnapshot(path, meta);
                case OsmTiledDbType.Full:
                    return new OsmTiledDb(path, meta);
            }
            
            throw new Exception("Could not determine db type from meta.");
        }
        
        internal static OsmTiledDbMeta LoadDbMeta(string path)
        {
            var dbMetaPath = PathToMeta(path);
            using var stream = FileSystemFacade.FileSystem.OpenRead(dbMetaPath);
            using var streamReader = new StreamReader(stream);
            using var jsonReader = new JsonTextReader(streamReader);
            return JsonSerializer.CreateDefault().Deserialize<OsmTiledDbMeta>(jsonReader);
        }
        
        public static string PathToIdIndex(string path)
        {
            return FileSystemFacade.FileSystem.Combine(path, "data.id.idx");
        }

        public static OsmTiledIndex LoadIndex(string path)
        {
            var stream = FileSystemFacade.FileSystem.OpenRead(
                PathToIdIndex(path));
            return new OsmTiledIndex(stream);
        }

        public static string PathToData(string path)
        {
            return FileSystemFacade.FileSystem.Combine(path, "data.db");
        }

        public static string PathToTileIndex(string path)
        {
            return FileSystemFacade.FileSystem.Combine(path, "data.tile.idx");
        }
        
        public static OsmTiledLinkedStream LoadData(string path)
        {
            using var indexStream = FileSystemFacade.FileSystem.OpenRead(
                PathToTileIndex(path));
            var stream = FileSystemFacade.FileSystem.OpenRead(
                PathToData(path));
            
            return OsmTiledLinkedStream.Deserialize(indexStream, stream);
        }
        
        public static string PathToMeta(string path)
        {
            return FileSystemFacade.FileSystem.Combine(path, "meta.json");
        }
    }
}