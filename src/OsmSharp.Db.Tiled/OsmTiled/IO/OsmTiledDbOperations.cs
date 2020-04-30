using System;
using OsmSharp.Db.Tiled.IO;
using System.IO;
using Newtonsoft.Json;

namespace OsmSharp.Db.Tiled.OsmTiled.IO
{
    internal static class OsmTiledDbOperations
    {
        public static string BuildOsmTiledDbPath(string path, long id, string type)
        {
            return FileSystemFacade.FileSystem.Combine(path,
                $"{id:0000000000000000}_{type}");
        }
        
        public static void SaveDbMeta(string path, OsmTiledDbMeta dbMeta)
        {
            var dbMetaPath = PathToMeta(path);
            using var stream = FileSystemFacade.FileSystem.Open(dbMetaPath, FileMode.Create);
            using var streamWriter = new StreamWriter(stream);
            JsonSerializer.CreateDefault().Serialize(streamWriter, dbMeta);
        }
        
        public static OsmTiledDbBase LoadDb(string path, long id, Func<long, OsmTiledDbBase> getDb)
        {
            var dbPath = BuildOsmTiledDbPath(path, id, OsmTiledDbType.Full);
            OsmTiledDbMeta? meta = null;
            if (FileSystemFacade.FileSystem.DirectoryExists(dbPath))
            {
                // a full db exists, use that one!
                meta = OsmTiledDbOperations.LoadDbMeta(dbPath);
            }
            else
            {
                // check for a snapshot.
                dbPath = BuildOsmTiledDbPath(path, id, OsmTiledDbType.Snapshot);
                if (FileSystemFacade.FileSystem.DirectoryExists(dbPath))
                {
                    meta = OsmTiledDbOperations.LoadDbMeta(dbPath);
                }
            }
            
            if (meta == null) throw new Exception($"Database {id} requested but not found!");
             
            switch (meta.Type)
            {
                case OsmTiledDbType.Snapshot:
                    return new OsmTiledDbSnapshot(dbPath, getDb, meta);
                case OsmTiledDbType.Full:
                    return new OsmTiledDb(dbPath, meta);
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