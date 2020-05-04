using System;
using System.Collections.Generic;
using System.Globalization;
using OsmSharp.Db.Tiled.IO;
using System.IO;
using Newtonsoft.Json;
using OsmSharp.Db.Tiled.Logging;

namespace OsmSharp.Db.Tiled.OsmTiled.IO
{
    internal static class OsmTiledDbOperations
    {
        public static IEnumerable<(string path, long id, long? timespan, string type)> GetDbPaths(string path, string? startsWith = null)
        {
            var directories = FileSystemFacade.FileSystem.EnumerateDirectories(path, startsWith);
            foreach (var directory in directories)
            {
                if (!OsmTiledDbOperations.TryParseDbPath(directory, out var id, out var timespan, out var type)) continue;

                yield return (directory, id, timespan, type);
            }
        }

        public static string BuildTempDbPath(string path)
        {
            return FileSystemFacade.FileSystem.Combine(path, "." + Guid.NewGuid().ToString());
        }

        public static string BuildDbPath(string path, long id, long? timespan, string type)
        {
            var guid = Guid.NewGuid();
            if (timespan == null) return FileSystemFacade.FileSystem.Combine(path,
                    $"{id:0000000000000000}_{type}");
            return FileSystemFacade.FileSystem.Combine(path,
                $"{id:0000000000000000}_{timespan:00000000000}_{type}");
        }

        public static bool TryParseDbPath(string path, out long id, out long? timespan, out string type)
        {
            timespan = null;
            id = default;
            type = string.Empty;
            
            // check path, needs to end with type.
            var dateTimeString = FileSystemFacade.FileSystem.LeafDirectoryName(path);
            if (!(dateTimeString.EndsWith(OsmTiledDbType.Full) || dateTimeString.EndsWith(OsmTiledDbType.Snapshot) || dateTimeString.EndsWith(OsmTiledDbType.Diff))) return false;
            if(dateTimeString == null) return false;
            
            // check first '_' index.
            var firstIndexOf = dateTimeString.IndexOf("_", StringComparison.Ordinal);
            if (firstIndexOf  <= 0) return false;
            
            // parse timestamp.
            if (!long.TryParse(dateTimeString.Substring(0, firstIndexOf ), NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture,
                out var millisecondEpochs))
            {
                return false;
            }

            // parse timespan.
            var lastIndexOf = dateTimeString.LastIndexOf("_", StringComparison.Ordinal);
            if (lastIndexOf > firstIndexOf)
            {
                if (long.TryParse(dateTimeString.Substring(firstIndexOf + 1, lastIndexOf - firstIndexOf - 1), NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture,
                    out var millisecondSpan))
                {
                    timespan = millisecondSpan;
                }
            }
            
            // parse type.
            var typeParse = dateTimeString.Substring(lastIndexOf + 1);
            if (typeParse != OsmTiledDbType.Diff &&
                typeParse != OsmTiledDbType.Full &&
                typeParse != OsmTiledDbType.Snapshot)
            {
                return false;
            }

            type = typeParse;
            id = millisecondEpochs;
            return true;
        }

        public static string? LoadLongestSnapshotDb(string path, long id)
        {
            var potentialDbs = GetDbPaths(path,
                $"{id:0000000000000000}");
            (string? path, long id, long? timespan, string type) best = (null, long.MinValue, null, string.Empty);
            foreach (var potentialDb in potentialDbs)
            {
                if (potentialDb.type != OsmTiledDbType.Snapshot) continue;
                
                if (best.timespan == null || 
                    (potentialDb.timespan != null &&
                    best.timespan.Value < potentialDb.timespan.Value))
                {
                    best = potentialDb;
                }
            }

            return best.path;
        }

        public static string? LoadLongestDiffDb(string path, long id)
        {
            var potentialDbs = GetDbPaths(path,
                $"{id:0000000000000000}");
            (string? path, long id, long? timespan, string type) best = (null, long.MinValue, null, string.Empty);
            foreach (var potentialDb in potentialDbs)
            {
                if (potentialDb.type != OsmTiledDbType.Diff) continue;
                
                if (best.timespan == null || 
                    (potentialDb.timespan != null &&
                     best.timespan.Value < potentialDb.timespan.Value))
                {
                    best = potentialDb;
                }
            }

            return best.path;
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
            string? dbPath = BuildDbPath(path, id, null, OsmTiledDbType.Full);
            OsmTiledDbMeta? meta = null;
            if (FileSystemFacade.FileSystem.DirectoryExists(dbPath))
            {
                // a full db exists, use that one!
                meta = LoadDbMeta(dbPath);
            }
            else
            {
                // check for a snapshot.
                dbPath = LoadLongestSnapshotDb(path, id);
                if (dbPath != null &&
                    FileSystemFacade.FileSystem.DirectoryExists(dbPath))
                {
                    meta = OsmTiledDbOperations.LoadDbMeta(dbPath);
                }
                else
                {
                    // check for a diff.
                    dbPath = LoadLongestDiffDb(path, id);
                    if (dbPath != null &&
                        FileSystemFacade.FileSystem.DirectoryExists(dbPath))
                    {
                        meta = OsmTiledDbOperations.LoadDbMeta(dbPath);
                    }
                }
            }
            
            if (dbPath == null || meta == null) throw new Exception($"Database {id} requested but not found!");
             
            switch (meta.Type)
            {
                case OsmTiledDbType.Snapshot:
                    return new OsmTiledDbSnapshot(dbPath, getDb, meta);
                case OsmTiledDbType.Diff:
                    return new OsmTiledDbDiff(dbPath, getDb, meta);
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

        private const long MaxInMemorySize = 1024 * 1024 * 20;

        public static OsmTiledIndex LoadIndex(string path)
        {
            var stream = FileSystemFacade.FileSystem.OpenRead(
                PathToIdIndex(path)).ToMemoryStreamSmall(MaxInMemorySize);
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
                PathToTileIndex(path)).ToMemoryStreamSmall(MaxInMemorySize);;
            var stream = FileSystemFacade.FileSystem.OpenRead(
                PathToData(path)).ToMemoryStreamSmall(MaxInMemorySize);;
            
            return OsmTiledLinkedStream.Deserialize(indexStream, stream);
        }
        
        public static string PathToMeta(string path)
        {
            return FileSystemFacade.FileSystem.Combine(path, "meta.json");
        }
    }
}