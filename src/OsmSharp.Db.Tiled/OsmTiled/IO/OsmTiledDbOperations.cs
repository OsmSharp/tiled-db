using System;
using System.Collections.Generic;
using System.Globalization;
using OsmSharp.Db.Tiled.IO;
using System.IO;
using System.Text.Json;
using OsmSharp.Db.Tiled.OsmTiled.Data;

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
            return FileSystemFacade.FileSystem.Combine(path, "" + Guid.NewGuid());
        }

        public static string BuildDbPath(string path, long id, long? timespan, string type)
        {
            if (timespan == null) return FileSystemFacade.FileSystem.Combine(path,
                    $"{IdToPath(id)}_{type}");
            return FileSystemFacade.FileSystem.Combine(path,
                $"{IdToPath(id)}_{timespan:00000000000}_{type}");
        }

        internal static string IdToPath(long id)
        {
            var date = id.FromUnixTime();
            return $"{date.Year:0000}{date.Month:00}{date.Day:00}-{date.Hour:00}{date.Minute:00}{date.Second:00}";
        }

        internal static bool TryIdFromPath(string path, out long id)
        {
            if (!DateTime.TryParseExact(path, "yyyyMMdd-HHmmss", System.Globalization.CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal, out var utcDate))
            {
                id = long.MaxValue;
                return true;
            }
            id = utcDate.ToUniversalTime().ToUnixTime();
            return true;
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
            if (!TryIdFromPath(dateTimeString.Substring(0, firstIndexOf ), out var secondsFromEpoch))
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
            id = secondsFromEpoch;
            return true;
        }

        public static string? LoadLongestSnapshotDb(string path, long id)
        {
            var potentialDbs = GetDbPaths(path,
                $"{IdToPath(id)}");
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
                $"{IdToPath(id)}");
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
            using var streamWriter = new Utf8JsonWriter(stream);
            JsonSerializer.Serialize(streamWriter, dbMeta);
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
            return JsonSerializer.Deserialize<OsmTiledDbMeta>(streamReader.ReadToEnd());
        }
        
        public static string PathToIdIndex(string path)
        {
            return FileSystemFacade.FileSystem.Combine(path, "data.id.idx");
        }

        private const long MaxInMemorySize = 1024 * 1024 * 20;

        public static OsmTiledDbOsmGeoIndex LoadOsmGeoIndex(string path)
        {
            var stream = FileSystemFacade.FileSystem.OpenRead(
                PathToIdIndex(path)).ToMemoryStreamSmall(MaxInMemorySize);
            return new OsmTiledDbOsmGeoIndex(stream);
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
            var stream = FileSystemFacade.FileSystem.OpenRead(
                PathToData(path)).ToMemoryStreamSmall(MaxInMemorySize);;
            
            return OsmTiledLinkedStream.Deserialize(stream);
        }
        
        public static IOsmTiledDbTileIndexReadOnly LoadTileIndex(string path)
        {
            var stream = FileSystemFacade.FileSystem.OpenRead(
                PathToTileIndex(path)).ToMemoryStreamSmall(MaxInMemorySize);
            
            return OsmTiledDbTileIndex.DeserializeReadonly(stream);
        }
        
        public static string PathToMeta(string path)
        {
            return FileSystemFacade.FileSystem.Combine(path, "meta.json");
        }
    }
}