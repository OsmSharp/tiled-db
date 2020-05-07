using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.OsmTiled.Changes;
using OsmSharp.Db.Tiled.OsmTiled.Data;
using OsmSharp.Db.Tiled.Tiles;
using OsmSharp.Db.Tiled.OsmTiled.IO;

namespace OsmSharp.Db.Tiled.OsmTiled.Build
{
    internal static class OsmTiledDbBuilderOperations
    {
        internal static long Write(this IEnumerable<(OsmGeo osmGeo, IEnumerable<(uint x, uint y)> tiles)> dataStream,
            string path, uint zoom, bool saveDeleted = false, IEnumerable<(uint x, uint y)>? tiles = null, byte[]? buffer = null)
        {
            buffer ??= new byte[1024];
            if (buffer.Length < 1024) Array.Resize(ref buffer, 1024);
            
            using var dataBase = FileSystemFacade.FileSystem.Open(
                OsmTiledDbOperations.PathToData(path), FileMode.Create);
            var data = new HugeBufferedStream(dataBase);
            using var tileIndexStream = FileSystemFacade.FileSystem.Open(
                OsmTiledDbOperations.PathToTileIndex(path), FileMode.Create);
            using var osmGeoIndexStream = FileSystemFacade.FileSystem.Open(
                OsmTiledDbOperations.PathToIdIndex(path), FileMode.Create);

            var tiledStream = new OsmTiledLinkedStream(data);
            var osmGeoIndex = new OsmTiledDbOsmGeoIndex(osmGeoIndexStream);
            var tileIndex = new OsmTiledDbTileIndex();

            // append to the stream.
            var count = 0L;
            var tilesList = new List<uint>();
            var lastDeleted = new OsmGeoKey(OsmGeoType.Relation,long.MaxValue);
            foreach (var (osmGeo, osmGeoTiles) in dataStream)
            {
                if (!osmGeo.Id.HasValue) throw new InvalidDataException("Cannot store data without a valid id.");

                // build tile list.
                tilesList.Clear();
                tilesList.AddRange(osmGeoTiles.Select(x => Tile.ToLocalId(x, zoom)));
                
                // write delete state if needed.
                if (lastDeleted.Id != long.MaxValue &&
                    lastDeleted.Id != osmGeo.Id.Value)
                {
                    // the last object was deleted, write as such.
                    osmGeoIndex.Append(new OsmGeoKey(osmGeo), -1);
                }
                lastDeleted = new OsmGeoKey(OsmGeoType.Relation, long.MaxValue);

                var isDeleted = osmGeo.IsDeleted();
                if (isDeleted)
                {
                    if (saveDeleted)
                    {
                        var location = tiledStream.Append(tilesList, osmGeo, buffer);
                        count++;
                        tileIndex.SetTilesIfNotSet(tilesList, location);
                    }
                    
                    // when deleted delay the write.
                    lastDeleted = new OsmGeoKey(osmGeo);
                }
                else
                {
                    // append data.
                    var location =  tiledStream.Append(tilesList, osmGeo, buffer);
                    count++;
                    tileIndex.SetTilesIfNotSet(tilesList, location);
                    
                    // when not deleted, append.
                    osmGeoIndex.Append(new OsmGeoKey(osmGeo), location);
                }
            }
            
            // write delete state if needed.
            if (lastDeleted.Id != long.MaxValue)
            {
                // the last object was deleted, write as such.
                osmGeoIndex.Append(lastDeleted, -1);
            }

            if (tiles != null)
            {
                // make sure to mark tiles as empty when they have not been written to.
                foreach (var tile in tiles)
                {
                    var localId = Tile.ToLocalId(tile, zoom);
                    if (!tileIndex.HasTile(localId))
                    {
                        tileIndex.SetAsEmpty(localId);
                    }
                }
            }

            // reverse indexed data and save tile index.
            tiledStream.Flush();
            tileIndex.Serialize(tileIndexStream);

            return count;
        }
    }
}