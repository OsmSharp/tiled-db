using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using OsmSharp.Changesets;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.OsmTiled.Changes;
using OsmSharp.Db.Tiled.OsmTiled.IO;
using OsmSharp.Db.Tiled.Tiles;

namespace OsmSharp.Db.Tiled.OsmTiled.Build
{
    /// <summary>
    /// Builds an OSM tiled db diff from an OSM stream.
    /// </summary>
    internal static class OsmTiledDbDiffBuilder
    {
        /// <summary>
        /// Builds a new database and write the structure to the given path.
        /// </summary>
        /// <param name="osmTiledDb">The tiled db.</param>
        /// <param name="changeset">The changeset stream.</param>
        /// <param name="path">The path to store the db at.</param>
        /// <param name="settings">The settings.</param>
        public static async Task ApplyChangSet(this OsmTiledDb osmTiledDb, OsmChange changeset, string path, 
            OsmTiledDbDiffBuildSettings? settings = null)
        {
            settings ??= new OsmTiledDbDiffBuildSettings();
             
            var zoom = osmTiledDb.Zoom;
            
            // collect all affected tiles and tile mutations.
            // build a modifications stream, a sorted stream augmented with tile ids. 
            var (timestamp, modifiedTiles, modifications) = changeset.BuildTiledStream(zoom,
                (key) => osmTiledDb.GetTiles(key.Type, key.Id).Select(x =>
                    Tile.ToLocalId(x, osmTiledDb.Zoom)));

            using var data = FileSystemFacade.FileSystem.Open(
                OsmTiledDbOperations.PathToData(path), FileMode.Create);
            using var dataTilesIndex = FileSystemFacade.FileSystem.Open(
                OsmTiledDbOperations.PathToTileIndex(path), FileMode.Create);
            using var dataIdIndex = FileSystemFacade.FileSystem.Open(
                OsmTiledDbOperations.PathToIdIndex(path), FileMode.Create);
            
            var tiledStream = new OsmTiledLinkedStream(data);
            var idIndex = new OsmTiledIndex(dataIdIndex);

            // loop over all tiles and their objects affected and apply the mutations.
            var buffer = new byte[1024];
            using var existingStream = osmTiledDb.Get(modifiedTiles
                .Select(x => Tile.FromLocalId(osmTiledDb.Zoom, x)).ToArray(), buffer)
                .Select<(OsmGeo osmGeo, IReadOnlyCollection<(uint x, uint y)> tiles), (IEnumerable<uint> tiles, OsmGeo osmGeo)>(x => ( 
                    x.tiles.Select(t => Tile.ToLocalId(t, osmTiledDb.Zoom)), x.osmGeo)).GetEnumerator();
            using var modifiedStream = modifications.GetEnumerator();
            var existingHasNext = existingStream.MoveNext();
            var modifiedHasNext = modifiedStream.MoveNext();

            while (existingHasNext || modifiedHasNext)
            {
                (IEnumerable<uint>? tiles, OsmGeo? osmGeo, OsmGeoKey key)? next = null;
                if (existingHasNext && modifiedHasNext)
                {
                    // compare and take first.
                    var existing = existingStream.Current;
                    var modified = modifiedStream.Current;
                    if (existing.osmGeo.Id == null) throw new InvalidDataException("Object found without an id.");
                    var modifiedId = OsmGeoCoder.Encode(modified.key.Type, modified.key.Id);
                    var existingId = OsmGeoCoder.Encode(existing.osmGeo.Type, existing.osmGeo.Id.Value);
                    if (existingId < modifiedId)
                    {
                        // move existing.
                        next = (existing.tiles.ToList(), existing.osmGeo, new OsmGeoKey(existing.osmGeo));
                        existingHasNext = existingStream.MoveNext();
                    }
                    else if (modifiedId < existingId)
                    {
                        // move modified.
                        next = (modified.tiles?.ToList(), modified.osmGeo, modified.key);
                        modifiedHasNext = modifiedStream.MoveNext();
                    }
                    else
                    { // overwrite existing if equal.
                        // move modified.
                        next = (modified.tiles?.ToList(), modified.osmGeo, modified.key);
                        modifiedHasNext = modifiedStream.MoveNext();
                        existingHasNext = existingStream.MoveNext();
                    }
                }
                else if (existingHasNext)
                {
                    // move existing.
                    var existing = existingStream.Current;
                    next = (existing.tiles.ToList(), existing.osmGeo, new OsmGeoKey(existing.osmGeo));
                    existingHasNext = existingStream.MoveNext();
                }
                else
                {
                    // move modified.
                    var modified = modifiedStream.Current;
                    next = (modified.tiles?.ToList(), modified.osmGeo, modified.key);
                    modifiedHasNext = modifiedStream.MoveNext();
                }

                if (next == null) throw new InvalidDataException("Next object cannot be null.");
                if (next?.osmGeo == null || next?.tiles == null)
                { // this object was delete, add it as such to the index.
                    idIndex.Append(next.Value.key, -1);
                    continue;
                }
                
                // apply settings.
                Prepare(next.Value.osmGeo, settings);
                
                // append to output.
                var tiles = next.Value.tiles.ToList();
                var location = tiledStream.Append(tiles, next.Value.osmGeo);
                idIndex.Append(new OsmGeoKey(next.Value.osmGeo), location);
            }
            
            // set empty tiles if any.
            foreach (var tile in modifiedTiles)
            {
                if (tiledStream.HasTile(tile)) continue;
                
                // tile was set as modified but it wasn't written to, it has to be empty.
                tiledStream.SetAsEmpty(tile);
            }

            // reverse indexed data and save tile index.
            tiledStream.Flush();
            tiledStream.SerializeIndex(dataTilesIndex);

            // save the meta-data.
            var dbMeta = new OsmTiledDbMeta
            {
                Base = osmTiledDb.Path, 
                Type = OsmTiledDbType.Diff,
                Zoom = zoom,
                Timestamp = timestamp
            };
            OsmTiledDbOperations.SaveDbMeta(path, dbMeta);
        }

        private static void Prepare(this OsmGeo osmGeo, OsmTiledDbDiffBuildSettings settings)
        {
            if (!settings.IncludeChangeset) osmGeo.ChangeSetId = null;
            if (!settings.IncludeUsername) osmGeo.UserName = null;
            if (!settings.IncludeUserId) osmGeo.UserId = null;
            if (!settings.IncludeVisible) osmGeo.Visible = null;
        }
    }
}