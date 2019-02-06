using OsmSharp.Db.Tiled.Ids;
using OsmSharp.Db.Tiled.Indexes;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.Tiles;
using OsmSharp.Streams;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using OsmSharp.Changesets;
using OsmSharp.Db.Tiled.Changesets;
using OsmSharp.Db.Tiled.Collections;
using OsmSharp.IO.PBF;
using Serilog;

namespace OsmSharp.Db.Tiled
{
    /// <summary>
    /// A database snapshot.
    /// </summary>
    /// <remarks>
    /// A snapshot contains all data at a fixed point in time.
    /// </remarks>
    public class DatabaseSnapshot : DatabaseBase, IDatabaseView
    {
        /// <summary>
        /// Creates a new data based on the given folder.
        /// </summary>
        public DatabaseSnapshot(string path, DatabaseMeta meta, bool mapped = true)
            : base(path, meta, mapped)
        {
            
        }
        
        /// <summary>
        /// Gets all the relevant tiles.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<Tile> GetTiles()
        {
            var basePath = FileSystemFacade.FileSystem.Combine(this.Path, this.Zoom.ToInvariantString());
            if (!FileSystemFacade.FileSystem.DirectoryExists(basePath))
            {
                yield break;
            }
            var mask = "*.nodes.osm.bin";
            if (this.Compressed) mask = mask + ".zip";
            foreach(var xDir in FileSystemFacade.FileSystem.EnumerateDirectories(
                basePath))
            {
                var xDirName = FileSystemFacade.FileSystem.DirectoryName(xDir);
                if (!uint.TryParse(xDirName, out var x))
                {
                    continue;
                }

                foreach (var tile in FileSystemFacade.FileSystem.EnumerateFiles(xDir, mask))
                {
                    var tileName = FileSystemFacade.FileSystem.FileName(tile);

                    if (!uint.TryParse(tileName.Substring(0,
                        tileName.IndexOf('.')), out var y))
                    {
                        continue;
                    }

                    yield return new Tile(x, y, this.Zoom);
                }
            }
        }

        /// <inheritdoc/>
        public override OsmGeo Get(OsmGeoType type, long id)
        { // in a snapshot the local tiles contain all data.
            return this.GetLocal(type, id);
        }

        /// <inheritdoc/>
        public override IEnumerable<OsmGeo> GetTile(Tile tile, OsmGeoType type)
        { // in a snapshot the local tiles contain all data.
            return this.GetLocalTile(tile, type);
        }
    }
}