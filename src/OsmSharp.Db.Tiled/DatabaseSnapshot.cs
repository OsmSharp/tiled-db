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
using OsmSharp.Db.Tiled.Collections;
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
        public DatabaseSnapshot(string path, uint zoom = 14, bool compressed = true, bool mapped = true)
            : base(path, mapped, zoom, compressed)
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
        
        /// <summary>
        /// Gets the object with the given type and id.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <param name="id">The id.</param>
        /// <returns>The object.</returns>
        public OsmGeo Get(OsmGeoType type, long id)
        {
            // in a snapshot a local is all we need.
            return this.GetLocal(type, id);
        }

        /// <summary>
        /// Gets the data in the given tile.
        /// </summary>
        /// <param name="tile"></param>
        /// <returns></returns>
        public IEnumerable<OsmGeo> GetTile(Tile tile)
        {
            // in a snapshot a local is all we need.
            return this.GetLocalTile(tile);
        }
    }
}