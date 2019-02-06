using System.Collections.Generic;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.Tiles;

namespace OsmSharp.Db.Tiled
{
    public static class IDatabaseViewExtensions
    {
        /// <summary>
        /// Loads a new database view using the given meta data.
        /// </summary>
        /// <param name="pathToMeta">The path to the meta data file.</param>
        /// <returns>The database view.</returns>
        public static IDatabaseView LoadFromMeta(string pathToMeta)
        {
            using (var stream = IO.FileSystemFacade.FileSystem.OpenRead(pathToMeta))
            {
                var dbMeta = DatabaseMeta.Deserialize(stream);

                var path = FileSystemFacade.FileSystem.DirectoryForFile(pathToMeta);
                if (string.IsNullOrEmpty(dbMeta.Base))
                { // this is a snapshot.
                    return new DatabaseSnapshot(path, dbMeta);
                }
                else
                { // this is a view, first load parent.
                    var parent = LoadFromMeta(dbMeta.Base);
                    return new DatabaseDiff(parent, path, dbMeta);
                }
            }
        }
        /// <summary>
        /// Gets the data in the given tile.
        /// </summary>
        /// <param name="view">The view.</param>
        /// <param name="tile">The tile to get the data for.</param>
        /// <returns>The data in the given tile.</returns>
        public static IEnumerable<OsmGeo> GetTile(this IDatabaseView view, Tile tile)
        { 
            foreach (var node in view.GetTile(tile, OsmGeoType.Node))
            {
                yield return node;
            }

            foreach (var way in view.GetTile(tile, OsmGeoType.Way))
            {
                yield return way;
            }

            foreach (var relation in view.GetTile(tile, OsmGeoType.Relation))
            {
                yield return relation;
            }
        }
    }
}