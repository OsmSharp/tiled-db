using System;
using System.Collections.Generic;
using System.IO;
using OsmSharp.Changesets;
using OsmSharp.Db.Tiled.Indexes;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.Tiles;
using OsmSharp.IO.Binary;
using OsmSharp.IO.PBF;

namespace OsmSharp.Db.Tiled.Changesets
{
    /// <summary>
    /// Build a diff database from a given database view and a changeset.
    /// </summary>
    internal static class DiffBuilder
    {
        /// <summary>
        /// Builds a new diff from the given view.
        /// </summary>
        /// <param name="view">The view.</param>
        /// <param name="changeset">The changeset.</param>
        /// <param name="path">The path of the new diff.</param>
        /// <returns>The diff.</returns>
        public static DatabaseDiff Build(DatabaseBase view, OsmChange changeset, string path = null)
        {
            // creates a new database diff representing the given changes.
            // create a target directory if one wasn't specified.
            if (string.IsNullOrWhiteSpace(path))
            {
                var epochs = DateTime.Now.ToUnixTime();
                path = FileSystemFacade.FileSystem.Combine(FileSystemFacade.FileSystem.ParentDirectory(view.Path),
                    $"diff-{epochs}");
            }
            
            // make sure path exists.
            if (!FileSystemFacade.FileSystem.DirectoryExists(path))
            {
                FileSystemFacade.FileSystem.CreateDirectory(path);
            }
            
            // create the new empty view.
            var diff = new DatabaseDiff(view, path, new DatabaseMeta()
            {
                Base = view.Path,
                Zoom = view.Zoom,
                Compressed = false
            }, false);
            
            // execute the deletes.
            if (changeset.Delete != null &&
                changeset.Delete.Length > 0)
            {
                foreach (var delete in changeset.Delete)
                {
                    diff.Delete(delete.Type, delete.Id.Value);
                }
            }
            
            // execute the creations.
            if (changeset.Create != null &&
                changeset.Create.Length > 0)
            {
                foreach (var create in changeset.Create)
                {
                    diff.Create(create);
                }
            }
            
            // execute the modifications.
            if (changeset.Modify != null &&
                changeset.Modify.Length > 0)
            {
                foreach (var modify in changeset.Modify)
                {
                    diff.Modify(modify);
                }
            }

            return diff;
        }
    }
}