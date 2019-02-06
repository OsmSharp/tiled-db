using System;
using System.Collections.Generic;
using System.IO;
using OsmSharp.Changesets;
using OsmSharp.Db.Tiled.Indexes;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.Tiles;
using OsmSharp.IO.Binary;
using OsmSharp.IO.PBF;
using Serilog;

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
            
//            // execute the deletes.
//            if (changeset.Delete != null &&
//                changeset.Delete.Length > 0)
//            {
//                for (var d = 0; d < changeset.Delete.Length; d++)
//                {
//                    var delete = changeset.Delete[d];
//                    diff.Delete(delete.Type, delete.Id.Value);
//                    if (changeset.Create.Length > 1000 && d % 1000 == 0)
//                    {
//                        Log.Information($"Deleted {d+1}/{changeset.Delete.Length} objects.");
//                    }
//                }
//                if (changeset.Delete.Length > 1000)
//                {
//                    Log.Information($"Deleted {changeset.Delete.Length}/{changeset.Delete.Length} objects.");
//                }
//            }
            
//            // execute the creations.
//            if (changeset.Create != null &&
//                changeset.Create.Length > 0)
//            {
//                for (var c = 0; c < changeset.Create.Length; c++)
//                {
//                    diff.Create(changeset.Create[c]);
//                    if (changeset.Create.Length > 1000 && c % 1000 == 0)
//                    {
//                        Log.Information($"Created {c}/{changeset.Create.Length} objects.");
//                    }
//                }
//                if (changeset.Create.Length > 1000)
//                {
//                    Log.Information($"Created {changeset.Create.Length}/{changeset.Create.Length} objects.");
//                }
//            }
            
            // execute the modifications.
            if (changeset.Modify != null &&
                changeset.Modify.Length > 0)
            {
                for (var m = 0; m < changeset.Modify.Length; m++)
                {
                    diff.Modify(changeset.Modify[m]);
                    if (changeset.Modify.Length > 1000 && m % 1000 == 0)
                    {
                        Log.Information($"Modified {m}/{changeset.Modify.Length} objects.");
                    }
                }
                if (changeset.Modify.Length > 1000)
                {
                    Log.Information($"Modified {changeset.Modify.Length}/{changeset.Modify.Length} objects.");
                }
            }

            return diff;
        }
    }
}