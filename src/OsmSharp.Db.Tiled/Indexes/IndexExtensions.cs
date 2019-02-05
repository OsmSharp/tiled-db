﻿using OsmSharp.Db.Tiled.IO;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;

namespace OsmSharp.Db.Tiled.Indexes
{
    internal static class IndexExtensions
    {
        /// <summary>
        /// Writes the given index to a given tile async.
        /// </summary>
        public static void Write(this Index index, string filename)
        {
            var directory = FileSystemFacade.FileSystem.DirectoryForFile(filename);
            if (!FileSystemFacade.FileSystem.DirectoryExists(directory))
            {
                FileSystemFacade.FileSystem.CreateDirectory(directory);
            }

            using (var stream = FileSystemFacade.FileSystem.Open(filename, FileMode.Create))
            {
                index.Serialize(stream);
            }
        }
    }
}