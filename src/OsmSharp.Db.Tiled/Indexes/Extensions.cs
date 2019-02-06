using OsmSharp.Db.Tiled.IO;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;

namespace OsmSharp.Db.Tiled.Indexes
{
    internal static class Extensions
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

        /// <summary>
        /// Appends an id pair to a stream.
        /// </summary>
        /// <param name="stream">The stream to append to.</param>
        /// <param name="id">The id.</param>
        public static void AppendToDeletedIndex(this Stream stream, long id)
        {
            stream.Write(BitConverter.GetBytes(id), 0, 8);
        }

        /// <summary>
        /// Appends an encoded id/mask pair to a stream.
        /// </summary>
        /// <param name="stream">The stream to append to.</param>
        /// <param name="id">The id.</param>
        /// <param name="mask">The mask.</param>
        public static void AppendToIndex(this Stream stream, long id, int mask)
        {
            Index.Encode(id, mask, out var data);
            stream.Write(BitConverter.GetBytes(data), 0, 8);
        }
    }
}