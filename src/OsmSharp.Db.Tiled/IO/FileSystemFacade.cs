using System;
using System.Collections.Generic;
using System.Text;

namespace OsmSharp.Db.Tiled.IO
{
    /// <summary>
    /// Defines a facade for the file system.
    /// </summary>
    internal static class FileSystemFacade
    {
        /// <summary>
        /// Gets the file system handler.
        /// </summary>
        public static IFileSystem FileSystem => GetFileSystem();

        /// <summary>
        /// Gets a function to get the file system handler.
        /// </summary>
        public static Func<IFileSystem> GetFileSystem = () => new DefaultFileSystem();
    }
}