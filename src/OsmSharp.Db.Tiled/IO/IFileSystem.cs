using System.Collections.Generic;
using System.IO;

namespace OsmSharp.Db.Tiled.IO
{
    /// <summary>
    /// An abstract file system.
    /// </summary>
    internal interface IFileSystem
    {
        /// <summary>
        /// Returns true if the given file exists.
        /// </summary>
        bool Exists(string file);

        /// <summary>
        /// Deletes the given file.
        /// </summary>
        /// <param name="file"></param>
        void Delete(string file);

        /// <summary>
        /// Returns the filename.
        /// </summary>
        string FileName(string file);

        /// <summary>
        /// Returns a relative path.
        /// </summary>
        /// <param name="basePath">The base path.</param>
        /// <param name="path">The path.</param>
        /// <returns>The path relative to the base path.</returns>
        string RelativePath(string basePath, string path);

        /// <summary>
        /// Gets the directory the given file is in.
        /// </summary>
        string DirectoryForFile(string file);

        /// <summary>
        /// Returns true if the given directory exists.
        /// </summary>
        bool DirectoryExists(string directory);
        
        /// <summary>
        /// Moves the given directory to a new location.
        /// </summary>
        /// <param name="source">The source.</param>
        /// <param name="target">The target.</param>
        void MoveDirectory(string source, string target);

        /// <summary>
        /// Returns the directory name of the last directory in the given path.
        /// </summary>
        string LeafDirectoryName(string directory);

        /// <summary>
        /// Creates the given directory.
        /// </summary>
        void CreateDirectory(string directory);

        /// <summary>
        /// Enumerates directories.
        /// </summary>
        IEnumerable<string> EnumerateDirectories(string directory, string? startsWith = null);

        /// <summary>
        /// Enumerates files.
        /// </summary>
        IEnumerable<string> EnumerateFiles(string directory, string? mask = null);

        /// <summary>
        /// Opens the given file for read-access.
        /// </summary>
        Stream OpenRead(string location);

        /// <summary>
        /// Opens the given file for write-access.
        /// </summary>
        Stream OpenWrite(string location);

        /// <summary>
        /// Opens the file at the given path.
        /// </summary>
        /// <param name="file">The file.</param>
        /// <param name="mode">The mode.</param>
        /// <returns>A stream.</returns>
        Stream Open(string file, FileMode mode);

        /// <summary>
        /// Combines an array of strings into a path.
        /// </summary>
        string Combine(params string[] paths);

        /// <summary>
        /// Returns the parent directory of the given directory.
        /// </summary>
        /// <param name="path">The directory.</param>
        /// <returns>The parent directory.</returns>
        string ParentDirectory(string path);
    }
}
