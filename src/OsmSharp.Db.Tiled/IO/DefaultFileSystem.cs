using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace OsmSharp.Db.Tiled.IO
{
    internal class DefaultFileSystem : IFileSystem
    {
        public string Combine(params string[] paths)
        {
            return Path.Combine(paths);
        }

        public bool Exists(string file)
        {
            return File.Exists(file);
        }

        public Stream OpenRead(string location)
        {
            return File.OpenRead(location);
        }

        public string RelativePath(string basePath, string path)
        {
            Console.WriteLine($"base path: {basePath}");
            Console.WriteLine($"path: {path}");
            if (!path.StartsWith(basePath))
            {
                Console.WriteLine("Does not start with.");
                return path;
            }

            var result = path.Substring(basePath.Length + 1);
            Console.WriteLine($"result: {result}");
            return result;
        }

        public Stream Open(string file, FileMode mode)
        {
            return File.Open(file, mode);
        }

        public string FileName(string file)
        {
            return Path.GetFileName(file);
        }

        public bool DirectoryExists(string directory)
        {
            return Directory.Exists(directory);
        }

        public string LeafDirectoryName(string directory)
        {
            return (new DirectoryInfo(directory)).Name;
        }

        public IEnumerable<string> EnumerateDirectories(string directory)
        {
            return Directory.EnumerateDirectories(directory);
        }

        public IEnumerable<string> EnumerateFiles(string directory, string? searchPattern = null)
        {
            return Directory.EnumerateFiles(directory, searchPattern);
        }

        public Stream OpenWrite(string location)
        {
            return File.OpenWrite(location);
        }

        public void Delete(string file)
        {
            File.Delete(file);
        }

        public string DirectoryForFile(string file)
        {
            return new FileInfo(file).Directory.FullName;
        }

        public void CreateDirectory(string directory)
        {
            Directory.CreateDirectory(directory);
        }

        public string ParentDirectory(string path)
        {
            var dirInfo = new DirectoryInfo(path);

            return dirInfo.Parent.FullName;
        }
    }
}
