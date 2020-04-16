using NUnit.Framework;
using OsmSharp.Db.Tiled.IO;
using System;
using System.Collections.Generic;
using System.Text;

namespace OsmSharp.Db.Tiled.Tests.Mocks
{
    /// <summary>
    /// Contains tests for the mock file system.
    /// </summary>
    [TestFixture]
    public class MockFileSystemTests
    {
        [Test]
        public void MockFileSystem_Linux_CreateNewDirectory_DirectoryShouldExist()
        {
            FileSystemFacade.FileSystem = new Mocks.MockFileSystem(@"/");

            FileSystemFacade.FileSystem.CreateDirectory(@"/data");

            Assert.True(FileSystemFacade.FileSystem.DirectoryExists(@"/data"));
            Assert.False(FileSystemFacade.FileSystem.DirectoryExists(@"/data1"));
        }
        
        [Test]
        public void MockFileSystem_Windows_CreateNewDirectory_DirectoryShouldExist()
        {
            FileSystemFacade.FileSystem = new Mocks.MockFileSystem(@"C:\");

            FileSystemFacade.FileSystem.CreateDirectory(@"C:\data");

            Assert.True(FileSystemFacade.FileSystem.DirectoryExists(@"C:\data"));
            Assert.False(FileSystemFacade.FileSystem.DirectoryExists(@"C:\data1"));
        }
    }
}
