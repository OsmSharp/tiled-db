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
            var root = $"/{Guid.NewGuid().ToString()}";
            
            FileSystemFacade.GetFileSystem = MockFileSystem.GetMockFileSystem;
            FileSystemFacade.FileSystem.CreateDirectory($"{root}/data");

            Assert.True(FileSystemFacade.FileSystem.DirectoryExists($"{root}/data"));
            Assert.False(FileSystemFacade.FileSystem.DirectoryExists($"{root}/data1"));
        }
    }
}
