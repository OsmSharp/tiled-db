using System;
using NUnit.Framework;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Streams;

namespace OsmSharp.Db.Tiled.Tests.Build
{
    /// <summary>
    /// Contains builder tests.
    /// </summary>
    [TestFixture]
    public class OsmDbBuilderTests
    {
        /// <summary>
        /// Tests building a database.
        /// </summary>
        [Test]
        public void OsmDbBuilder_BuildDb_ShouldBuildInitial()
        {
            FileSystemFacade.FileSystem = new Mocks.MockFileSystem(@"/");
            FileSystemFacade.FileSystem.CreateDirectory(@"/data");

            // build the database.
            var osmGeos = new OsmGeo[]
            {
                new Node()
                {
                    Id = 0,
                    Latitude = 50,
                    Longitude = 4,
                    ChangeSetId = 1,
                    UserId = 1,
                    UserName = "Ben",
                    Visible = true,
                    TimeStamp = DateTime.Now,
                    Version = 1
                },
                new Node()
                {
                    Id = 1,
                    Latitude = 50,
                    Longitude = 4,
                    ChangeSetId = 1,
                    UserId = 1,
                    UserName = "Ben",
                    Visible = true,
                    TimeStamp = DateTime.Now,
                    Version = 1
                },
                new Way()
                {
                    Id = 0,
                    ChangeSetId = 1,
                    Nodes = new long[]
                    {
                        0, 1
                    },
                    Tags = null,
                    TimeStamp = DateTime.Now,
                    UserId = 1,
                    UserName = "Ben",
                    Version = 1,
                    Visible = true
                }
            };
            OsmSharp.Db.Tiled.Build.OsmTiledHistoryDbBuilder.Build(
               osmGeos, @"/data", 14);

            Assert.True(FileSystemFacade.FileSystem.DirectoryExists(@"/data"));
            Assert.True(FileSystemFacade.FileSystem.Exists(@"/data/meta.json"));

            var meta = OsmTiledHistoryDbOperations.LoadDbMeta("/data/meta.json");
            var initialPath = meta.Latest;
            
            // check if initial dir exists.
            Assert.True(FileSystemFacade.FileSystem.DirectoryExists(initialPath));
            Assert.True(FileSystemFacade.FileSystem.Exists(
                FileSystemFacade.FileSystem.Combine(initialPath, "meta.json")));
        }
    }
}