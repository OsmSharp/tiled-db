using System;
using System.Threading.Tasks;
using NUnit.Framework;
using OsmSharp.Db.Tiled.Build;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.OsmTiled;
using OsmSharp.Db.Tiled.OsmTiled.IO;
using OsmSharp.Db.Tiled.Tests.Mocks;

namespace OsmSharp.Db.Tiled.Tests
{
    [TestFixture]
    public class OsmTiledHistoryDbTests
    {
        [Test]
        public async Task OsmTiledHistoryDb_Create_ShouldCreateNew()
        {
            var osmGeos = new OsmGeo[]
            {
                new Node()
                {
                    Id = 456414,
                    Latitude = 50,
                    Longitude = 4,
                    ChangeSetId = 1,
                    UserId = 1,
                    UserName = "Ben",
                    Visible = true,
                    TimeStamp = DateTime.Now,
                    Version = 1
                }
            };
            
            FileSystemFacade.FileSystem = new MockFileSystem(@"/");
            FileSystemFacade.FileSystem.CreateDirectory(@"/data");

            var newDb = OsmTiledHistoryDb.Create(@"/data", osmGeos);
            
            Assert.NotNull(newDb.Latest);
        }
        
        [Test]
        public async Task OsmTiledHistoryDb_TryReload_NoNewData_ShouldDoNothingAndReturnFalse()
        {
            var osmGeos = new OsmGeo[]
            {
                new Node()
                {
                    Id = 456414,
                    Latitude = 50,
                    Longitude = 4,
                    ChangeSetId = 1,
                    UserId = 1,
                    UserName = "Ben",
                    Visible = true,
                    TimeStamp = DateTime.Now,
                    Version = 1
                }
            };
            
            FileSystemFacade.FileSystem = new MockFileSystem(@"/");
            FileSystemFacade.FileSystem.CreateDirectory(@"/data");

            var db = OsmTiledHistoryDb.Create(@"/data", osmGeos);
            
            // reload db.
            Assert.False(db.TryReload());
        }
        
        [Test]
        public async Task OsmTiledHistoryDb_TryReload_NewData_ShouldLoadNewDbAndReturnTrue()
        {
            FileSystemFacade.FileSystem = new MockFileSystem(@"/");
            FileSystemFacade.FileSystem.CreateDirectory(@"/data");

            var db = OsmTiledHistoryDb.Create(@"/data", new OsmGeo[]
            {
                new Node()
                {
                    Id = 456414,
                    Latitude = 50,
                    Longitude = 4,
                    ChangeSetId = 1,
                    UserId = 1,
                    UserName = "Ben",
                    Visible = true,
                    TimeStamp = DateTime.Now,
                    Version = 1
                }
            });
            
            // update db without using the db method (as if it was updated out of process).
            var tiledDb = new OsmGeo[]
            {
                new Node()
                {
                    Id = 456414,
                    Latitude = 50,
                    Longitude = 4,
                    ChangeSetId = 1,
                    UserId = 1,
                    UserName = "Ben",
                    Visible = true,
                    TimeStamp = DateTime.Now,
                    Version = 2
                }
            }.Update("/data");
            
            // update meta data.
            var meta = new OsmTiledHistoryDbMeta()
            {
                Latest =  FileSystemFacade.FileSystem.RelativePath("/data", tiledDb.Path)
            };
            OsmTiledHistoryDbOperations.SaveDbMeta("/data", meta);
            
            // reload db.
            Assert.True(db.TryReload());
        }
    }
}