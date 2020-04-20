using System;
using NUnit.Framework;
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
        public void OsmTiledHistoryDb_TryReload_NoNewData_ShouldDoNothingAndReturnFalse()
        {
            FileSystemFacade.FileSystem = new MockFileSystem(@"/");
            FileSystemFacade.FileSystem.CreateDirectory(@"/data");
            FileSystemFacade.FileSystem.CreateDirectory(@"/data/snapshot1");
            FileSystemFacade.FileSystem.CreateDirectory(@"/data/snapshot2");
            
            // setup meta data files.
            OsmTiledDbOperations.SaveDbMeta(@"/data/snapshot1", new OsmTiledDbMeta()
            {
                Base = string.Empty,
                Timestamp = DateTime.Now,
                Type = OsmTiledDbType.Full,
                Zoom = 14
            });
            OsmTiledHistoryDbOperations.SaveDbMeta(@"/data", new OsmTiledHistoryDbMeta()
            {
                Latest = "/data/snapshot1/"
            });
            
            // load db.
            Assert.True(OsmTiledHistoryDb.TryLoad(@"/data", out var db));
            
            // reload db.
            Assert.False(db.TryReload());
        }
        
        [Test]
        public void OsmTiledHistoryDb_TryReload_NewData_ShouldLoadNewDbAndReturnTrue()
        {
            FileSystemFacade.FileSystem = new MockFileSystem(@"/");
            FileSystemFacade.FileSystem.CreateDirectory(@"/data");
            FileSystemFacade.FileSystem.CreateDirectory(@"/data/snapshot1");
            FileSystemFacade.FileSystem.CreateDirectory(@"/data/snapshot2");
            
            // setup meta data files.
            OsmTiledDbOperations.SaveDbMeta(@"/data/snapshot1", new OsmTiledDbMeta()
            {
                Base = string.Empty,
                Timestamp = DateTime.Now,
                Type = OsmTiledDbType.Full,
                Zoom = 12
            });
            OsmTiledHistoryDbOperations.SaveDbMeta(@"/data", new OsmTiledHistoryDbMeta()
            {
                Latest = "/data/snapshot1/"
            });
            
            // load db.
            Assert.True(OsmTiledHistoryDb.TryLoad(@"/data", out var db));
            
            // setup new meta data files.
            OsmTiledDbOperations.SaveDbMeta(@"/data/snapshot2", new OsmTiledDbMeta()
            {
                Base = string.Empty,
                Timestamp = DateTime.Now,
                Type = OsmTiledDbType.Full,
                Zoom = 12
            });
            OsmTiledHistoryDbOperations.SaveDbMeta(@"/data", new OsmTiledHistoryDbMeta()
            {
                Latest = "/data/snapshot2/"
            });
            
            // reload db.
            Assert.True(db.TryReload());
            Assert.AreEqual("/data/snapshot2/", db.Latest.Path);
        }
    }
}