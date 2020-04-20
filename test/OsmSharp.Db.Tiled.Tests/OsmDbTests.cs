//using System;
//using NUnit.Framework;
//using OsmSharp.Db.Tiled.IO;
//using OsmSharp.Db.Tiled.Snapshots;
//using OsmSharp.Db.Tiled.Snapshots.IO;
//using OsmSharp.Db.Tiled.Tests.Mocks;
//
//namespace OsmSharp.Db.Tiled.Tests
//{
//    [TestFixture]
//    public class OsmDbTests
//    {
//        [Test]
//        public void OsmDb_TryReload_NoNewData_ShouldDoNothingAndReturnFalse()
//        {
//            FileSystemFacade.FileSystem = new MockFileSystem(@"/");
//            FileSystemFacade.FileSystem.CreateDirectory(@"/data");
//            FileSystemFacade.FileSystem.CreateDirectory(@"/data/snapshot1");
//            FileSystemFacade.FileSystem.CreateDirectory(@"/data/snapshot2");
//            
//            // setup meta data files.
//            SnapshotDbOperations.SaveDbMeta(@"/data/snapshot1", new SnapshotDbMeta()
//            {
//                Base = string.Empty,
//                Timestamp = DateTime.Now,
//                Type = SnapshotDbType.Full,
//                Zoom = 12
//            });
//            OsmDbOperations.SaveDbMeta(@"/data", new OsmDbMeta()
//            {
//                Latest = "/data/snapshot1/"
//            });
//            
//            // load db.
//            Assert.True(OsmDb.TryLoad(@"/data", out var db));
//            
//            // reload db.
//            Assert.False(db.TryReload());
//        }
//        
//        [Test]
//        public void OsmDb_TryReload_NewData_ShouldLoadNewDbAndReturnTrue()
//        {
//            FileSystemFacade.FileSystem = new MockFileSystem(@"/");
//            FileSystemFacade.FileSystem.CreateDirectory(@"/data");
//            FileSystemFacade.FileSystem.CreateDirectory(@"/data/snapshot1");
//            FileSystemFacade.FileSystem.CreateDirectory(@"/data/snapshot2");
//            
//            // setup meta data files.
//            SnapshotDbOperations.SaveDbMeta(@"/data/snapshot1", new SnapshotDbMeta()
//            {
//                Base = string.Empty,
//                Timestamp = DateTime.Now,
//                Type = SnapshotDbType.Full,
//                Zoom = 12
//            });
//            OsmDbOperations.SaveDbMeta(@"/data", new OsmDbMeta()
//            {
//                Latest = "/data/snapshot1/"
//            });
//            
//            // load db.
//            Assert.True(OsmDb.TryLoad(@"/data", out var db));
//            
//            // setup new meta data files.
//            SnapshotDbOperations.SaveDbMeta(@"/data/snapshot2", new SnapshotDbMeta()
//            {
//                Base = string.Empty,
//                Timestamp = DateTime.Now,
//                Type = SnapshotDbType.Full,
//                Zoom = 12
//            });
//            OsmDbOperations.SaveDbMeta(@"/data", new OsmDbMeta()
//            {
//                Latest = "/data/snapshot2/"
//            });
//            
//            // reload db.
//            Assert.True(db.TryReload());
//            Assert.AreEqual("/data/snapshot2/", db.Latest.Path);
//        }
//    }
//}