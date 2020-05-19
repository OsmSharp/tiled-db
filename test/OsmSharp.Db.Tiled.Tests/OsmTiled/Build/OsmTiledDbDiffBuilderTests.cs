using System;
using System.Linq;
using NUnit.Framework;
using OsmSharp.Changesets;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.OsmTiled;
using OsmSharp.Db.Tiled.OsmTiled.Build;
using OsmSharp.Db.Tiled.Tests.Mocks;
using OsmSharp.Db.Tiled.Tiles;

namespace OsmSharp.Db.Tiled.Tests.OsmTiled.Build
{
    [TestFixture]
    public class OsmTiledDbDiffBuilderTests
    {
        [Test]
        public void OsmTiledDbDiffBuilder_CreateOneNode_ShouldCreateOneNodeTile()
        {
            var root = $"/{Guid.NewGuid().ToString()}";
            
            FileSystemFacade.GetFileSystem = MockFileSystem.GetMockFileSystem;
            FileSystemFacade.FileSystem.CreateDirectory(root);
            FileSystemFacade.FileSystem.CreateDirectory($"{root}/original");
            FileSystemFacade.FileSystem.CreateDirectory($"{root}/diff");

            // build the database.
            var osmGeos = new OsmGeo[] { };
            osmGeos.Build($"{root}/original", 14);
            var osmTiledDb = new OsmTiledDb($"{root}/original");
            
            // apply diff.
            var diff = new OsmChange()
            {
                Create = new OsmGeo[]
                {
                    new Node()
                    {
                        Id = 4561327,
                        Version = 1,
                        Latitude = 50,
                        Longitude = 4,
                        TimeStamp = DateTime.Now
                    }
                }
            };
            osmTiledDb.BuildDiff(diff, $"{root}/diff");
            
            // indexes should exist.
            Assert.True(FileSystemFacade.FileSystem.Exists($"{root}/diff/data.db"));
            Assert.True(FileSystemFacade.FileSystem.Exists($"{root}/diff/data.id.idx"));
            Assert.True(FileSystemFacade.FileSystem.Exists($"{root}/diff/data.tile.idx"));
            
            // load diff and query.
            var diffDb = new OsmTiledDbDiff($"{root}/diff", _ => osmTiledDb);
            var node = diffDb.Get(OsmGeoType.Node, 4561327);
            Assert.NotNull(node);
        }
        
        [Test]
        public void OsmTiledDbDiffBuilder_DeleteNode_ShouldDeleteNode()
        {
            var root = $"/{Guid.NewGuid().ToString()}";
            
            FileSystemFacade.GetFileSystem = MockFileSystem.GetMockFileSystem;
            FileSystemFacade.FileSystem.CreateDirectory($"{root}/original");
            FileSystemFacade.FileSystem.CreateDirectory($"{root}/diff");

            // build the database.
            var osmGeos = new OsmGeo[] { 
                new Node()
                {
                    Id = 4561327,
                    Version = 1,
                    Latitude = 50,
                    Longitude = 4,
                    TimeStamp = DateTime.Now
                }
            };
            osmGeos.Build($"{root}/original", 14);
            var osmTiledDb = new OsmTiledDb($"{root}/original");
            
            // apply diff.
            var diff = new OsmChange()
            {
                Delete = new OsmGeo[]
                {
                    new Node()
                    {
                        Id = 4561327,
                        Version = 1,
                        Latitude = 50,
                        Longitude = 4,
                        TimeStamp = DateTime.Now.AddDays(1)
                    }
                }
            };
            osmTiledDb.BuildDiff(diff, $"{root}/diff");
            
            // indexes should exist.
            Assert.True(FileSystemFacade.FileSystem.Exists($"{root}/diff/data.db"));
            Assert.True(FileSystemFacade.FileSystem.Exists($"{root}/diff/data.id.idx"));
            Assert.True(FileSystemFacade.FileSystem.Exists($"{root}/diff/data.tile.idx"));
            
            // load diff and query.
            var diffDb = new OsmTiledDbDiff($"{root}/diff", _ => osmTiledDb);
            var node = diffDb.Get(OsmGeoType.Node, 4561327);
            Assert.Null(node);
        }
        
        [Test]
        public void OsmTiledDbDiffBuilder_ModifyNode_ShouldUpdateNode()
        {
            var root = $"/{Guid.NewGuid().ToString()}";
            
            FileSystemFacade.GetFileSystem = MockFileSystem.GetMockFileSystem;
            FileSystemFacade.FileSystem.CreateDirectory($"{root}/original");
            FileSystemFacade.FileSystem.CreateDirectory($"{root}/diff");

            // build the database.
            var osmGeos = new OsmGeo[] { 
                new Node()
                {
                    Id = 4561327,
                    Version = 1,
                    Latitude = 50,
                    Longitude = 4,
                    TimeStamp = DateTime.Now
                }
            };
            osmGeos.Build($"{root}/original");
            var osmTiledDb = new OsmTiledDb($"{root}/original");
            
            // apply diff.
            var diff = new OsmChange()
            {
                Modify = new OsmGeo[]
                {
                    new Node()
                    {
                        Id = 4561327,
                        Version = 1,
                        Latitude = 4,
                        Longitude = 50,
                        TimeStamp = DateTime.Now.AddDays(1)
                    }
                }
            };
            osmTiledDb.BuildDiff(diff, $"{root}/diff");
            
            // indexes should exist.
            Assert.True(FileSystemFacade.FileSystem.Exists($"{root}/diff/data.db"));
            Assert.True(FileSystemFacade.FileSystem.Exists($"{root}/diff/data.id.idx"));
            Assert.True(FileSystemFacade.FileSystem.Exists($"{root}/diff/data.tile.idx"));
            
            // load diff and query.
            var diffDb = new OsmTiledDbDiff($"{root}/diff", _ => osmTiledDb);
            var osmGeo = diffDb.Get(OsmGeoType.Node, 4561327);
            Assert.NotNull(osmGeo);
            if (osmGeo is Node node)
            {
                Assert.AreEqual(4, node.Latitude);
                Assert.AreEqual(50, node.Longitude);
            }
            else
            {
                Assert.Fail();
            }
            
            // node should only be in new tile.
            Assert.True(diffDb.Get(new []{ Tile.FromWorld(50, 4, diffDb.Zoom) }).Any());
            Assert.False(diffDb.Get(new []{ Tile.FromWorld(4, 50, diffDb.Zoom) }).Any());
        }
        
        [Test]
        public void OsmTiledDbDiffBuilder_CreateOneWay_NodesExist_ShouldCreateOneWayTile()
        {
            var root = $"/{Guid.NewGuid().ToString()}";
            
            FileSystemFacade.GetFileSystem = MockFileSystem.GetMockFileSystem;
            FileSystemFacade.FileSystem.CreateDirectory($"{root}/original");
            FileSystemFacade.FileSystem.CreateDirectory($"{root}/diff");

            // build the database.
            var osmGeos = new OsmGeo[] { 
                new Node()
                {
                    Id = 4561327,
                    Version = 1,
                    Latitude = 50,
                    Longitude = 4,
                    TimeStamp = DateTime.Now
                },
                new Node()
                {
                    Id = 4561328,
                    Version = 1,
                    Latitude = 50,
                    Longitude = 4,
                    TimeStamp = DateTime.Now
                }
            };
            osmGeos.Build($"{root}/original");
            var osmTiledDb = new OsmTiledDb($"{root}/original");
            
            // apply diff.
            var diff = new OsmChange()
            {
                Create = new OsmGeo[]
                {
                    new Way()
                    {
                        Id = 45327,
                        Version = 1,
                        Nodes = new [] { 4561327L, 4561328 },
                        TimeStamp = DateTime.Now.AddDays(1)
                    }
                }
            };
            osmTiledDb.BuildDiff(diff, $"{root}/diff");
            
            // load diff and query.
            var diffDb = new OsmTiledDbDiff($"{root}/diff", _ => osmTiledDb);
            var way = diffDb.Get(OsmGeoType.Way, 45327);
            Assert.NotNull(way);
            
            // nodes and their way should be in tile.
            var tileData = diffDb.Get(new[] {Tile.FromWorld(4, 50, diffDb.Zoom)}).ToList();
            Assert.AreEqual(3, tileData.Count);
            Assert.AreEqual(4561327, tileData[0].osmGeo.Id);
            Assert.AreEqual(4561328, tileData[1].osmGeo.Id);
            Assert.AreEqual(45327, tileData[2].osmGeo.Id);
        }
        
        [Test]
        public void OsmTiledDbDiffBuilder_CreateOneWay_NodesNew_ShouldCreateOneWayTile()
        {
            var root = $"/{Guid.NewGuid().ToString()}";
            
            FileSystemFacade.GetFileSystem = MockFileSystem.GetMockFileSystem;
            FileSystemFacade.FileSystem.CreateDirectory($"{root}/original");
            FileSystemFacade.FileSystem.CreateDirectory($"{root}/diff");

            // build the database.
            var osmGeos = new OsmGeo[] { };
            osmGeos.Build($"{root}/original");
            var osmTiledDb = new OsmTiledDb($"{root}/original");
            
            // apply diff.
            var diff = new OsmChange()
            {
                Create = new OsmGeo[]
                {
                    new Node()
                    {
                        Id = 4561327,
                        Version = 1,
                        Latitude = 50,
                        Longitude = 4,
                        TimeStamp = DateTime.Now
                    },
                    new Node()
                    {
                        Id = 4561328,
                        Version = 1,
                        Latitude = 50,
                        Longitude = 4,
                        TimeStamp = DateTime.Now
                    },
                    new Way()
                    {
                        Id = 45327,
                        Version = 1,
                        Nodes = new [] { 4561327L, 4561328 },
                        TimeStamp = DateTime.Now
                    }
                }
            };
            osmTiledDb.BuildDiff(diff, $"{root}/diff");
            
            // load diff and query.
            var diffDb = new OsmTiledDbDiff($"{root}/diff", _ => osmTiledDb);
            var way = diffDb.Get(OsmGeoType.Way, 45327);
            Assert.NotNull(way);
            
            // nodes and their way should be in tile.
            var tileData = diffDb.Get(new[] {Tile.FromWorld(4, 50, diffDb.Zoom)}).ToList();
            Assert.AreEqual(3, tileData.Count);
            Assert.AreEqual(4561327, tileData[0].osmGeo.Id);
            Assert.AreEqual(4561328, tileData[1].osmGeo.Id);
            Assert.AreEqual(45327, tileData[2].osmGeo.Id);
        }
        
        [Test]
        public void OsmTiledDbDiffBuilder_MoveNodesNewTile_ShouldMoveWayToNewTile()
        {
            var root = $"/{Guid.NewGuid().ToString()}";
            
            FileSystemFacade.GetFileSystem = MockFileSystem.GetMockFileSystem;
            FileSystemFacade.FileSystem.CreateDirectory($"{root}/original");
            FileSystemFacade.FileSystem.CreateDirectory($"{root}/diff");

            // build the database.
            var osmGeos = new OsmGeo[] 
            {
                new Node()
                {
                    Id = 4561327,
                    Version = 1,
                    Latitude = 50,
                    Longitude = 4,
                    TimeStamp = DateTime.Now
                },
                new Node()
                {
                    Id = 4561328,
                    Version = 1,
                    Latitude = 50,
                    Longitude = 4,
                    TimeStamp = DateTime.Now
                },
                new Way()
                {
                    Id = 45327,
                    Version = 1,
                    Nodes = new [] { 4561327L, 4561328 },
                    TimeStamp = DateTime.Now
                }
            };
            osmGeos.Build($"{root}/original");
            var osmTiledDb = new OsmTiledDb($"{root}/original");
            
            // apply diff.
            var diff = new OsmChange()
            {
                Modify = new OsmGeo[]
                {
                    new Node()
                    {
                        Id = 4561327,
                        Version = 2,
                        Latitude = 4,
                        Longitude = 50,
                        TimeStamp = DateTime.Now.AddDays(1)
                    },
                    new Node()
                    {
                        Id = 4561328,
                        Version = 2,
                        Latitude = 4,
                        Longitude = 50,
                        TimeStamp = DateTime.Now.AddDays(1)
                    }
                }
            };
            osmTiledDb.BuildDiff(diff, $"{root}/diff");
            
            // load diff and query.
            var diffDb = new OsmTiledDbDiff($"{root}/diff", _ => osmTiledDb);
            var way = diffDb.Get(OsmGeoType.Way, 45327);
            Assert.NotNull(way);
            
            // data should only be in new tile.
            Assert.True(diffDb.Get(new []{ Tile.FromWorld(50, 4, diffDb.Zoom) }).Any());
            Assert.False(diffDb.Get(new []{ Tile.FromWorld(4, 50, diffDb.Zoom) }).Any());
        }
    }
}