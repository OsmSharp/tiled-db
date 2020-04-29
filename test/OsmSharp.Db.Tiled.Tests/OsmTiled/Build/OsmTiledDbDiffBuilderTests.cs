using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using OsmSharp.Changesets;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.OsmTiled;
using OsmSharp.Db.Tiled.OsmTiled.Build;
using OsmSharp.Db.Tiled.Tiles;

namespace OsmSharp.Db.Tiled.Tests.OsmTiled.Build
{
    [TestFixture]
    public class OsmTiledDbDiffBuilderTests
    {
        [Test]
        public async Task OsmTiledDbDiffBuilder_CreateOneNode_ShouldCreateOneNodeTile()
        {
            FileSystemFacade.FileSystem = new Mocks.MockFileSystem(@"/");
            FileSystemFacade.FileSystem.CreateDirectory(@"/original");
            FileSystemFacade.FileSystem.CreateDirectory(@"/diff");

            // build the database.
            var osmGeos = new OsmGeo[] { };
            await osmGeos.Build(@"/original", 14);
            var osmTiledDb = new OsmTiledDb(@"/original");
            
            // apply diff.
            var diff = new OsmChange()
            {
                Create = new OsmGeo[]
                {
                    new Node()
                    {
                        Id = 4561327,
                        Latitude = 50,
                        Longitude = 4
                    }
                }
            };
            osmTiledDb.ApplyChangSet(diff, @"/diff");
            
            // indexes should exist.
            Assert.True(FileSystemFacade.FileSystem.Exists("/diff/data.db"));
            Assert.True(FileSystemFacade.FileSystem.Exists("/diff/data.id.idx"));
            Assert.True(FileSystemFacade.FileSystem.Exists("/diff/data.tile.idx"));
            
            // load diff and query.
            var diffDb = new OsmTiledDbDiff(@"/diff");
            var node = diffDb.Get(OsmGeoType.Node, 4561327);
            Assert.NotNull(node);
        }
        
        [Test]
        public async Task OsmTiledDbDiffBuilder_DeleteNode_ShouldDeleteNode()
        {
            FileSystemFacade.FileSystem = new Mocks.MockFileSystem(@"/");
            FileSystemFacade.FileSystem.CreateDirectory(@"/original");
            FileSystemFacade.FileSystem.CreateDirectory(@"/diff");

            // build the database.
            var osmGeos = new OsmGeo[] { 
                new Node()
                {
                    Id = 4561327,
                    Latitude = 50,
                    Longitude = 4
                }
            };
            await osmGeos.Build(@"/original", 14);
            var osmTiledDb = new OsmTiledDb(@"/original");
            
            // apply diff.
            var diff = new OsmChange()
            {
                Delete = new OsmGeo[]
                {
                    new Node()
                    {
                        Id = 4561327,
                        Latitude = 50,
                        Longitude = 4
                    }
                }
            };
            osmTiledDb.ApplyChangSet(diff, @"/diff");
            
            // indexes should exist.
            Assert.True(FileSystemFacade.FileSystem.Exists("/diff/data.db"));
            Assert.True(FileSystemFacade.FileSystem.Exists("/diff/data.id.idx"));
            Assert.True(FileSystemFacade.FileSystem.Exists("/diff/data.tile.idx"));
            
            // load diff and query.
            var diffDb = new OsmTiledDbDiff(@"/diff");
            var node = diffDb.Get(OsmGeoType.Node, 4561327);
            Assert.Null(node);
        }
        
        [Test]
        public async Task OsmTiledDbDiffBuilder_ModifyNode_ShouldUpdateNode()
        {
            FileSystemFacade.FileSystem = new Mocks.MockFileSystem(@"/");
            FileSystemFacade.FileSystem.CreateDirectory(@"/original");
            FileSystemFacade.FileSystem.CreateDirectory(@"/diff");

            // build the database.
            var osmGeos = new OsmGeo[] { 
                new Node()
                {
                    Id = 4561327,
                    Latitude = 50,
                    Longitude = 4
                }
            };
            await osmGeos.Build(@"/original");
            var osmTiledDb = new OsmTiledDb(@"/original");
            
            // apply diff.
            var diff = new OsmChange()
            {
                Modify = new OsmGeo[]
                {
                    new Node()
                    {
                        Id = 4561327,
                        Latitude = 4,
                        Longitude = 50
                    }
                }
            };
            osmTiledDb.ApplyChangSet(diff, @"/diff");
            
            // indexes should exist.
            Assert.True(FileSystemFacade.FileSystem.Exists("/diff/data.db"));
            Assert.True(FileSystemFacade.FileSystem.Exists("/diff/data.id.idx"));
            Assert.True(FileSystemFacade.FileSystem.Exists("/diff/data.tile.idx"));
            
            // load diff and query.
            var diffDb = new OsmTiledDbDiff(@"/diff");
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
        public async Task OsmTiledDbDiffBuilder_CreateOneWay_NodesExist_ShouldCreateOneWayTile()
        {
            FileSystemFacade.FileSystem = new Mocks.MockFileSystem(@"/");
            FileSystemFacade.FileSystem.CreateDirectory(@"/original");
            FileSystemFacade.FileSystem.CreateDirectory(@"/diff");

            // build the database.
            var osmGeos = new OsmGeo[] { 
                new Node()
                {
                    Id = 4561327,
                    Latitude = 50,
                    Longitude = 4
                },
                new Node()
                {
                    Id = 4561328,
                    Latitude = 50,
                    Longitude = 4
                }
            };
            await osmGeos.Build(@"/original");
            var osmTiledDb = new OsmTiledDb(@"/original");
            
            // apply diff.
            var diff = new OsmChange()
            {
                Create = new OsmGeo[]
                {
                    new Way()
                    {
                        Id = 45327,
                        Nodes = new [] { 4561327L, 4561328 }
                    }
                }
            };
            osmTiledDb.ApplyChangSet(diff, @"/diff");
            
            // load diff and query.
            var diffDb = new OsmTiledDbDiff(@"/diff");
            var way = diffDb.Get(OsmGeoType.Way, 45327);
            Assert.NotNull(way);
        }
        
        [Test]
        public async Task OsmTiledDbDiffBuilder_CreateOneWay_NodesNew_ShouldCreateOneWayTile()
        {
            FileSystemFacade.FileSystem = new Mocks.MockFileSystem(@"/");
            FileSystemFacade.FileSystem.CreateDirectory(@"/original");
            FileSystemFacade.FileSystem.CreateDirectory(@"/diff");

            // build the database.
            var osmGeos = new OsmGeo[] { };
            await osmGeos.Build(@"/original");
            var osmTiledDb = new OsmTiledDb(@"/original");
            
            // apply diff.
            var diff = new OsmChange()
            {
                Create = new OsmGeo[]
                {
                    new Node()
                    {
                        Id = 4561327,
                        Latitude = 50,
                        Longitude = 4
                    },
                    new Node()
                    {
                        Id = 4561328,
                        Latitude = 50,
                        Longitude = 4
                    },
                    new Way()
                    {
                        Id = 45327,
                        Nodes = new [] { 4561327L, 4561328 }
                    }
                }
            };
            osmTiledDb.ApplyChangSet(diff, @"/diff");
            
            // load diff and query.
            var diffDb = new OsmTiledDbDiff(@"/diff");
            var way = diffDb.Get(OsmGeoType.Way, 45327);
            Assert.NotNull(way);
        }
        
        [Test]
        public async Task OsmTiledDbDiffBuilder_ModifyWayToNewTile_ShouldMoveWayToNewTile()
        {
            FileSystemFacade.FileSystem = new Mocks.MockFileSystem(@"/");
            FileSystemFacade.FileSystem.CreateDirectory(@"/original");
            FileSystemFacade.FileSystem.CreateDirectory(@"/diff");

            // build the database.
            var osmGeos = new OsmGeo[] 
            {
                new Node()
                {
                    Id = 4561327,
                    Latitude = 50,
                    Longitude = 4
                },
                new Node()
                {
                    Id = 4561328,
                    Latitude = 50,
                    Longitude = 4
                },
                new Way()
                {
                    Id = 45327,
                    Nodes = new [] { 4561327L, 4561328 }
                }
            };
            await osmGeos.Build(@"/original");
            var osmTiledDb = new OsmTiledDb(@"/original");
            
            // apply diff.
            var diff = new OsmChange()
            {
                Modify = new OsmGeo[]
                {
                    new Node()
                    {
                        Id = 4561327,
                        Latitude = 4,
                        Longitude = 50
                    },
                    new Node()
                    {
                        Id = 4561328,
                        Latitude = 4,
                        Longitude = 50
                    }
                },
                Create =  new OsmGeo[]
                {
                    new Way()
                    {
                        Id = 45327,
                        Nodes = new [] { 4561327L, 4561328 }
                    }
                }
            };
            osmTiledDb.ApplyChangSet(diff, @"/diff");
            
            // load diff and query.
            var diffDb = new OsmTiledDbDiff(@"/diff");
            var way = diffDb.Get(OsmGeoType.Way, 45327);
            Assert.NotNull(way);
            
            // data should only be in new tile.
            Assert.True(diffDb.Get(new []{ Tile.FromWorld(50, 4, diffDb.Zoom) }).Any());
            Assert.False(diffDb.Get(new []{ Tile.FromWorld(4, 50, diffDb.Zoom) }).Any());
        }
    }
}