using System;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.OsmTiled;
using OsmSharp.Db.Tiled.OsmTiled.Build;
using OsmSharp.Db.Tiled.Tests.Mocks;
using OsmSharp.Db.Tiled.Tiles;

namespace OsmSharp.Db.Tiled.Tests.OsmTiled.Build
{
    [TestFixture]
    public class OsmTiledDbBuilderTests
    {
        /// <summary>
        /// Tests building a database.
        /// </summary>
        [Test]
        public void OsmTiledDbBuilder_OneNode_Build_ShouldCreateOneNodeTile()
        {
            var root = $"/{Guid.NewGuid().ToString()}";
            
            FileSystemFacade.GetFileSystem = MockFileSystem.GetMockFileSystem;
            FileSystemFacade.FileSystem.CreateDirectory($"{root}/data");

            // build the database.
            var osmGeos = new OsmGeo[]
            {
                new Node()
                {
                    Id = 4561327,
                    Version = 1,
                    Latitude = 50,
                    Longitude = 4
                }
            };
            osmGeos.Build($"{root}/data", 14);

            // check files and paths.
            Assert.True(FileSystemFacade.FileSystem.DirectoryExists($"{root}/data"));
            
            // indexes should exist.
            Assert.True(FileSystemFacade.FileSystem.Exists($"{root}/data/data.db"));
            Assert.True(FileSystemFacade.FileSystem.Exists($"{root}/data/data.id.idx"));
            Assert.True(FileSystemFacade.FileSystem.Exists($"{root}/data/data.tile.idx"));
        }
        
        [Test]
        public void OsmTiledDbBuilder_OneNode_DbShouldContainNode()
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
                    Longitude = 4
                }
            };
            osmGeos.Build($"{root}/original");
            var osmTiledDb = new OsmTiledDb($"{root}/original");
            
            var osmGeo = osmTiledDb.Get(OsmGeoType.Node, 4561327);
            Assert.NotNull(osmGeo);
            Assert.True(osmGeo is Node);
        }
        
        [Test]
        public void OsmTiledDbBuilder_OneNode_DbShouldContainNodeInTile()
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
                    Longitude = 4
                }
            };
            osmGeos.Build($"{root}/original");
            var osmTiledDb = new OsmTiledDb($"{root}/original");

            var tileOsmGeos = osmTiledDb.Get(new (uint x, uint y)[]
            {
                Tile.FromWorld(4, 50, osmTiledDb.Zoom)
            });
            Assert.NotNull(tileOsmGeos);
            var osmGeo = tileOsmGeos.FirstOrDefault();
            Assert.NotNull(osmGeo);
            Assert.True(osmGeo.osmGeo is Node);
        }
        
        [Test]
        public void OsmTiledDbBuilder_OneNode_DbShouldContainNodeInTiles()
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
                    Longitude = 4
                }
            };
            osmGeos.Build($"{root}/original");
            var osmTiledDb = new OsmTiledDb($"{root}/original");

            var tileOsmGeos = osmTiledDb.Get(new (uint x, uint y)[]
            {
                Tile.FromWorld(4, 50, osmTiledDb.Zoom),
                Tile.FromWorld(4, 50, osmTiledDb.Zoom)
            });
            Assert.NotNull(tileOsmGeos);
            var osmGeo = tileOsmGeos.FirstOrDefault();
            Assert.NotNull(osmGeo);
            Assert.True(osmGeo.osmGeo is Node);
        }
    }
}