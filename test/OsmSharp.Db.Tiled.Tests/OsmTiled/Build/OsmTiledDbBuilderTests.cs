using System;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.OsmTiled;
using OsmSharp.Db.Tiled.OsmTiled.Build;
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
        public async Task OsmTiledDbBuilder_OneNode_Build_ShouldCreateOneNodeTile()
        {
            FileSystemFacade.FileSystem = new Mocks.MockFileSystem(@"/");
            FileSystemFacade.FileSystem.CreateDirectory(@"/data");

            // build the database.
            var osmGeos = new OsmGeo[]
            {
                new Node()
                {
                    Id = 4561327,
                    Latitude = 50,
                    Longitude = 4
                }
            };
            osmGeos.Build(@"/data", 14);

            // check files and paths.
            Assert.True(FileSystemFacade.FileSystem.DirectoryExists(@"/data"));
            
            // indexes should exist.
            Assert.True(FileSystemFacade.FileSystem.Exists("/data/data.db"));
            Assert.True(FileSystemFacade.FileSystem.Exists("/data/data.id.idx"));
            Assert.True(FileSystemFacade.FileSystem.Exists("/data/data.tile.idx"));
        }
        
        [Test]
        public async Task OsmTiledDbBuilder_OneNode_DbShouldContainNode()
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
            osmGeos.Build(@"/original");
            var osmTiledDb = new OsmTiledDb(@"/original");
            
            var osmGeo = osmTiledDb.Get(OsmGeoType.Node, 4561327);
            Assert.NotNull(osmGeo);
            Assert.True(osmGeo is Node);
        }
        
        [Test]
        public async Task OsmTiledDbBuilder_OneNode_DbShouldContainNodeInTile()
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
            osmGeos.Build(@"/original");
            var osmTiledDb = new OsmTiledDb(@"/original");

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
        public async Task OsmTiledDbBuilder_OneNode_DbShouldContainNodeInTiles()
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
            osmGeos.Build(@"/original");
            var osmTiledDb = new OsmTiledDb(@"/original");

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