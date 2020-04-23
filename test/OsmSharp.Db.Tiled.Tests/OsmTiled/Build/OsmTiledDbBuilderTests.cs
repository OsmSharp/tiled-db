using System;
using System.Threading.Tasks;
using NUnit.Framework;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.OsmTiled;
using OsmSharp.Db.Tiled.OsmTiled.Build;
using OsmSharp.Db.Tiled.OsmTiled.Tiles;
using OsmSharp.Streams;

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
            await osmGeos.Build(@"/data", 14);

            // check files and paths.
            Assert.True(FileSystemFacade.FileSystem.DirectoryExists(@"/data"));
            
            // indexes should exist.
            Assert.True(FileSystemFacade.FileSystem.Exists("/data/data.db"));
            Assert.True(FileSystemFacade.FileSystem.Exists("/data/data.id.idx"));
            Assert.True(FileSystemFacade.FileSystem.Exists("/data/data.tile.idx"));
        }
    }
}