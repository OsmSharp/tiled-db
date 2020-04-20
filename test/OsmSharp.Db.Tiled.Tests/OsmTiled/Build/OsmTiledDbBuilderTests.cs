using System;
using System.Threading.Tasks;
using NUnit.Framework;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.OsmTiled.Build;
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
                    Id = 0,
                    Latitude = 50,
                    Longitude = 4
                }
            };
            await OsmTiledDbBuilder.Build(
               new OsmEnumerableStreamSource(osmGeos), @"/data", 14);

            Assert.True(FileSystemFacade.FileSystem.DirectoryExists(@"/data"));
            
            // indexes should exist.
            Assert.True(FileSystemFacade.FileSystem.Exists("/data/nodes.idx"));
            Assert.True(FileSystemFacade.FileSystem.Exists("/data/ways.idx"));
            Assert.True(FileSystemFacade.FileSystem.Exists("/data/relations.idx"));

            // data should exist.
            Assert.True(FileSystemFacade.FileSystem.DirectoryExists(@"/data/14/8374"));
            Assert.True(FileSystemFacade.FileSystem.Exists(@"/data/14/8374/5556.osm.tile"));
        }
    }
}