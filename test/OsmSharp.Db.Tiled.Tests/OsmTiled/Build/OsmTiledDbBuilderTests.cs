using System;
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
        public void OsmTiledDbBuilder_OneNode_Build_ShouldCreateOneNodeTile()
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
            OsmTiledDbBuilder.Build(
               new OsmEnumerableStreamSource(osmGeos), @"/data", 14);

            Assert.True(FileSystemFacade.FileSystem.DirectoryExists(@"/data"));

            // check if zoom-level dirs exist.
            Assert.True(FileSystemFacade.FileSystem.DirectoryExists(@"/data/0"));
            Assert.True(FileSystemFacade.FileSystem.DirectoryExists(@"/data/2"));
            Assert.True(FileSystemFacade.FileSystem.DirectoryExists(@"/data/4"));
            Assert.True(FileSystemFacade.FileSystem.DirectoryExists(@"/data/6"));
            Assert.True(FileSystemFacade.FileSystem.DirectoryExists(@"/data/8"));
            Assert.True(FileSystemFacade.FileSystem.DirectoryExists(@"/data/10"));
            Assert.True(FileSystemFacade.FileSystem.DirectoryExists(@"/data/12"));
            Assert.True(FileSystemFacade.FileSystem.DirectoryExists(@"/data/14"));

            // check per level for the proper files.
            Assert.True(FileSystemFacade.FileSystem.DirectoryExists(@"/data/0/0"));
            Assert.True(FileSystemFacade.FileSystem.Exists(@"/data/0/0/0.idx"));
            Assert.True(FileSystemFacade.FileSystem.DirectoryExists(@"/data/2/2"));
            Assert.True(FileSystemFacade.FileSystem.Exists(@"/data/2/2/1.idx"));
            Assert.True(FileSystemFacade.FileSystem.DirectoryExists(@"/data/4/8"));
            Assert.True(FileSystemFacade.FileSystem.Exists(@"/data/4/8/5.idx"));
            Assert.True(FileSystemFacade.FileSystem.DirectoryExists(@"/data/6/32"));
            Assert.True(FileSystemFacade.FileSystem.Exists(@"/data/6/32/21.idx"));
            Assert.True(FileSystemFacade.FileSystem.DirectoryExists(@"/data/8/130"));
            Assert.True(FileSystemFacade.FileSystem.Exists(@"/data/8/130/86.idx"));
            Assert.True(FileSystemFacade.FileSystem.DirectoryExists(@"/data/10/523"));
            Assert.True(FileSystemFacade.FileSystem.Exists(@"/data/10/523/347.idx"));
            Assert.True(FileSystemFacade.FileSystem.DirectoryExists(@"/data/12/2093"));
            Assert.True(FileSystemFacade.FileSystem.Exists(@"/data/12/2093/1389.idx"));
            Assert.True(FileSystemFacade.FileSystem.DirectoryExists(@"/data/14/8374"));
            Assert.True(FileSystemFacade.FileSystem.Exists(@"/data/14/8374/5556.nodes.osm.bin"));
        }
    }
}