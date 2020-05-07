using System;
using NUnit.Framework;
using OsmSharp.Db.Tiled.OsmTiled.IO;

namespace OsmSharp.Db.Tiled.Tests.OsmTiled.IO
{
    [TestFixture]
    public class OsmTiledDbOperationsTests
    {
        [Test]
        public void OsmTiledDbOperations_IdToPath_ShouldGenerateUTCBasePath()
        {
            var path = OsmTiledDbOperations.IdToPath(
                new DateTime(2021, 05, 01, 15, 17, 16, DateTimeKind.Utc).ToUnixTime());
            Assert.AreEqual("20210501-151716", path);
        }
        
        [Test]
        public void OsmTiledDbOperations_IdFromPath_ShouldParseGenerateUTCBasePath()
        {
            Assert.True(OsmTiledDbOperations.TryIdFromPath("20210501-151716", out var id));
            Assert.AreEqual(new DateTime(2021, 05, 01, 15, 17, 16, DateTimeKind.Utc).ToUnixTime(), id);
        }
    }
}