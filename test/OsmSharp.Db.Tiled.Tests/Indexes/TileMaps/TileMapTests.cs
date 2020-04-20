using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using OsmSharp.Db.Tiled.Indexes.TileMaps;

namespace OsmSharp.Db.Tiled.Tests.Indexes.TileMaps
{
    [TestFixture]
    public class TileMapTests
    {
        [Test]
        public void TileMap_SettingElements_ShouldSetElements()
        {
            var sparseArray = new TileMap(451746 + (100 * 100) + 1);
            var ids = new List<long>();
            for (var i = 0; i < 100; i++)
            {
                var id = 451746 + (i * 100);
                sparseArray[id] = (uint)id.GetHashCode();
                ids.Add(id);
            }
            for (var i = 0; i < 100; i++)
            {
                var id = 61127 + (i * 100);
                sparseArray[id] = (uint)id.GetHashCode();
                ids.Add(id);
            }
            for (var i = 0; i < 100; i++)
            {
                var id = 1132 + (i * 100);
                sparseArray[id] = (uint)id.GetHashCode();
                ids.Add(id);
            }

            foreach (var id in ids)
            {
                var tile = sparseArray[id];
                Assert.AreEqual((uint)id.GetHashCode(), tile);
            }
        }

        [Test]
        public void TileMap_SerializeDeserialize_ShouldBeCopy()
        {
            var sparseArrayOriginal = new TileMap(451746 + (100 * 100) + 1);
            var ids = new List<long>();
            for (var i = 0; i < 100; i++)
            {
                var id = 451746 + (i * 100);
                sparseArrayOriginal[id] = (uint)id.GetHashCode();
                ids.Add(id);
            }
            for (var i = 0; i < 100; i++)
            {
                var id = 61127 + (i * 100);
                sparseArrayOriginal[id] = (uint)id.GetHashCode();
                ids.Add(id);
            }
            for (var i = 0; i < 100; i++)
            {
                var id = 1132 + (i * 100);
                sparseArrayOriginal[id] = (uint)id.GetHashCode();
                ids.Add(id);
            }

            var stream = new MemoryStream();
            sparseArrayOriginal.Serialize(stream);
            stream.Seek(0, SeekOrigin.Begin);

            var sparseArray = TileMap.Deserialize(stream);
            foreach (var id in ids)
            {
                var tile = sparseArray[id];
                Assert.AreEqual((uint)id.GetHashCode(), tile);
            }
        }
    }
}