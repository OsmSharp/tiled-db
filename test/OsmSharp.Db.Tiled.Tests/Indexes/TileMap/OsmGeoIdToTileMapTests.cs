using System.Collections.Generic;
using System.IO;
using System.Linq;
using NUnit.Framework;
using OsmSharp.Db.Tiled.Indexes.TileMap;

namespace OsmSharp.Db.Tiled.Tests.Indexes.TileMap
{
    [TestFixture]
    public class OsmGeoIdToTileMapTests
    {
        [Test]
        public void OsmGeoIdToTileMap_SettingElements_ShouldSetElements()
        {
            IEnumerable<uint> Tiles(long localI)
            {
                var count = (localI % 20) + 1;
                for (var c = 0; c < count; c++)
                {
                    yield return (uint)(localI + c).GetHashCode();
                }
            }
            
            var tileMap = new OsmGeoIdToTileMap();
            var ids = new List<long>();
            for (var i = 0; i < 100; i++)
            {
                var id = 451746 + (i * 100);
                tileMap.Add(id, Tiles(id));
                ids.Add(id);
            }
            for (var i = 0; i < 100; i++)
            {
                var id = 61127 + (i * 100);
                tileMap.Add(id, Tiles(id));
                ids.Add(id);
            }
            for (var i = 0; i < 100; i++)
            {
                var id = 1132 + (i * 100);
                tileMap.Add(id, Tiles(id));
                ids.Add(id);
            }

            foreach (var id in ids)
            {
                var expectedTiles = Tiles(id);
                var tiles = tileMap.Get(id);
                CollectionAssert.AreEquivalent(expectedTiles, tiles);
            }
        }
        
        [Test]
        public void OsmGeoIdToTileMap_SerializeDeserialize_ShouldBeCopy()
        {
            IEnumerable<uint> Tiles(long localI)
            {
                var count = (localI % 20) + 1;
                for (var c = 0; c < count; c++)
                {
                    yield return (uint)(localI + c).GetHashCode();
                }
            }
            
            var tileMapOriginal = new OsmGeoIdToTileMap();
            var ids = new List<long>();
            for (var i = 0; i < 100; i++)
            {
                var id = 451746 + (i * 100);
                tileMapOriginal.Add(id, Tiles(id));
                ids.Add(id);
            }
            for (var i = 0; i < 100; i++)
            {
                var id = 61127 + (i * 100);
                tileMapOriginal.Add(id, Tiles(id));
                ids.Add(id);
            }
            for (var i = 0; i < 100; i++)
            {
                var id = 1132 + (i * 100);
                tileMapOriginal.Add(id, Tiles(id));
                ids.Add(id);
            }
            
            var stream = new MemoryStream();
            tileMapOriginal.Serialize(stream);
            stream.Seek(0, SeekOrigin.Begin);

            var tileMap = OsmGeoIdToTileMap.Deserialize(stream);

            foreach (var id in ids)
            {
                var expectedTiles = Tiles(id);
                var tiles = tileMap.Get(id);
                CollectionAssert.AreEquivalent(expectedTiles, tiles);
            }
        }
    }
}