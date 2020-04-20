using System.Collections.Generic;
using System.Threading.Tasks;
using NUnit.Framework;
using OsmSharp.Db.Tiled.OsmTiled.Tiles;

namespace OsmSharp.Db.Tiled.Tests.OsmTiled.Tiles
{
    [TestFixture]
    public class OsmDbTileTests
    {
        [Test]
        public async Task OsmDbTile_OneNode_Get_ShouldGetNode()
        {
            var data = BinaryStreamHelper.Create(new OsmGeo[] { new Node()
            {
                Id = 45158746,
                Latitude = 45.336701909968134,
                Longitude = 8.085937
            }});
            
            
            var osmDbTile = await OsmDbTile.BuildFromOsmBinaryStream(data);

            var osmGeo = osmDbTile.Get(OsmGeoType.Node, 45158746);
            Assert.NotNull(osmGeo);
            Assert.IsInstanceOf<Node>(osmGeo);
            Assert.AreEqual(45158746, osmGeo.Id);
        }
        
        [Test]
        public async Task OsmDbTile_OneNode_GetEnumerable_ShouldEnumerateNode()
        {
            var data = BinaryStreamHelper.Create(new OsmGeo[] { new Node()
            {
                Id = 45158746,
                Latitude = 45.336701909968134,
                Longitude = 8.085937
            }});
            
            
            var osmDbTile = await OsmDbTile.BuildFromOsmBinaryStream(data);

            var osmGeos = osmDbTile.Get();
            Assert.NotNull(osmGeos);
            foreach (var osmGeo in osmGeos)
            {
                Assert.NotNull(osmGeo);
                Assert.IsInstanceOf<Node>(osmGeo);
                Assert.AreEqual(45158746, osmGeo.Id);
            }
        }
        
        [Test]
        public async Task OsmDbTile_100Node_GetEnumerables_ShouldEnumerateNodesInOrder()
        {
            var nodes = new List<Node>();
            for (var i = 0; i < 100; i++)
            {
                nodes.Add(new Node()
                {
                    Id = 451746 + (i * 100),
                    Latitude = 45.336701909968134,
                    Longitude = 8.085937
                });
            }
            
            var data = BinaryStreamHelper.Create(nodes);
            
            var osmDbTile = await OsmDbTile.BuildFromOsmBinaryStream(data);

            var osmGeos = osmDbTile.Get();
            Assert.NotNull(osmGeos);
            var j = 0;
            foreach (var osmGeo in osmGeos)
            {
                Assert.NotNull(osmGeo);
                Assert.IsInstanceOf<Node>(osmGeo);
                Assert.AreEqual(451746 + (j * 100), osmGeo.Id);
                j++;
            }
        }
        
        [Test]
        public async Task OsmDbTile_100Nodes_Get_ShouldGetNodes()
        {
            var nodes = new List<Node>();
            for (var i = 0; i < 100; i++)
            {
                nodes.Add(new Node()
                {
                    Id = 451746 + (i * 100),
                    Latitude = 45.336701909968134,
                    Longitude = 8.085937
                });
            }
            
            var data = BinaryStreamHelper.Create(nodes);
            
            var osmDbTile = await OsmDbTile.BuildFromOsmBinaryStream(data);

            for (var i = 0; i < 100; i++)
            {
                var id = 451746 + (i * 100);
                var osmGeo = osmDbTile.Get(OsmGeoType.Node, id);
                
                Assert.NotNull(osmGeo);
                Assert.IsInstanceOf<Node>(osmGeo);
                Assert.AreEqual(id, osmGeo.Id);
            }
        }
        
        [Test]
        public async Task OsmDbTile_100Nodes_Get_NonExisting_ShouldGetNull()
        {
            var nodes = new List<Node>();
            for (var i = 0; i < 100; i++)
            {
                nodes.Add(new Node()
                {
                    Id = 451746 + (i * 100),
                    Latitude = 45.336701909968134,
                    Longitude = 8.085937
                });
            }
            
            var data = BinaryStreamHelper.Create(nodes);
            
            var osmDbTile = await OsmDbTile.BuildFromOsmBinaryStream(data);

            for (var i = 0; i < 100; i++)
            {
                var id = 451746 + (i * 100) + 5;
                var osmGeo = osmDbTile.Get(OsmGeoType.Node, id);
                
                Assert.Null(osmGeo);
            }
        }
        
        [Test]
        public async Task OsmDbTile_OneWay_Get_ShouldGetWay()
        {
            var data = BinaryStreamHelper.Create(new OsmGeo[] { new Way()
            {
                Id = 45158746
            }});
            
            var osmDbTile = await OsmDbTile.BuildFromOsmBinaryStream(data);

            var osmGeo = osmDbTile.Get(OsmGeoType.Way, 45158746);
            Assert.NotNull(osmGeo);
            Assert.IsInstanceOf<Way>(osmGeo);
            Assert.AreEqual(45158746, osmGeo.Id);
        }
        
        [Test]
        public async Task OsmDbTile_OneRelation_Get_ShouldGetRelation()
        {
            var data = BinaryStreamHelper.Create(new OsmGeo[] { new Way()
            {
                Id = 45158746
            }});
            
            var osmDbTile = await OsmDbTile.BuildFromOsmBinaryStream(data);

            var osmGeo = osmDbTile.Get(OsmGeoType.Way, 45158746);
            Assert.NotNull(osmGeo);
            Assert.IsInstanceOf<Way>(osmGeo);
            Assert.AreEqual(45158746, osmGeo.Id);
        }
        
        
        
        [Test]
        public async Task OsmDbTile_300OsmGeos_GetEnumerables_ShouldEnumerateInOrder()
        {
            var osmGeos = new List<OsmGeo>();
            for (var i = 0; i < 100; i++)
            {
                osmGeos.Add(new Node()
                {
                    Id = 451746 + (i * 100),
                    Latitude = 45.336701909968134,
                    Longitude = 8.085937
                });
            }
            for (var i = 0; i < 100; i++)
            {
                osmGeos.Add(new Way()
                {
                    Id = 61127 + (i * 100)
                });
            }
            for (var i = 0; i < 100; i++)
            {
                osmGeos.Add(new Relation()
                {
                    Id = 1132 + (i * 100)
                });
            }
            
            var data = BinaryStreamHelper.Create(osmGeos);
            
            var osmDbTile = await OsmDbTile.BuildFromOsmBinaryStream(data);

            var osmGeosOut = osmDbTile.Get();
            Assert.NotNull(osmGeosOut);
            var j = 0;
            foreach (var osmGeo in osmGeosOut)
            {
                Assert.NotNull(osmGeo);
                if (j < 100)
                {
                    Assert.IsInstanceOf<Node>(osmGeo);
                    Assert.AreEqual(451746 + (j * 100), osmGeo.Id);
                }
                else if (j < 200)
                {
                    Assert.IsInstanceOf<Way>(osmGeo);
                    Assert.AreEqual(61127 + ((j - 100) * 100), osmGeo.Id);
                }
                else
                {
                    Assert.IsInstanceOf<Relation>(osmGeo);
                    Assert.AreEqual(1132 + ((j - 200) * 100), osmGeo.Id);
                }
                
                j++;
            }
        }
        
        [Test]
        public async Task OsmDbTile_300OsmGeos_Get_ShouldGetOsmGeos()
        {
            var osmGeos = new List<OsmGeo>();
            for (var i = 0; i < 100; i++)
            {
                osmGeos.Add(new Node()
                {
                    Id = 451746 + (i * 100),
                    Latitude = 45.336701909968134,
                    Longitude = 8.085937
                });
            }
            for (var i = 0; i < 100; i++)
            {
                osmGeos.Add(new Way()
                {
                    Id = 61127 + (i * 100)
                });
            }
            for (var i = 0; i < 100; i++)
            {
                osmGeos.Add(new Relation()
                {
                    Id = 1132 + (i * 100)
                });
            }
            
            var data = BinaryStreamHelper.Create(osmGeos);
            
            var osmDbTile = await OsmDbTile.BuildFromOsmBinaryStream(data);

            for (var i = 0; i < 100; i++)
            {
                var id = 451746 + (i * 100);
                var osmGeo = osmDbTile.Get(OsmGeoType.Node, id);
                
                Assert.NotNull(osmGeo);
                Assert.IsInstanceOf<Node>(osmGeo);
                Assert.AreEqual(id, osmGeo.Id);
            }
            for (var i = 0; i < 100; i++)
            {
                var id = 61127 + (i * 100);
                var osmGeo = osmDbTile.Get(OsmGeoType.Way, id);
                
                Assert.NotNull(osmGeo);
                Assert.IsInstanceOf<Way>(osmGeo);
                Assert.AreEqual(id, osmGeo.Id);
            }
            for (var i = 0; i < 100; i++)
            {
                var id = 1132 + (i * 100);
                var osmGeo = osmDbTile.Get(OsmGeoType.Relation, id);
                
                Assert.NotNull(osmGeo);
                Assert.IsInstanceOf<Relation>(osmGeo);
                Assert.AreEqual(id, osmGeo.Id);
            }
        }
        
        [Test]
        public async Task OsmDbTile_300OsmGeos_Get_NonExisting_ShouldGetNull()
        {
            var osmGeos = new List<OsmGeo>();
            for (var i = 0; i < 100; i++)
            {
                osmGeos.Add(new Node()
                {
                    Id = 451746 + (i * 100),
                    Latitude = 45.336701909968134,
                    Longitude = 8.085937
                });
            }
            for (var i = 0; i < 100; i++)
            {
                osmGeos.Add(new Way()
                {
                    Id = 61127 + (i * 100)
                });
            }
            for (var i = 0; i < 100; i++)
            {
                osmGeos.Add(new Relation()
                {
                    Id = 1132 + (i * 100)
                });
            }
            
            var data = BinaryStreamHelper.Create(osmGeos);
            
            var osmDbTile = await OsmDbTile.BuildFromOsmBinaryStream(data);

            for (var i = 0; i < 100; i++)
            {
                var id = 451746 + (i * 100) + 5;
                var osmGeo = osmDbTile.Get(OsmGeoType.Node, id);
                
                Assert.Null(osmGeo);
            }
            for (var i = 0; i < 100; i++)
            {
                var id = 61127 + (i * 100) + 11;
                var osmGeo = osmDbTile.Get(OsmGeoType.Way, id);
                
                Assert.Null(osmGeo);
            }
            for (var i = 0; i < 100; i++)
            {
                var id = 1132 + (i * 100) + 89;
                var osmGeo = osmDbTile.Get(OsmGeoType.Relation, id);
                
                Assert.Null(osmGeo);
            }
        }
    }
}