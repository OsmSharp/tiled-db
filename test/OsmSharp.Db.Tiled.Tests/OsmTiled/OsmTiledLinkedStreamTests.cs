using System;
using System.IO;
using System.Linq;
using NUnit.Framework;
using OsmSharp.Db.Tiled.OsmTiled;
using OsmSharp.Db.Tiled.Tiles;
using OsmSharp.IO.Xml;

namespace OsmSharp.Db.Tiled.Tests.OsmTiled
{
    [TestFixture]
    public class OsmTiledLinkedStreamTests
    {
        [Test]
        public void OsmTiledLinkedStream_1ObjectOneTile_ShouldStoreObjectInTile()
        {
            var linkedStream = new OsmTiledLinkedStream(new MemoryStream());

            var node = new Node()
            {
                Id = 456414,
                Latitude = 50,
                Longitude = 4,
                ChangeSetId = 1,
                UserId = 1,
                UserName = "Ben",
                Visible = true,
                TimeStamp = DateTime.Now,
                Version = 1
            };
            var tile = Tile.ToLocalId(Tile.FromWorld(node.Longitude.Value, node.Latitude.Value, 14), 14);
            linkedStream.Append(tile, node);

            var nodes = linkedStream.GetForTile(tile).ToList();
            Assert.AreEqual(1, nodes.Count);
            Assert.AreEqual(node.Id, nodes[0].Id);
        }
        
        [Test]
        public void OsmTiledLinkedStream_2ObjectsOneTile_ShouldStoreObjectInTile()
        {
            var linkedStream = new OsmTiledLinkedStream(new MemoryStream());

            var tile = Tile.FromWorld(4, 50, 14);
            var tileId1 = Tile.ToLocalId(tile, 14);
            linkedStream.Append(tileId1, new Node()
            {
                Id = 456414,
                Latitude = 50,
                Longitude = 4,
                ChangeSetId = 1,
                UserId = 1,
                UserName = "Ben",
                Visible = true,
                TimeStamp = DateTime.Now,
                Version = 1
            });
            linkedStream.Append(tileId1, new Node()
            {
                Id = 456415,
                Latitude = 50,
                Longitude = 4,
                ChangeSetId = 1,
                UserId = 1,
                UserName = "Ben",
                Visible = true,
                TimeStamp = DateTime.Now,
                Version = 1
            });

            var nodes = linkedStream.GetForTile(tileId1).ToList();
            Assert.AreEqual(2, nodes.Count);
            Assert.AreEqual(456414, nodes[0].Id);
            Assert.AreEqual(456415, nodes[1].Id);
        }
        
        [Test]
        public void OsmTiledLinkedStream_1ObjectMultipleTiles_ShouldStoreObjectAllTiles()
        {
            var linkedStream = new OsmTiledLinkedStream(new MemoryStream());

            var tile = Tile.FromWorld(50, 4, 14);
            var tileId1 = Tile.ToLocalId(tile, 14);
            var tileId2 = Tile.ToLocalId(tile.x + 1, tile.y, 14);
            linkedStream.Append(new [] { tileId1, tileId2 }, new Way()
            {
                Id = 235189,
                ChangeSetId = 1,
                Nodes = new long[]
                {
                    456414, 456415
                },
                Tags = null,
                TimeStamp = DateTime.Now,
                UserId = 1,
                UserName = "Ben",
                Version = 1,
                Visible = true
            });

            var osmGeos = linkedStream.GetForTile(tileId1).ToList();
            Assert.AreEqual(1, osmGeos.Count);
            Assert.AreEqual(235189, osmGeos[0].Id);

            osmGeos = linkedStream.GetForTile(tileId2).ToList();
            Assert.AreEqual(1, osmGeos.Count);
            Assert.AreEqual(235189, osmGeos[0].Id);
        }
        
        [Test]
        public void OsmTiledLinkedStream_2ObjectMultipleTiles_ShouldStoreObjectAllTiles()
        {
            var linkedStream = new OsmTiledLinkedStream(new MemoryStream());

            var tile = Tile.FromWorld(50, 4, 14);
            var tileId1 = Tile.ToLocalId(tile, 14);
            var tileId2 = Tile.ToLocalId(tile.x + 1, tile.y, 14);
            linkedStream.Append(new [] { tileId1, tileId2 }, new Way()
            {
                Id = 235189,
                ChangeSetId = 1,
                Nodes = new long[]
                {
                    456414, 456415
                },
                Tags = null,
                TimeStamp = DateTime.Now,
                UserId = 1,
                UserName = "Ben",
                Version = 1,
                Visible = true
            });
            linkedStream.Append(new [] { tileId1, tileId2 }, new Way()
            {
                Id = 235190,
                ChangeSetId = 1,
                Nodes = new long[]
                {
                    456414, 456415
                },
                Tags = null,
                TimeStamp = DateTime.Now,
                UserId = 1,
                UserName = "Ben",
                Version = 1,
                Visible = true
            });

            var osmGeos = linkedStream.GetForTile(tileId1).ToList();
            Assert.AreEqual(2, osmGeos.Count);
            Assert.AreEqual(235189, osmGeos[0].Id);
            Assert.AreEqual(235190, osmGeos[1].Id);

            osmGeos = linkedStream.GetForTile(tileId2).ToList();
            Assert.AreEqual(2, osmGeos.Count);
            Assert.AreEqual(235189, osmGeos[0].Id);
            Assert.AreEqual(235190, osmGeos[1].Id);
        }
        
        [Test]
        public void OsmTiledLinkedStream_MultipleObjectMultipleTiles_ShouldStoreObjectsOnTiles()
        {
            var linkedStream = new OsmTiledLinkedStream(new MemoryStream());

            var node1 = new Node()
            {
                Id = 456414,
                Latitude = 50,
                Longitude = 4,
                ChangeSetId = 1,
                UserId = 1,
                UserName = "Ben",
                Visible = true,
                TimeStamp = DateTime.Now,
                Version = 1
            };
            var way1 = new Way()
            {
                Id = 235189,
                ChangeSetId = 1,
                Nodes = new long[]
                {
                    456414, 456415
                },
                Tags = null,
                TimeStamp = DateTime.Now,
                UserId = 1,
                UserName = "Ben",
                Version = 1,
                Visible = true
            };
            var relation1 = new Relation()
            {
                Id = 982313,
                ChangeSetId = 1,
                Members = new RelationMember[]
                {
                    new RelationMember()
                    {
                        Id = 456414,
                        Role = "",
                        Type = OsmGeoType.Node
                    },
                    new RelationMember()
                    {
                        Id = 456415,
                        Role = "",
                        Type = OsmGeoType.Node
                    },
                    new RelationMember()
                    {
                        Id = 235189,
                        Role = "",
                        Type = OsmGeoType.Way
                    }
                },
                Tags = null,
                TimeStamp = DateTime.Now,
                UserId = 1,
                UserName = "Ben",
                Version = 1,
                Visible = true
            };
            var tile = Tile.FromWorld(4, 50, 14);
            var tileId1 = Tile.ToLocalId(tile, 14);
            var tileId2 = Tile.ToLocalId(tile.x + 1, tile.y, 14);
            var tileId3 = Tile.ToLocalId(tile.x + 1, tile.y + 1, 14);
            var tileId4 = Tile.ToLocalId(tile.x - 1, tile.y, 14);
            var tileId5 = Tile.ToLocalId(tile.x - 1, tile.y - 1, 14);
            
            // store data.
            linkedStream.Append(new [] { tileId1 }, node1);
            linkedStream.Append(new [] { tileId1, tileId2 }, way1);
            linkedStream.Append(new [] { tileId1, tileId2, tileId3, tileId4, tileId5 }, relation1);

            var osmGeos = linkedStream.GetForTile(tileId1).ToList();
            Assert.AreEqual(3, osmGeos.Count);
            Assert.AreEqual(relation1.Id, osmGeos[2].Id);
            Assert.AreEqual(way1.Id, osmGeos[1].Id);
            Assert.AreEqual(node1.Id, osmGeos[0].Id);

            osmGeos = linkedStream.GetForTile(tileId2).ToList();
            Assert.AreEqual(2, osmGeos.Count);
            Assert.AreEqual(relation1.Id, osmGeos[1].Id);
            Assert.AreEqual(way1.Id, osmGeos[0].Id);

            osmGeos = linkedStream.GetForTile(tileId3).ToList();
            Assert.AreEqual(1, osmGeos.Count);
            Assert.AreEqual(relation1.Id, osmGeos[0].Id);

            osmGeos = linkedStream.GetForTile(tileId4).ToList();
            Assert.AreEqual(1, osmGeos.Count);
            Assert.AreEqual(relation1.Id, osmGeos[0].Id);

            osmGeos = linkedStream.GetForTile(tileId5).ToList();
            Assert.AreEqual(1, osmGeos.Count);
            Assert.AreEqual(relation1.Id, osmGeos[0].Id);
        }

        [Test]
        public void OsmTiledLinkedStream_MultipleObjectMultipleTiles_GetForMultipleTiles_ShouldEnumerateObjectsOnTiles()
        {
            var linkedStream = new OsmTiledLinkedStream(new MemoryStream());

            var node1 = new Node()
            {
                Id = 456414,
                Latitude = 50,
                Longitude = 4,
                ChangeSetId = 1,
                UserId = 1,
                UserName = "Ben",
                Visible = true,
                TimeStamp = DateTime.Now,
                Version = 1
            };
            var way1 = new Way()
            {
                Id = 235189,
                ChangeSetId = 1,
                Nodes = new long[]
                {
                    456414, 456415
                },
                Tags = null,
                TimeStamp = DateTime.Now,
                UserId = 1,
                UserName = "Ben",
                Version = 1,
                Visible = true
            };
            var relation1 = new Relation()
            {
                Id = 982313,
                ChangeSetId = 1,
                Members = new RelationMember[]
                {
                    new RelationMember()
                    {
                        Id = 456414,
                        Role = "",
                        Type = OsmGeoType.Node
                    },
                    new RelationMember()
                    {
                        Id = 456415,
                        Role = "",
                        Type = OsmGeoType.Node
                    },
                    new RelationMember()
                    {
                        Id = 235189,
                        Role = "",
                        Type = OsmGeoType.Way
                    }
                },
                Tags = null,
                TimeStamp = DateTime.Now,
                UserId = 1,
                UserName = "Ben",
                Version = 1,
                Visible = true
            };
            var tile = Tile.FromWorld(4, 50, 14);
            var tileId1 = Tile.ToLocalId(tile, 14);
            var tileId2 = Tile.ToLocalId(tile.x + 1, tile.y, 14);
            var tileId3 = Tile.ToLocalId(tile.x + 1, tile.y + 1, 14);
            var tileId4 = Tile.ToLocalId(tile.x - 1, tile.y, 14);
            var tileId5 = Tile.ToLocalId(tile.x - 1, tile.y - 1, 14);

            // store data.
            linkedStream.Append(new[] {tileId1}, node1);
            linkedStream.Append(new[] {tileId1, tileId2}, way1);
            linkedStream.Append(new[] {tileId1, tileId2, tileId3, tileId4, tileId5}, relation1);

            var osmGeos = linkedStream.GetForTiles(new[] {tileId2, tileId3}).ToList();
            Assert.AreEqual(2, osmGeos.Count);
            Assert.AreEqual(way1.Id, osmGeos[0].Id);
            Assert.AreEqual(relation1.Id, osmGeos[1].Id);

            osmGeos = linkedStream.GetForTiles(new[] {tileId1, tileId2, tileId3}).ToList();
            Assert.AreEqual(3, osmGeos.Count);
            Assert.AreEqual(node1.Id, osmGeos[0].Id);
            Assert.AreEqual(way1.Id, osmGeos[1].Id);
            Assert.AreEqual(relation1.Id, osmGeos[2].Id);
        }

//        [Test]
//        public void OsmTiledLinkedStream_2ObjectMultipleTiles_ReverseAll_ShouldNotChangeStorage()
//        {
//            var linkedStream = new OsmTiledLinkedStream(new MemoryStream());
//
//            var tile = Tile.FromWorld(50, 4, 14);
//            var tileId1 = Tile.ToLocalId(tile, 14);
//            var tileId2 = Tile.ToLocalId(tile.x + 1, tile.y, 14);
//            linkedStream.Append(new [] { tileId1, tileId2 }, new Way()
//            {
//                Id = 235189,
//                ChangeSetId = 1,
//                Nodes = new long[]
//                {
//                    456414, 456415
//                },
//                Tags = null,
//                TimeStamp = DateTime.Now,
//                UserId = 1,
//                UserName = "Ben",
//                Version = 1,
//                Visible = true
//            });
//            linkedStream.Append(new [] { tileId1, tileId2 }, new Way()
//            {
//                Id = 235190,
//                ChangeSetId = 1,
//                Nodes = new long[]
//                {
//                    456414, 456415
//                },
//                Tags = null,
//                TimeStamp = DateTime.Now,
//                UserId = 1,
//                UserName = "Ben",
//                Version = 1,
//                Visible = true
//            });
//            
//            linkedStream.ReverseAll();
//
//            var osmGeos = linkedStream.GetForTile(tileId1).ToList();
//            Assert.AreEqual(2, osmGeos.Count);
//            Assert.AreEqual(235189, osmGeos[0].Id);
//            Assert.AreEqual(235190, osmGeos[1].Id);
//
//            osmGeos = linkedStream.GetForTile(tileId2).ToList();
//            Assert.AreEqual(2, osmGeos.Count);
//            Assert.AreEqual(235189, osmGeos[0].Id);
//            Assert.AreEqual(235190, osmGeos[1].Id);
//        }
//        
//        [Test]
//        public void OsmTiledLinkedStream_MultipleObjectMultipleTiles_ReverseAll_ShouldNotChangeStorage()
//        {
//            var linkedStream = new OsmTiledLinkedStream(new MemoryStream());
//
//            var node1 = new Node()
//            {
//                Id = 456414,
//                Latitude = 50,
//                Longitude = 4,
//                ChangeSetId = 1,
//                UserId = 1,
//                UserName = "Ben",
//                Visible = true,
//                TimeStamp = DateTime.Now,
//                Version = 1
//            };
//            var way1 = new Way()
//            {
//                Id = 235189,
//                ChangeSetId = 1,
//                Nodes = new long[]
//                {
//                    456414, 456415
//                },
//                Tags = null,
//                TimeStamp = DateTime.Now,
//                UserId = 1,
//                UserName = "Ben",
//                Version = 1,
//                Visible = true
//            };
//            var relation1 = new Relation()
//            {
//                Id = 982313,
//                ChangeSetId = 1,
//                Members = new RelationMember[]
//                {
//                    new RelationMember()
//                    {
//                        Id = 456414,
//                        Role = "",
//                        Type = OsmGeoType.Node
//                    },
//                    new RelationMember()
//                    {
//                        Id = 456415,
//                        Role = "",
//                        Type = OsmGeoType.Node
//                    },
//                    new RelationMember()
//                    {
//                        Id = 235189,
//                        Role = "",
//                        Type = OsmGeoType.Way
//                    }
//                },
//                Tags = null,
//                TimeStamp = DateTime.Now,
//                UserId = 1,
//                UserName = "Ben",
//                Version = 1,
//                Visible = true
//            };
//            var tile = Tile.FromWorld(50, 4, 14);
//            var tileId1 = Tile.ToLocalId(tile, 14);
//            var tileId2 = Tile.ToLocalId(tile.x + 1, tile.y, 14);
//            var tileId3 = Tile.ToLocalId(tile.x + 1, tile.y + 1, 14);
//            var tileId4 = Tile.ToLocalId(tile.x - 1, tile.y, 14);
//            var tileId5 = Tile.ToLocalId(tile.x - 1, tile.y - 1, 14);
//            
//            // store data.
//            linkedStream.Append(new [] { tileId1 }, node1);
//            linkedStream.Append(new [] { tileId1, tileId2 }, way1);
//            linkedStream.Append(new [] { tileId1, tileId2, tileId3, tileId4, tileId5 }, relation1);
//            
//            linkedStream.ReverseAll();
//
//            var osmGeos = linkedStream.GetForTile(tileId1).ToList();
//            Assert.AreEqual(3, osmGeos.Count);
//            Assert.AreEqual(relation1.Id, osmGeos[2].Id);
//            Assert.AreEqual(way1.Id, osmGeos[1].Id);
//            Assert.AreEqual(node1.Id, osmGeos[0].Id);
//
//            osmGeos = linkedStream.GetForTile(tileId2).ToList();
//            Assert.AreEqual(2, osmGeos.Count);
//            Assert.AreEqual(relation1.Id, osmGeos[1].Id);
//            Assert.AreEqual(way1.Id, osmGeos[0].Id);
//
//            osmGeos = linkedStream.GetForTile(tileId3).ToList();
//            Assert.AreEqual(1, osmGeos.Count);
//            Assert.AreEqual(relation1.Id, osmGeos[0].Id);
//
//            osmGeos = linkedStream.GetForTile(tileId4).ToList();
//            Assert.AreEqual(1, osmGeos.Count);
//            Assert.AreEqual(relation1.Id, osmGeos[0].Id);
//
//            osmGeos = linkedStream.GetForTile(tileId5).ToList();
//            Assert.AreEqual(1, osmGeos.Count);
//            Assert.AreEqual(relation1.Id, osmGeos[0].Id);
//        }
    }
}