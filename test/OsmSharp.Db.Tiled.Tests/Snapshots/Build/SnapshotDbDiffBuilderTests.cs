using System;
using System.Linq;
using NUnit.Framework;
using OsmSharp.Changesets;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.Snapshots.Build;
using OsmSharp.Db.Tiled.Tests.Mocks;
using OsmSharp.Db.Tiled.Tiles;
using OsmSharp.Streams;

namespace OsmSharp.Db.Tiled.Tests.Snapshots.Build
{
    /// <summary>
    /// Contains builder tests.
    /// </summary>
    [TestFixture]
    public class SnapshotDbDiffBuilderTests
    {
        [Test]
        public void SnapshotDbDiffBuilder_OneNode_CreateAnotherNode_ShouldHaveTwoNodes()
        {
            FileSystemFacade.FileSystem = new MockFileSystem(@"/");
            FileSystemFacade.FileSystem.CreateDirectory(@"/data");

            // build the database.
            var osmGeos = new OsmGeo[]
            {
                new Node
                {
                    Id = 0,
                    Latitude = 50,
                    Longitude = 4,
                    ChangeSetId = 1,
                    UserId = 1,
                    UserName = "Ben",
                    Visible = true,
                    TimeStamp = DateTime.Now,
                    Version = 1
                }
            };
            var db = SnapshotDbFullBuilder.Build(
               new OsmEnumerableStreamSource(osmGeos), @"/data", 14);
            
            // build the diff.
            var change = new OsmChange
            {
                Create = new OsmGeo[]
                {
                    new Node
                    {
                        Id = 1,
                        Latitude = 51,
                        Longitude = 5,
                        ChangeSetId = 1,
                        UserId = 1,
                        UserName = "Ben",
                        Visible = true,
                        TimeStamp = DateTime.Now,
                        Version = 1
                    }
                }
            };

            var diff = db.BuildDiff(change, path: "/data-create");
            
            // check if the node are there.
            var node = diff.Get(OsmGeoType.Node, 0);
            Assert.NotNull(node);
            Assert.AreEqual(0, node.Id);
            node = diff.Get(OsmGeoType.Node, 1);
            Assert.NotNull(node);
            Assert.AreEqual(1, node.Id);
            
            // check if the tiles are there.
            var tile = diff.GetTile(new Tile(8374, 5556, 14), OsmGeoType.Node);
            Assert.NotNull(tile);
            var tileList = tile.ToList();
            Assert.AreEqual(1, tileList.Count);
            node = tileList[0] as Node;
            Assert.NotNull(node);
            Assert.AreEqual(0, node.Id);
            
            tile = diff.GetTile(new Tile(8419, 5484, 14), OsmGeoType.Node);
            Assert.NotNull(tile);
            tileList = tile.ToList();
            Assert.AreEqual(1, tileList.Count);
            node = tileList[0] as Node;
            Assert.NotNull(node);
            Assert.AreEqual(1, node.Id);
        }
        
        [Test]
        public void SnapshotDbDiffBuilder_OneNode_ModifyNode_ShouldHaveOneModifiedNode()
        {
            FileSystemFacade.FileSystem = new MockFileSystem(@"/");
            FileSystemFacade.FileSystem.CreateDirectory(@"/data");

            // build the database.
            var osmGeos = new OsmGeo[]
            {
                new Node
                {
                    Id = 0,
                    Latitude = 50,
                    Longitude = 4,
                    ChangeSetId = 1,
                    UserId = 1,
                    UserName = "Ben",
                    Visible = true,
                    TimeStamp = DateTime.Now,
                    Version = 1
                }
            };
            var db = SnapshotDbFullBuilder.Build(
               new OsmEnumerableStreamSource(osmGeos), @"/data", 14);
            
            // build the diff.
            var change = new OsmChange
            {
                Modify = new OsmGeo[]
                {
                    new Node
                    {
                        Id = 0,
                        Latitude = 50,
                        Longitude = 4,
                        ChangeSetId = 2,
                        UserId = 1,
                        UserName = "Ben",
                        Visible = true,
                        TimeStamp = DateTime.Now,
                        Version = 2
                    }
                }
            };

            var diff = db.BuildDiff(change, path: "/data-modify");
            
            // check if the node is still there but it should be the new version.
            var node = diff.Get(OsmGeoType.Node, 0);
            Assert.NotNull(node);
            Assert.AreEqual(0, node.Id);
            Assert.AreEqual(2, node.ChangeSetId);
            Assert.AreEqual(2, node.Version);
            
            // check if the tiles are there.
            var tile = diff.GetTile(new Tile(8374, 5556, 14), OsmGeoType.Node);
            Assert.NotNull(tile);
            var tileList = tile.ToList();
            Assert.AreEqual(1, tileList.Count);
            node = tileList[0] as Node;
            Assert.NotNull(node);
            Assert.AreEqual(0, node.Id);
            Assert.AreEqual(2, node.ChangeSetId);
            Assert.AreEqual(2, node.Version);
        }
        
        [Test]
        public void SnapshotDbDiffBuilder_OneNode_DeleteNode_ShouldHaveNoNodes()
        {
            FileSystemFacade.FileSystem = new MockFileSystem(@"/");
            FileSystemFacade.FileSystem.CreateDirectory(@"/data");

            // build the database.
            var osmGeos = new OsmGeo[]
            {
                new Node
                {
                    Id = 0,
                    Latitude = 50,
                    Longitude = 4,
                    ChangeSetId = 1,
                    UserId = 1,
                    UserName = "Ben",
                    Visible = true,
                    TimeStamp = DateTime.Now,
                    Version = 1
                }
            };
            var db = SnapshotDbFullBuilder.Build(
               new OsmEnumerableStreamSource(osmGeos), @"/data", 14);
            
            // build the diff.
            var change = new OsmChange
            {
                Delete = new OsmGeo[]
                {
                    new Node
                    {
                        Id = 0,
                        Latitude = 50,
                        Longitude = 4,
                        ChangeSetId = 1,
                        UserId = 1,
                        UserName = "Ben",
                        Visible = true,
                        TimeStamp = DateTime.Now,
                        Version = 1
                    }
                }
            };

            var diff = db.BuildDiff(change, path: "/data-delete");
            
            // check if the node is still there but it should be the new version.
            var node = diff.Get(OsmGeoType.Node, 0);
            Assert.Null(node);
            
            // check if the tiles are there.
            var tile = diff.GetTile(new Tile(8374, 5556, 14), OsmGeoType.Node);
            Assert.NotNull(tile);
            var tileList = tile.ToList();
            Assert.AreEqual(0, tileList.Count);
        }
                
        [Test]
        public void SnapshotDbDiffBuilder_OneWay_CreateAnotherWay_ShouldHaveTwoWaysAndAllNodes()
        {
            FileSystemFacade.FileSystem = new MockFileSystem(@"/");
            FileSystemFacade.FileSystem.CreateDirectory(@"/data");

            // build the database.
            var osmGeos = new OsmGeo[]
            {
                new Node
                {
                    Id = 0,
                    Latitude = 50,
                    Longitude = 4,
                    ChangeSetId = 1,
                    UserId = 1,
                    UserName = "Ben",
                    Visible = true,
                    TimeStamp = DateTime.Now,
                    Version = 1
                },
                new Node
                {
                    Id = 1,
                    Latitude = 50,
                    Longitude = 4,
                    ChangeSetId = 1,
                    UserId = 1,
                    UserName = "Ben",
                    Visible = true,
                    TimeStamp = DateTime.Now,
                    Version = 1
                },
                new Way()
                {
                    Id = 0,
                    ChangeSetId = 1,
                    UserId = 1,
                    UserName = "Ben",
                    Visible = true,
                    TimeStamp = DateTime.Now,
                    Version = 1,
                    Nodes = new long []
                    {
                        0, 1
                    }
                }
            };
            var db = SnapshotDbFullBuilder.Build(
               new OsmEnumerableStreamSource(osmGeos), @"/data", 14);
            
            // build the diff.
            var change = new OsmChange
            {
                Create = new OsmGeo[]
                {
                    new Way
                    {
                        Id = 1,
                        ChangeSetId = 2,
                        UserId = 1,
                        UserName = "Ben",
                        Visible = true,
                        TimeStamp = DateTime.Now,
                        Version = 1,
                        Nodes = new long []
                        {
                            1, 0
                        }
                    }
                }
            };

            var diff = db.BuildDiff(change, path: "/data-create-way");
            
            // check if the nodes are there.
            var node = diff.Get(OsmGeoType.Node, 0);
            Assert.NotNull(node);
            Assert.AreEqual(0, node.Id);
            node = diff.Get(OsmGeoType.Node, 1);
            Assert.NotNull(node);
            Assert.AreEqual(1, node.Id);
            
            // check if the ways are there.
            var way = diff.Get(OsmGeoType.Way, 0) as Way;
            Assert.NotNull(way);
            Assert.AreEqual(0, way.Id);
            Assert.AreEqual(1, way.ChangeSetId);
            Assert.NotNull(way.Nodes);
            Assert.AreEqual(0, way.Nodes[0]);
            Assert.AreEqual(1, way.Nodes[1]);
            way = diff.Get(OsmGeoType.Way, 1) as Way;
            Assert.NotNull(way);
            Assert.AreEqual(1, way.Id);
            Assert.AreEqual(2, way.ChangeSetId);
            Assert.NotNull(way.Nodes);
            Assert.AreEqual(1, way.Nodes[0]);
            Assert.AreEqual(0, way.Nodes[1]);
            
            // check if the tiles are there.
            var tile = diff.GetTile(new Tile(8374, 5556, 14), OsmGeoType.Way);
            Assert.NotNull(tile);
            var tileList = tile.ToList();
            way = tileList[0] as Way;
            Assert.NotNull(way);
            Assert.AreEqual(0,way.Id);
            way = tileList[1] as Way;
            Assert.NotNull(way);
            Assert.AreEqual(1,way.Id);
        }
                
        [Test]
        public void SnapshotDbDiffBuilder_OneWay_ModifyWay_ShouldOneModifiedWay()
        {
            FileSystemFacade.FileSystem = new MockFileSystem(@"/");
            FileSystemFacade.FileSystem.CreateDirectory(@"/data");

            // build the database.
            var osmGeos = new OsmGeo[]
            {
                new Node
                {
                    Id = 0,
                    Latitude = 50,
                    Longitude = 4,
                    ChangeSetId = 1,
                    UserId = 1,
                    UserName = "Ben",
                    Visible = true,
                    TimeStamp = DateTime.Now,
                    Version = 1
                },
                new Node
                {
                    Id = 1,
                    Latitude = 50,
                    Longitude = 4,
                    ChangeSetId = 1,
                    UserId = 1,
                    UserName = "Ben",
                    Visible = true,
                    TimeStamp = DateTime.Now,
                    Version = 1
                },
                new Way
                {
                    Id = 0,
                    ChangeSetId = 1,
                    UserId = 1,
                    UserName = "Ben",
                    Visible = true,
                    TimeStamp = DateTime.Now,
                    Version = 1,
                    Nodes = new long []
                    {
                        0, 1
                    }
                }
            };
            var db = SnapshotDbFullBuilder.Build(
               new OsmEnumerableStreamSource(osmGeos), @"/data", 14);
            
            // build the diff.
            var change = new OsmChange
            {
                Create = new OsmGeo[]
                {
                    new Way
                    {
                        Id = 0,
                        ChangeSetId = 2,
                        UserId = 1,
                        UserName = "Ben",
                        Visible = true,
                        TimeStamp = DateTime.Now,
                        Version = 1,
                        Nodes = new long []
                        {
                            1, 0
                        }
                    }
                }
            };

            var diff = db.BuildDiff(change, path: "/data-modify-way");
            
            // check if the nodes are there.
            var node = diff.Get(OsmGeoType.Node, 0);
            Assert.NotNull(node);
            Assert.AreEqual(0, node.Id);
            node = diff.Get(OsmGeoType.Node, 1);
            Assert.NotNull(node);
            Assert.AreEqual(1, node.Id);
            
            // check if the ways are there.
            var way = diff.Get(OsmGeoType.Way, 0) as Way;
            Assert.NotNull(way);
            Assert.AreEqual(0, way.Id);
            Assert.AreEqual(2, way.ChangeSetId);
            Assert.NotNull(way.Nodes);
            Assert.AreEqual(1, way.Nodes[0]);
            Assert.AreEqual(0, way.Nodes[1]);
            
            // check if the tiles are there.
            var tile = diff.GetTile(new Tile(8374, 5556, 14), OsmGeoType.Way);
            Assert.NotNull(tile);
            var tileList = tile.ToList();
            Assert.AreEqual(1, tileList.Count);
            way = tileList[0] as Way;
            Assert.NotNull(way);
            Assert.AreEqual(0, way.Id);
        }
        
        [Test]
        public void SnapshotDbDiffBuilder_OneWay_DeleteWay_ShouldHaveNoWays()
        {
            FileSystemFacade.FileSystem = new MockFileSystem(@"/");
            FileSystemFacade.FileSystem.CreateDirectory(@"/data");

            // build the database.
            var osmGeos = new OsmGeo[]
            {
                new Node
                {
                    Id = 0,
                    Latitude = 50,
                    Longitude = 4,
                    ChangeSetId = 1,
                    UserId = 1,
                    UserName = "Ben",
                    Visible = true,
                    TimeStamp = DateTime.Now,
                    Version = 1
                },
                new Node
                {
                    Id = 1,
                    Latitude = 50,
                    Longitude = 4,
                    ChangeSetId = 1,
                    UserId = 1,
                    UserName = "Ben",
                    Visible = true,
                    TimeStamp = DateTime.Now,
                    Version = 1
                },
                new Way
                {
                    Id = 0,
                    ChangeSetId = 1,
                    UserId = 1,
                    UserName = "Ben",
                    Visible = true,
                    TimeStamp = DateTime.Now,
                    Version = 1,
                    Nodes = new long []
                    {
                        0, 1
                    }
                }
            };
            var db = SnapshotDbFullBuilder.Build(
                new OsmEnumerableStreamSource(osmGeos), @"/data", 14);
            
            // build the diff.
            var change = new OsmChange
            {
                Delete = new OsmGeo[]
                {
                    new Way
                    {
                        Id = 0,
                        ChangeSetId = 1,
                        UserId = 1,
                        UserName = "Ben",
                        Visible = true,
                        TimeStamp = DateTime.Now,
                        Version = 1,
                        Nodes = new long []
                        {
                            0, 1
                        }
                    }
                }
            };

            var diff = db.BuildDiff(change, path: "/data-delete-way");
            
            // check if the node is still there but it should be the new version.
            var node = diff.Get(OsmGeoType.Way, 0);
            Assert.Null(node);
            
            // check if the tiles are there.
            var tile = diff.GetTile(new Tile(8374, 5556, 14), OsmGeoType.Way);
            Assert.NotNull(tile);
            var tileList = tile.ToList();
            Assert.AreEqual(0, tileList.Count);
        }
                
        [Test]
        public void SnapshotDbDiffBuilder_OneRelation_ModifyRelation_ShouldOneModifiedRelation()
        {
            FileSystemFacade.FileSystem = new MockFileSystem(@"/");
            FileSystemFacade.FileSystem.CreateDirectory(@"/data");

            // build the database.
            var osmGeos = new OsmGeo[]
            {
                new Node
                {
                    Id = 0,
                    Latitude = 50,
                    Longitude = 4,
                    ChangeSetId = 1,
                    UserId = 1,
                    UserName = "Ben",
                    Visible = true,
                    TimeStamp = DateTime.Now,
                    Version = 1
                },
                new Node
                {
                    Id = 1,
                    Latitude = 50,
                    Longitude = 4,
                    ChangeSetId = 1,
                    UserId = 1,
                    UserName = "Ben",
                    Visible = true,
                    TimeStamp = DateTime.Now,
                    Version = 1
                },
                new Way()
                {
                    Id = 0,
                    ChangeSetId = 1,
                    UserId = 1,
                    UserName = "Ben",
                    Visible = true,
                    TimeStamp = DateTime.Now,
                    Version = 1,
                    Nodes = new long []
                    {
                        0, 1
                    }
                },
                new Relation()
                {
                    Id = 0,
                    ChangeSetId = 1,
                    UserId = 1,
                    UserName = "Ben",
                    Visible = true,
                    TimeStamp = DateTime.Now,
                    Version = 1,
                    Members = new RelationMember []
                    {
                        new RelationMember(0, "none", OsmGeoType.Way), 
                        new RelationMember(1, "none", OsmGeoType.Node), 
                    }
                }
            };
            var db = SnapshotDbFullBuilder.Build(
               new OsmEnumerableStreamSource(osmGeos), @"/data", 14);
            
            // build the diff.
            var change = new OsmChange
            {
                Modify = new OsmGeo[]
                {
                    new Relation()
                    {
                        Id = 0,
                        ChangeSetId = 2,
                        UserId = 1,
                        UserName = "Ben",
                        Visible = true,
                        TimeStamp = DateTime.Now,
                        Version = 1,
                        Members = new RelationMember []
                        {
                            new RelationMember(0, "another-role", OsmGeoType.Way), 
                            new RelationMember(0, "none", OsmGeoType.Node), 
                        }
                    }
                }
            };

            var diff = db.BuildDiff(change, path: "/data-modify-relation");
            
            // check if the ways are there.
            var relation = diff.Get(OsmGeoType.Relation, 0) as Relation;
            Assert.NotNull(relation);
            Assert.AreEqual(0, relation.Id);
            Assert.AreEqual(2, relation.ChangeSetId);
            Assert.NotNull(relation.Members);
            Assert.AreEqual(0, relation.Members[0].Id);
            Assert.AreEqual("another-role", relation.Members[0].Role);
            Assert.AreEqual(0, relation.Members[1].Id);
            Assert.AreEqual("none", relation.Members[1].Role);
            
            // check if the tiles are there.
            var tile = diff.GetTile(new Tile(8374, 5556, 14), OsmGeoType.Relation);
            Assert.NotNull(tile);
            var tileList = tile.ToList();
            Assert.AreEqual(1, tileList.Count);
            relation = tileList[0] as Relation;
            Assert.NotNull(relation);
            Assert.AreEqual(0, relation.Id);
        }
                
        [Test]
        public void SnapshotDbDiffBuilder_OneRelation_CreateAnotherRelation_ShouldHaveTwoRelations()
        {
            FileSystemFacade.FileSystem = new MockFileSystem(@"/");
            FileSystemFacade.FileSystem.CreateDirectory(@"/data");

            // build the database.
            var osmGeos = new OsmGeo[]
            {
                new Node
                {
                    Id = 0,
                    Latitude = 50,
                    Longitude = 4,
                    ChangeSetId = 1,
                    UserId = 1,
                    UserName = "Ben",
                    Visible = true,
                    TimeStamp = DateTime.Now,
                    Version = 1
                },
                new Node
                {
                    Id = 1,
                    Latitude = 50,
                    Longitude = 4,
                    ChangeSetId = 1,
                    UserId = 1,
                    UserName = "Ben",
                    Visible = true,
                    TimeStamp = DateTime.Now,
                    Version = 1
                },
                new Way
                {
                    Id = 0,
                    ChangeSetId = 1,
                    UserId = 1,
                    UserName = "Ben",
                    Visible = true,
                    TimeStamp = DateTime.Now,
                    Version = 1,
                    Nodes = new long []
                    {
                        0, 1
                    }
                },
                new Relation()
                {
                    Id = 0,
                    ChangeSetId = 1,
                    UserId = 1,
                    UserName = "Ben",
                    Visible = true,
                    TimeStamp = DateTime.Now,
                    Version = 1,
                    Members = new RelationMember []
                    {
                        new RelationMember(0, "none", OsmGeoType.Way), 
                        new RelationMember(1, "none", OsmGeoType.Node), 
                    }
                }
            };
            var db = SnapshotDbFullBuilder.Build(
               new OsmEnumerableStreamSource(osmGeos), @"/data", 14);
            
            // build the diff.
            var change = new OsmChange
            {
                Create = new OsmGeo[]
                {
                    new Way
                    {
                        Id = 0,
                        ChangeSetId = 2,
                        UserId = 1,
                        UserName = "Ben",
                        Visible = true,
                        TimeStamp = DateTime.Now,
                        Version = 1,
                        Nodes = new long []
                        {
                            1, 0
                        }
                    }
                }
            };

            var diff = db.BuildDiff(change, path: "/data-modify-way");
            
            // check if the nodes are there.
            var node = diff.Get(OsmGeoType.Node, 0);
            Assert.NotNull(node);
            Assert.AreEqual(0, node.Id);
            node = diff.Get(OsmGeoType.Node, 1);
            Assert.NotNull(node);
            Assert.AreEqual(1, node.Id);
            
            // check if the ways are there.
            var way = diff.Get(OsmGeoType.Way, 0) as Way;
            Assert.NotNull(way);
            Assert.AreEqual(0, way.Id);
            Assert.AreEqual(2, way.ChangeSetId);
            Assert.NotNull(way.Nodes);
            Assert.AreEqual(1, way.Nodes[0]);
            Assert.AreEqual(0, way.Nodes[1]);
            
            // check if the tiles are there.
            var tile = diff.GetTile(new Tile(8374, 5556, 14), OsmGeoType.Way);
            Assert.NotNull(tile);
            var tileList = tile.ToList();
            Assert.AreEqual(1, tileList.Count);
            way = tileList[0] as Way;
            Assert.NotNull(way);
            Assert.AreEqual(0, way.Id);
        }
        
        [Test]
        public void SnapshotDbDiffBuilder_OneRelation_DeleteRelation_ShouldHaveNoRelations()
        {
            FileSystemFacade.FileSystem = new MockFileSystem(@"/");
            FileSystemFacade.FileSystem.CreateDirectory(@"/data");

            // build the database.
            var osmGeos = new OsmGeo[]
            {
                new Node
                {
                    Id = 0,
                    Latitude = 50,
                    Longitude = 4,
                    ChangeSetId = 1,
                    UserId = 1,
                    UserName = "Ben",
                    Visible = true,
                    TimeStamp = DateTime.Now,
                    Version = 1
                },
                new Node
                {
                    Id = 1,
                    Latitude = 50,
                    Longitude = 4,
                    ChangeSetId = 1,
                    UserId = 1,
                    UserName = "Ben",
                    Visible = true,
                    TimeStamp = DateTime.Now,
                    Version = 1
                },
                new Way
                {
                    Id = 0,
                    ChangeSetId = 1,
                    UserId = 1,
                    UserName = "Ben",
                    Visible = true,
                    TimeStamp = DateTime.Now,
                    Version = 1,
                    Nodes = new long []
                    {
                        0, 1
                    }
                },
                new Relation()
                {
                    Id = 0,
                    ChangeSetId = 1,
                    UserId = 1,
                    UserName = "Ben",
                    Visible = true,
                    TimeStamp = DateTime.Now,
                    Version = 1,
                    Members = new RelationMember []
                    {
                        new RelationMember(0, "none", OsmGeoType.Way), 
                        new RelationMember(1, "none", OsmGeoType.Node), 
                    }
                }
            };
            var db = SnapshotDbFullBuilder.Build(
                new OsmEnumerableStreamSource(osmGeos), @"/data", 14);
            
            // build the diff.
            var change = new OsmChange
            {
                Delete = new OsmGeo[]
                {
                    new Relation()
                    {
                        Id = 0,
                        ChangeSetId = 1,
                        UserId = 1,
                        UserName = "Ben",
                        Visible = true,
                        TimeStamp = DateTime.Now,
                        Version = 1,
                        Members = new RelationMember []
                        {
                            new RelationMember(0, "none", OsmGeoType.Way), 
                            new RelationMember(1, "none", OsmGeoType.Node), 
                        }
                    }
                }
            };

            var diff = db.BuildDiff(change, path: "/data-delete-way");
            
            // check if the node is still there but it should be the new version.
            var node = diff.Get(OsmGeoType.Relation, 0);
            Assert.Null(node);
            
            // check if the tiles are there.
            var tile = diff.GetTile(new Tile(8374, 5556, 14), OsmGeoType.Way);
            Assert.NotNull(tile);
            var tileList = tile.ToList();
            Assert.AreEqual(0, tileList.Count);
        }
    }
}