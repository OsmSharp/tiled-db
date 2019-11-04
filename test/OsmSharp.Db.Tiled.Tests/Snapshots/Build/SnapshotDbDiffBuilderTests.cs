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

            var diff = db.BuildDiff("/data1", change);
            
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

            var diff = db.BuildDiff("/data1", change);
            
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

            var diff = db.BuildDiff("/data1", change);
            
            // check if the node is still there but it should be the new version.
            var node = diff.Get(OsmGeoType.Node, 0);
            Assert.Null(node);
            
            // check if the tiles are there.
            var tile = diff.GetTile(new Tile(8374, 5556, 14), OsmGeoType.Node);
            Assert.Null(tile);
            var tileList = tile.ToList();
            Assert.AreEqual(1, tileList.Count);
            node = tileList[0] as Node;
            Assert.NotNull(node);
            Assert.AreEqual(0, node.Id);
            Assert.AreEqual(2, node.ChangeSetId);
            Assert.AreEqual(2, node.Version);
        }
    }
}