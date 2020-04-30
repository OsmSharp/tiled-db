using System;
using System.Threading.Tasks;
using NUnit.Framework;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.OsmTiled;
using OsmSharp.Db.Tiled.OsmTiled.Build;
using OsmSharp.Streams;

namespace OsmSharp.Db.Tiled.Tests.OsmTiled
{
    [TestFixture]
    public class OsmTiledDbTests
    {
        [Test]
        public async Task OsmTiledDb_Get_ShouldGetObjects()
        {
            FileSystemFacade.FileSystem = new Mocks.MockFileSystem(@"/");

            // build the database.
            var osmGeos = new OsmGeo[]
            {
                new Node()
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
                },
                new Node()
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
                },
                new Way()
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
                },
                new Relation()
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
                }
            };
            
            FileSystemFacade.FileSystem.CreateDirectory(@"/OsmTiledDbTests");
            osmGeos.Build(@"/OsmTiledDbTests");
            
            var osmTiledDb = new OsmTiledDb("/OsmTiledDbTests");
            var node1 = osmTiledDb.Get(OsmGeoType.Node, 456414);
            Assert.NotNull(node1);
            var node2 = osmTiledDb.Get(OsmGeoType.Node, 456415);
            Assert.NotNull(node2);
            var way1 = osmTiledDb.Get(OsmGeoType.Way, 235189);
            Assert.NotNull(way1);
            var relation1 = osmTiledDb.Get(OsmGeoType.Relation, 982313);
            Assert.NotNull(relation1);
            
            var doesNotExist = osmTiledDb.Get(OsmGeoType.Relation, 982314);
            Assert.Null(doesNotExist);
            doesNotExist = osmTiledDb.Get(OsmGeoType.Node, 456413);
            Assert.Null(doesNotExist);
            doesNotExist = osmTiledDb.Get(OsmGeoType.Way, 235188);
            Assert.Null(doesNotExist);
        }
        
        [Test]
        public async Task OsmTiledDb_GetTile_ShouldGetAllObjectsInTile()
        {
            FileSystemFacade.FileSystem = new Mocks.MockFileSystem(@"/");

            // build the database.
            var osmGeos = new OsmGeo[]
            {
                new Node()
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
                },
                new Node()
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
                },
                new Way()
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
                },
                new Relation()
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
                }
            };
            
            FileSystemFacade.FileSystem.CreateDirectory(@"/OsmTiledDbTests");
            osmGeos.Build(@"/OsmTiledDbTests", 14);
            
            // 14/8374/5556.osm.tile
            var osmTiledDb = new OsmTiledDb("/OsmTiledDbTests");

            var tile = osmTiledDb.Get(new (uint x, uint y) [] { (8374, 5556) });
            Assert.NotNull(tile);

            using var enumerator = tile.GetEnumerator();
            Assert.True(enumerator.MoveNext());
            Assert.NotNull(enumerator.Current);
            Assert.AreEqual(456414, enumerator.Current.osmGeo.Id);
            Assert.AreEqual(OsmGeoType.Node, enumerator.Current.osmGeo.Type);
            Assert.True(enumerator.MoveNext());
            Assert.NotNull(enumerator.Current);
            Assert.AreEqual(456415, enumerator.Current.osmGeo.Id);
            Assert.AreEqual(OsmGeoType.Node, enumerator.Current.osmGeo.Type);
            Assert.True(enumerator.MoveNext());
            Assert.NotNull(enumerator.Current);
            Assert.AreEqual(235189, enumerator.Current.osmGeo.Id);
            Assert.AreEqual(OsmGeoType.Way, enumerator.Current.osmGeo.Type);
            Assert.True(enumerator.MoveNext());
            Assert.NotNull(enumerator.Current);
            Assert.AreEqual(982313, enumerator.Current.osmGeo.Id);
            Assert.AreEqual(OsmGeoType.Relation, enumerator.Current.osmGeo.Type);
        }
    }
}