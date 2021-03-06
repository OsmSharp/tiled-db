using System;
using System.Threading.Tasks;
using NUnit.Framework;
using OsmSharp.Changesets;
using OsmSharp.Db.Tiled.Build;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.OsmTiled;
using OsmSharp.Db.Tiled.OsmTiled.IO;
using OsmSharp.Db.Tiled.Tests.Mocks;

namespace OsmSharp.Db.Tiled.Tests
{
    [TestFixture]
    public class OsmTiledHistoryDbTests
    {
        [Test]
        public void OsmTiledHistoryDb_Create_ShouldCreateNew()
        {
            var root = $"/{Guid.NewGuid().ToString()}";
            
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
                }
            };
            
            FileSystemFacade.GetFileSystem = MockFileSystem.GetMockFileSystem;
            FileSystemFacade.FileSystem.CreateDirectory($"{root}/data");

            var newDb = OsmTiledHistoryDb.Create($"{root}/data", osmGeos);
            
            Assert.NotNull(newDb.Latest);
        }
        
        [Test]
        public void OsmTiledHistoryDb_TryReload_NoNewData_ShouldDoNothingAndReturnFalse()
        {
            var root = $"/{Guid.NewGuid().ToString()}";
            
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
                }
            };
            
            FileSystemFacade.GetFileSystem = MockFileSystem.GetMockFileSystem;
            FileSystemFacade.FileSystem.CreateDirectory($"{root}/data");

            var db = OsmTiledHistoryDb.Create($"{root}/data", osmGeos);
            
            // reload db.
            Assert.False(db.TryReloadLatest());
        }
        
        [Test]
        public void OsmTiledHistoryDb_TryReload_NewData_ShouldLoadNewDbAndReturnTrue()
        {
            var root = $"/{Guid.NewGuid().ToString()}";
            
            FileSystemFacade.GetFileSystem = MockFileSystem.GetMockFileSystem;
            FileSystemFacade.FileSystem.CreateDirectory($"{root}/data");

            var db = OsmTiledHistoryDb.Create($"{root}/data", new OsmGeo[]
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
                }
            });
            
            // update db without using the db method (as if it was updated out of process).
            var tiledDb = new OsmGeo[]
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
                    TimeStamp = DateTime.Now.AddDays(1),
                    Version = 2
                }
            }.Add($"{root}/data");
            
            // reload db.
            Assert.True(db.TryReloadLatest());
        }
        
        [Test]
        public void OsmTiledHistoryDb_GetOn_ShouldGetDbValidForTimeStamp()
        {
            var root = $"/{Guid.NewGuid().ToString()}";
            
            FileSystemFacade.GetFileSystem = MockFileSystem.GetMockFileSystem;
            FileSystemFacade.FileSystem.CreateDirectory($"{root}/data");

            var db = OsmTiledHistoryDb.Create($"{root}/data", new OsmGeo[]
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
                    TimeStamp = new DateTime(2020, 01, 01, 0, 0, 0, DateTimeKind.Utc),
                    Version = 1
                }
            });
            
            // update db without using the db method (as if it was updated out of process).
            db.Add(new OsmGeo[]
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
                    TimeStamp = new DateTime(2021, 01, 01, 0, 0, 0, DateTimeKind.Utc),
                    Version = 2
                }
            });
            
            // update db without using the db method (as if it was updated out of process).
            db.Add(new OsmGeo[]
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
                    TimeStamp = new DateTime(2022, 01, 01, 0, 0, 0, DateTimeKind.Utc),
                    Version = 3
                }
            });
            
            // get db for anywhere in 2020, should return snapshot with timestamp 2021.
            var tiledDb = db.GetOn(new DateTime(2020, 4, 5, 0, 0, 0, DateTimeKind.Utc));
            Assert.NotNull(tiledDb);
            Assert.AreEqual(new DateTime(2021, 01, 01, 0, 0, 0, DateTimeKind.Utc).ToUnixTime(),
                tiledDb.Id);
            
            // get db for anywhere in 2021, should return snapshot with timestamp 2022.
            tiledDb = db.GetOn(new DateTime(2021, 4, 5, 0, 0, 0, DateTimeKind.Utc));
            Assert.NotNull(tiledDb);
            Assert.AreEqual(new DateTime(2022, 01, 01, 0, 0, 0, DateTimeKind.Utc).ToUnixTime(),
                tiledDb.Id);
            
            // get db for anywhere in 2022, should return null.
            tiledDb = db.GetOn(new DateTime(2022, 4, 5, 0, 0, 0, DateTimeKind.Utc));
            Assert.Null(tiledDb);
            
            // get db for before in 2020, should return snapshot with timestamp 2020.
            tiledDb = db.GetOn(new DateTime(2018, 4, 5, 0, 0, 0, DateTimeKind.Utc));
            Assert.NotNull(tiledDb);
            Assert.AreEqual(new DateTime(2020, 01, 01, 0, 0, 0, DateTimeKind.Utc).ToUnixTime(),
                tiledDb.Id);
        }
        
        [Test]
        public void OsmTiledHistoryDb_TakeSnapshot_ShouldCreateSnapshot()
        {
            var root = $"/{Guid.NewGuid().ToString()}";
            
            FileSystemFacade.GetFileSystem = MockFileSystem.GetMockFileSystem;
            FileSystemFacade.FileSystem.CreateDirectory($"{root}/data");

            var db = OsmTiledHistoryDb.Create($"{root}/data", new OsmGeo[]
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
                    TimeStamp = new DateTime(2020, 01, 01, 0, 0, 0, DateTimeKind.Utc),
                    Version = 1
                }
            });
            
            // update db without using the db method (as if it was updated out of process).
            db.ApplyDiff(new OsmChange()
            {
                Modify =
                    new OsmGeo[]
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
                            TimeStamp = new DateTime(2020, 01, 01, 1, 0, 0, DateTimeKind.Utc),
                            Version = 2
                        }
                    }
            });
            
            // update db without using the db method (as if it was updated out of process).
            db.ApplyDiff(new OsmChange()
            {
                Modify =
                    new OsmGeo[]
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
                            TimeStamp = new DateTime(2020, 01, 01, 2, 0, 0, DateTimeKind.Utc),
                            Version = 3
                        }
                    }
            });
            
            // take a snapshot.
            var snapshot = db.TakeSnapshot(new DateTime(2020, 01, 01, 2, 0, 0, DateTimeKind.Utc), 
                new TimeSpan(1, 30, 0));
        }
    }
}