using System;
using NUnit.Framework;
using OsmSharp.Changesets;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.Snapshots.Build;
using OsmSharp.Db.Tiled.Tests.Mocks;
using OsmSharp.Streams;

namespace OsmSharp.Db.Tiled.Tests.Snapshots.Build
{
    /// <summary>
    /// Contains builder tests.
    /// </summary>
    [TestFixture]
    public class SnapshotDbDiffBuilderTests
    {
        /// <summary>
        /// Tests building a database.
        /// </summary>
        [Test]
        public void SnapshotDbDiffBuilder_OneNode_UpdateOneNode_Should()
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
        }
    }
}