using NUnit.Framework;
using OsmSharp.Db.Tiled.IO;
using System;

namespace OsmSharp.Db.Tiled.Tests
{
    [TestFixture]
    public class DatabaseTests
    {
        /// <summary>
        /// Tests creating a new node in an empty database.
        /// </summary>
        [Test]
        public void TestEmptyCreateNode()
        {
            FileSystemFacade.FileSystem = new Mocks.MockFileSystem(@"C:\");
            FileSystemFacade.FileSystem.CreateDirectory(@"C:\data");

            var db = new Database(@"C:\data");
            db.CreateNode(new Node()
            {
                Id = -1,
                ChangeSetId = 2,
                Latitude = 50,
                Longitude = 4,
                UserId = 1,
                UserName = "Ben",
                Visible = true,
                TimeStamp = DateTime.Now,
                Version = 1
            });
        }
    }
}
