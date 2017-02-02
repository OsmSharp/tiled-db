using Anyways.Osm.TiledDb.Indexing;
using NUnit.Framework;

namespace Anyways.Osm.TiledDb.Tests.Indexing
{
    /// <summary>
    /// Contains tests for the one-to-one id map.
    /// </summary>
    [TestFixture]
    public class OneToManyIdMapTests
    {
        /// <summary>
        /// Tests creating a new map.
        /// </summary>
        [Test]
        public void TestNew()
        {
            var map = new OneToManyIdMap();

            var data = new ulong[1024];
            Assert.AreEqual(0, map.TryGet(0, ref data));
            Assert.AreEqual(0, map.TryGet(long.MaxValue - 1, ref data));
        }

        /// <summary>
        /// Tests adding and getting.
        /// </summary>
        [Test]
        public void TestAddGet()
        {
            var map = new OneToManyIdMap();
            
            map.Add(1, 10, 20, 30, 40, 50, 60);
            map.Add(10, 100, 200, 300, 400);
            map.Add(100, 1000, 2000, 3000);
            map.Add(1000, 10000, 20000);
            var data = new ulong[1024];
            Assert.AreEqual(6, map.TryGet(1, ref data));
            Assert.AreEqual(10, data[0]);
            Assert.AreEqual(20, data[1]);
            Assert.AreEqual(30, data[2]);
            Assert.AreEqual(40, data[3]);
            Assert.AreEqual(50, data[4]);
            Assert.AreEqual(60, data[5]);
            Assert.AreEqual(4, map.TryGet(10, ref data));
            Assert.AreEqual(100, data[0]);
            Assert.AreEqual(200, data[1]);
            Assert.AreEqual(300, data[2]);
            Assert.AreEqual(400, data[3]);
            Assert.AreEqual(3, map.TryGet(100, ref data));
            Assert.AreEqual(1000, data[0]);
            Assert.AreEqual(2000, data[1]);
            Assert.AreEqual(3000, data[2]);
            Assert.AreEqual(2, map.TryGet(1000, ref data));
            Assert.AreEqual(10000, data[0]);
            Assert.AreEqual(20000, data[1]);
        }
    }
}
