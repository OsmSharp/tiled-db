using Anyways.Osm.TiledDb.Indexing;
using NUnit.Framework;

namespace Anyways.Osm.TiledDb.Tests.Indexing
{
    /// <summary>
    /// Contains tests for the one-to-one id map.
    /// </summary>
    [TestFixture]
    public class OneToOneIdMapTests
    {
        /// <summary>
        /// Tests creating a new map.
        /// </summary>
        [Test]
        public void TestNew()
        {
            var map = new OneToOneIdMap();

            Assert.AreEqual(false, map.IsReadonly);
            Assert.AreEqual(ulong.MaxValue, map.Get(0));
            Assert.AreEqual(ulong.MaxValue, map.Get(long.MaxValue - 1));
        }

        /// <summary>
        /// Tests adding and getting.
        /// </summary>
        [Test]
        public void TestAddGet()
        {
            var map = new OneToOneIdMap();

            Assert.AreEqual(false, map.IsReadonly);
            map.Add(1, 10);
            map.Add(10, 100);
            map.Add(100, 1000);
            map.Add(1000, 10000);
            Assert.AreEqual(10, map.Get(1));
            Assert.AreEqual(100, map.Get(10));
            Assert.AreEqual(1000, map.Get(100));
            Assert.AreEqual(10000, map.Get(1000));
        }
    }
}