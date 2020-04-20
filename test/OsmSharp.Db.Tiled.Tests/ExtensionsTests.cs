//using System.Linq;
//using NUnit.Framework;
//
//namespace OsmSharp.Db.Tiled.Tests
//{
//    [TestFixture]
//    public class ExtensionsTests
//    {    
//        [Test]
//        public void Extensions_Merge_Empty_With_Empty_Empty()
//        {
//            var osm1 = new OsmGeo[] { };
//            var osm2 = new OsmGeo[] { };
//
//            var merged = osm1.Merge(osm2);
//            Assert.NotNull(merged);
//            var mergedList = merged.ToList();
//            Assert.AreEqual(0, mergedList.Count);
//        }
//        
//        [Test]
//        public void Extensions_Merge_OneNode_With_Empty_OneNode()
//        {
//            var osm1 = new OsmGeo[]
//            {
//                new Node
//                {
//                    Id = 1
//                }
//            };
//            var osm2 = new OsmGeo[] { };
//
//            var merged = osm1.Merge(osm2);
//            Assert.NotNull(merged);
//            var mergedList = merged.ToList();
//            Assert.AreEqual(1, mergedList.Count);
//            Assert.AreEqual(1, mergedList[0].Id);
//        }
//        
//        [Test]
//        public void Extensions_Merge_Empty_With_OneNode_OneNode()
//        {
//            var osm1 = new OsmGeo[] { };
//            var osm2 = new OsmGeo[]
//            {
//                new Node
//                {
//                    Id = 1
//                }
//            };
//
//            var merged = osm1.Merge(osm2);
//            Assert.NotNull(merged);
//            var mergedList = merged.ToList();
//            Assert.AreEqual(1, mergedList.Count);
//            Assert.AreEqual(1, mergedList[0].Id);
//        }
//        
//        [Test]
//        public void Extensions_Merge_OneNode_With_OneNode_NoConflicts_TwoNodes()
//        {
//            var osm1 = new OsmGeo[]
//            {
//                new Node
//                {
//                    Id = 1
//                }
//            };
//            var osm2 = new OsmGeo[]
//            {
//                new Node
//                {
//                    Id = 2
//                }
//            };
//
//            var merged = osm1.Merge(osm2);
//            Assert.NotNull(merged);
//            var mergedList = merged.ToList();
//            Assert.AreEqual(2, mergedList.Count);
//            Assert.AreEqual(1, mergedList[0].Id);
//            Assert.AreEqual(2, mergedList[1].Id);
//        }
//    }
//}