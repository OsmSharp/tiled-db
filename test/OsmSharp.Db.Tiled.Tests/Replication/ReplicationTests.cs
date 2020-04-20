//using System.Threading.Tasks;
//using NUnit.Framework;
//using OsmSharp.API;
//
//namespace OsmSharp.Db.Tiled.Tests.Replication
//{
//    [TestFixture]
//    public class ReplicationTests
//    {
//        [Test]
//        public void Replication_Minutely_ShouldBeMinutely()
//        {
//            var replicationConfig = OsmSharp.Db.Tiled.Replication.Replication.Minutely;
//            
//            Assert.AreEqual(60, replicationConfig.Period);
//            Assert.AreEqual("https://planet.openstreetmap.org/replication/minute/", replicationConfig.Url);
//            Assert.True(replicationConfig.IsMinutely);
//            Assert.False(replicationConfig.IsHourly);
//            Assert.False(replicationConfig.IsDaily);
//        }
//        
//        [Test]
//        public void Replication_Hourly_ShouldBeHourly()
//        {
//            var replicationConfig = OsmSharp.Db.Tiled.Replication.Replication.Hourly;
//            
//            Assert.AreEqual(3600, replicationConfig.Period);
//            Assert.AreEqual("https://planet.openstreetmap.org/replication/hour/", replicationConfig.Url);
//            Assert.False(replicationConfig.IsMinutely);
//            Assert.True(replicationConfig.IsHourly);
//            Assert.False(replicationConfig.IsDaily);
//        }
//        
//        [Test]
//        public void Replication_Daily_ShouldBeDaily()
//        {
//            var replicationConfig = OsmSharp.Db.Tiled.Replication.Replication.Daily;
//            
//            Assert.AreEqual(24 * 3600, replicationConfig.Period);
//            Assert.AreEqual("https://planet.openstreetmap.org/replication/day/", replicationConfig.Url);
//            Assert.False(replicationConfig.IsMinutely);
//            Assert.False(replicationConfig.IsHourly);
//            Assert.True(replicationConfig.IsDaily);
//        }
//
//        [Test]
//        public async Task Replication_GetReplicationState_0_ShouldReturnNull()
//        {
//            IO.Http.HttpHandler.Default = new ReplicationServerMockHttpHandler();
//            var result = await OsmSharp.Db.Tiled.Replication.Replication.GetReplicationState(OsmSharp.Db.Tiled.Replication.Replication.Daily, 
//                0);
//            Assert.IsNull(result);
//        }
//
//        [Test]
//        public async Task Replication_GetReplicationState_SmallerThanZero_ShouldReturnNull()
//        {
//            IO.Http.HttpHandler.Default = new ReplicationServerMockHttpHandler();
//            var result = await OsmSharp.Db.Tiled.Replication.Replication.GetReplicationState(OsmSharp.Db.Tiled.Replication.Replication.Daily, 
//                -10);
//            Assert.IsNull(result);
//        }
//
//        [Test]
//        public async Task Replication_GetReplicationState_AboveMax_ShouldReturnNull()
//        {
//            IO.Http.HttpHandler.Default = new ReplicationServerMockHttpHandler();
//            var result = await OsmSharp.Db.Tiled.Replication.Replication.GetReplicationState(OsmSharp.Db.Tiled.Replication.Replication.Daily, 
//                long.MaxValue);
//            Assert.IsNull(result);
//        }
//
//        [Test]
//        public async Task Replication_GetReplicationState_NonExisting_ShouldReturnNull()
//        {
//            IO.Http.HttpHandler.Default = new ReplicationServerMockHttpHandler();
//            var result = await OsmSharp.Db.Tiled.Replication.Replication.GetReplicationState(OsmSharp.Db.Tiled.Replication.Replication.Daily, 
//                OsmSharp.Db.Tiled.Replication.Replication.MaxSequenceNumber - 100024);
//            Assert.IsNull(result);
//        }
//    }
//}