using System.Threading.Tasks;
using NUnit.Framework;
using OsmSharp.Db.Tiled.Replication;

namespace OsmSharp.Db.Tiled.Tests.Replication
{
    [TestFixture]
    public class ReplicationConfigTests
    {
        [Test]
        public void ReplicationConfig_MinutelyConfig_ShouldReturnMinutelyData()
        {
            var replicationConfig = new ReplicationConfig("https://planet.openstreetmap.org/replication/minute/", 60);
            
            Assert.AreEqual(60, replicationConfig.Period);
            Assert.AreEqual("https://planet.openstreetmap.org/replication/minute/", replicationConfig.Url);
            Assert.True(replicationConfig.IsMinutely);
            Assert.False(replicationConfig.IsHourly);
            Assert.False(replicationConfig.IsDaily);
        }
        
        [Test]
        public void ReplicationConfig_HourlyConfig_ShouldReturnHourlyData()
        {
            var replicationConfig = new ReplicationConfig("https://planet.openstreetmap.org/replication/hour/", 3600);
            
            Assert.AreEqual(3600, replicationConfig.Period);
            Assert.AreEqual("https://planet.openstreetmap.org/replication/hour/", replicationConfig.Url);
            Assert.False(replicationConfig.IsMinutely);
            Assert.True(replicationConfig.IsHourly);
            Assert.False(replicationConfig.IsDaily);
        }
        
        [Test]
        public void ReplicationConfig_DailyConfig_ShouldReturnDailyData()
        {
            var replicationConfig = new ReplicationConfig("https://planet.openstreetmap.org/replication/day/", 24 * 3600);
            
            Assert.AreEqual(24 * 3600, replicationConfig.Period);
            Assert.AreEqual("https://planet.openstreetmap.org/replication/day/", replicationConfig.Url);
            Assert.False(replicationConfig.IsMinutely);
            Assert.False(replicationConfig.IsHourly);
            Assert.True(replicationConfig.IsDaily);
        }
        
        [Test]
        public async Task ReplicationConfig_DailyConfig_Test()
        {
            IO.Http.HttpHandler.Default = new ReplicationServerMockHttpHandler();
            var replicationConfig = new ReplicationConfig("https://planet.openstreetmap.org/replication/day/", 24 * 3600);

            var result = await replicationConfig.LatestReplicationState();
            
            Assert.AreEqual(2568, result.SequenceNumber);
        }
    }
}