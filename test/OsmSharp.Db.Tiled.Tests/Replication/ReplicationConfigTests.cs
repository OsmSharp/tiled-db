using System;
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
        public async Task ReplicationConfig_DailyConfig_LatestShouldGiveStateSequence()
        {
            IO.Http.HttpHandler.Default = new ReplicationServerMockHttpHandler();
            var replicationConfig = new ReplicationConfig("https://planet.openstreetmap.org/replication/day/", 24 * 3600);

            var result = await replicationConfig.LatestReplicationState();
            
            Assert.AreEqual(2569, result.SequenceNumber);
        }

        [Test]
        public async Task ReplicationConfig_DailyConfig_GuessSequenceNumberAt_ShouldReturnSequenceNumberContainingDay()
        {
            IO.Http.HttpHandler.Default = new ReplicationServerMockHttpHandler();
            var replicationConfig = new ReplicationConfig("https://planet.openstreetmap.org/replication/day/", 24 * 3600);

            var result = await replicationConfig.GuessSequenceNumberAt(new DateTime(2019, 08, 3, 8, 15, 0));
            
            Assert.AreEqual(2517, result);
        }

        [Test]
        public async Task ReplicationConfig_HourlyConfig_GuessSequenceNumberAt_ShouldReturnSequenceNumberContainingHour()
        {
            IO.Http.HttpHandler.Default = new ReplicationServerMockHttpHandler();
            var replicationConfig = new ReplicationConfig("https://planet.openstreetmap.org/replication/hour/", 3600);

            var result = await replicationConfig.GuessSequenceNumberAt(new DateTime(2019, 08, 3, 8, 15, 0));
            
            Assert.AreEqual(60386, result);
        }

        [Test]
        public async Task ReplicationConfig_MinutelyConfig_GuessSequenceNumberAt_ShouldReturnSequenceNumberContainingMinute()
        {
            IO.Http.HttpHandler.Default = new ReplicationServerMockHttpHandler();
            var replicationConfig = new ReplicationConfig("https://planet.openstreetmap.org/replication/minute/", 60);

            var result = await replicationConfig.GuessSequenceNumberAt(new DateTime(2019, 08, 3, 8, 15, 0));
            
            Assert.AreEqual(3610524, result);
        }

        [Test]
        public async Task ReplicationConfig_DailyConfig_GetReplicationState_ShouldReturnSequenceNumberAndProperDate()
        {
            IO.Http.HttpHandler.Default = new ReplicationServerMockHttpHandler();
            var replicationConfig = new ReplicationConfig("https://planet.openstreetmap.org/replication/day/", 24 * 3600);

            var result = await replicationConfig.GetReplicationState(2517);
            
            Assert.AreEqual(2517, result.SequenceNumber);
            Assert.AreEqual(new DateTime(2019, 08, 04, 0, 0, 0, DateTimeKind.Utc), result.Timestamp);
            Assert.AreEqual(replicationConfig, result.Config);
        }

        [Test]
        public async Task ReplicationConfig_DailyConfig_DownloadDiff_ShouldReturnParsedOsmChange()
        {
            IO.Http.HttpHandler.Default = new ReplicationServerMockHttpHandler();
            var replicationConfig = new ReplicationConfig("https://planet.openstreetmap.org/replication/day/", 24 * 3600);

            var result = await replicationConfig.DownloadDiff(2517);
            
            Assert.NotNull(result);
            Assert.NotNull(result.Modify);
            Assert.NotNull(result.Create);
            Assert.NotNull(result.Delete);
        }
    }
}