using System;
using System.Threading.Tasks;
using NUnit.Framework;
using OsmSharp.Db.Tiled.Replication;

namespace OsmSharp.Db.Tiled.Tests.Replication
{
    [TestFixture]
    public class ReplicationDiffEnumeratorTests
    {
        [Test]
        public async Task ReplicationDiff_MoveTo_0_False()
        {
            IO.Http.HttpHandler.Default = new ReplicationServerMockHttpHandler();
            var enumerator = new ReplicationDiffEnumerator(
                Tiled.Replication.Replication.Daily);
            Assert.False(await enumerator.MoveTo(0));
        }
        
        [Test]
        public async Task ReplicationDiff_MoveTo_1_True()
        {
            IO.Http.HttpHandler.Default = new ReplicationServerMockHttpHandler();
            var enumerator = new ReplicationDiffEnumerator(
                Tiled.Replication.Replication.Daily);
            Assert.True(await enumerator.MoveTo(1));
        }
        
        [Test]
        public async Task ReplicationDiff_MoveTo_NonExisting_False()
        {
            IO.Http.HttpHandler.Default = new ReplicationServerMockHttpHandler();
            var enumerator = new ReplicationDiffEnumerator(
                Tiled.Replication.Replication.Daily);
            Assert.False(await enumerator.MoveTo(
                OsmSharp.Db.Tiled.Replication.Replication.MaxSequenceNumber - 10249));
        }
        
        [Test]
        public async Task ReplicationDiff_MoveTo_Last_True()
        {
            IO.Http.HttpHandler.Default = new ReplicationServerMockHttpHandler();
            var enumerator = new ReplicationDiffEnumerator(
                Tiled.Replication.Replication.Daily);
            var latest = await Tiled.Replication.Replication.Daily.LatestReplicationState();
            Assert.True(await enumerator.MoveTo(
                latest.SequenceNumber));
        }
        
        [Test]
        public async Task ReplicationDiff_MoveNext_1_2()
        {
            IO.Http.HttpHandler.Default = new ReplicationServerMockHttpHandler();
            var enumerator = new ReplicationDiffEnumerator(
                Tiled.Replication.Replication.Daily);
            await enumerator.MoveTo(1);
            Assert.True(await enumerator.MoveNext());
            Assert.AreEqual(2, enumerator.State.SequenceNumber);
        }

        [Test]
        public async Task ReplicationDiff_Daily_MoveTo_TimeStamp_ShouldOverlapTimestamp()
        {
            IO.Http.HttpHandler.Default = new ReplicationServerMockHttpHandler();
            var enumerator = new ReplicationDiffEnumerator(
                Tiled.Replication.Replication.Daily);
            var timestamp = new DateTime(2019, 08, 3, 8, 15, 0);
            Assert.True(await enumerator.MoveTo(timestamp));
            Assert.True(enumerator.State.Overlaps(timestamp));
            Assert.AreEqual(2517, enumerator.State.SequenceNumber);
        }

        [Test]
        public async Task ReplicationDiff_Hourly_MoveTo_TimeStamp_ShouldOverlapTimestamp()
        {
            IO.Http.HttpHandler.Default = new ReplicationServerMockHttpHandler();
            var enumerator = new ReplicationDiffEnumerator(
                Tiled.Replication.Replication.Hourly);
            var timestamp = new DateTime(2019, 08, 3, 8, 15, 0);
            Assert.True(await enumerator.MoveTo(timestamp));
            Assert.True(enumerator.State.Overlaps(timestamp));
            Assert.AreEqual(60386, enumerator.State.SequenceNumber);
        }

        [Test]
        public async Task ReplicationDiff_Minutely_MoveTo_TimeStamp_ShouldOverlapTimestamp()
        {
            IO.Http.HttpHandler.Default = new ReplicationServerMockHttpHandler();
            var enumerator = new ReplicationDiffEnumerator(
                Tiled.Replication.Replication.Minutely);
            var timestamp = new DateTime(2019, 09, 22, 15, 15, 0, DateTimeKind.Utc);
            Assert.True(await enumerator.MoveTo(timestamp));
            Assert.True(enumerator.State.Overlaps(timestamp));
            Assert.AreEqual(3682948, enumerator.State.SequenceNumber);
        }
        
        // TODO: figure out how to test this, we can't test this really because the enumerator will not continue until there is a next diff available.
//        [Test]
//        public async Task ReplicationDiff_MoveNext_Last_False()
//        {
//            IO.Http.HttpHandler.Default = new ReplicationServerMockHttpHandler();
//            var enumerator = new ReplicationDiffEnumerator(
//                Tiled.Replication.Replication.Daily);
//            var latest = await Tiled.Replication.Replication.Daily.LatestReplicationState();
//            await enumerator.MoveTo(latest.SequenceNumber);
//            Assert.False(await enumerator.MoveNext());
//        }
    }
}