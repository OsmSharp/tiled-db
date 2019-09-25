using System.Threading.Tasks;
using NUnit.Framework;
using OsmSharp.Db.Tiled.Replication;

namespace OsmSharp.Db.Tiled.Tests.Replication
{
    [TestFixture]
    public class ReplicationChangesetEnumeratorTests
    {
        [Test]
        public async Task ReplicationChangeset_MoveTo_0_False()
        {
            IO.Http.HttpHandler.Default = new ReplicationServerMockHttpHandler();
            var enumerator = new ReplicationChangesetEnumerator(
                Tiled.Replication.Replication.Daily);
            Assert.False(await enumerator.MoveTo(0));
        }
        
        [Test]
        public async Task ReplicationChangeset_MoveTo_1_True()
        {
            IO.Http.HttpHandler.Default = new ReplicationServerMockHttpHandler();
            var enumerator = new ReplicationChangesetEnumerator(
                Tiled.Replication.Replication.Daily);
            Assert.True(await enumerator.MoveTo(1));
        }
        
        [Test]
        public async Task ReplicationChangeset_MoveTo_NonExisting_False()
        {
            IO.Http.HttpHandler.Default = new ReplicationServerMockHttpHandler();
            var enumerator = new ReplicationChangesetEnumerator(
                Tiled.Replication.Replication.Daily);
            Assert.False(await enumerator.MoveTo(
                OsmSharp.Db.Tiled.Replication.Replication.MaxSequenceNumber - 10249));
        }
        
        [Test]
        public async Task ReplicationChangeset_MoveTo_Last_True()
        {
            IO.Http.HttpHandler.Default = new ReplicationServerMockHttpHandler();
            var enumerator = new ReplicationChangesetEnumerator(
                Tiled.Replication.Replication.Daily);
            var latest = await Tiled.Replication.Replication.Daily.LatestReplicationState();
            Assert.True(await enumerator.MoveTo(
                latest.SequenceNumber));
        }
        
        [Test]
        public async Task ReplicationChangeset_MoveNext_1_2()
        {
            IO.Http.HttpHandler.Default = new ReplicationServerMockHttpHandler();
            var enumerator = new ReplicationChangesetEnumerator(
                Tiled.Replication.Replication.Daily);
            await enumerator.MoveTo(1);
            Assert.True(await enumerator.MoveNext());
            Assert.AreEqual(2, enumerator.State.SequenceNumber);
        }
        
        // TODO: figure out how to test this, we can't test this really because the enumerator will not continue until there is a next diff available.
//        [Test]
//        public async Task ReplicationChangeset_MoveNext_Last_False()
//        {
//            IO.Http.HttpHandler.Default = new ReplicationServerMockHttpHandler();
//            var enumerator = new ReplicationChangesetEnumerator(
//                Tiled.Replication.Replication.Daily);
//            var latest = await Tiled.Replication.Replication.Daily.LatestReplicationState();
//            await enumerator.MoveTo(latest.SequenceNumber);
//            Assert.False(await enumerator.MoveNext());
//        }
    }
}