using System;
using System.Threading.Tasks;
using NUnit.Framework;
using OsmSharp.Db.Tiled.Replication;

namespace OsmSharp.Db.Tiled.Tests.Replication
{
    [TestFixture]
    public class CatchupReplicationDiffEnumeratorTests
    {
        [Test]
        public async Task CatchupReplicationDiffEnumerator_MoveNext_Future_False()
        {
            IO.Http.HttpHandler.Default = new ReplicationServerMockHttpHandler();
            var enumerator = new CatchupReplicationDiffEnumerator(
                new DateTime(2020, 01, 01));

            Assert.False(await enumerator.MoveNext());
        }
        
        [Test]
        public async Task CatchupReplicationDiffEnumerator_MoveNext_FirstMinuteThenHourThenDayThenHourThenMinute()
        {
            IO.Http.HttpHandler.Default = new ReplicationServerMockHttpHandler();
            var enumerator = new CatchupReplicationDiffEnumerator(
                new DateTime(2019, 09, 22, 20, 55, 0, DateTimeKind.Utc));

            // first minutes will happen.
            Assert.True(await enumerator.MoveNext());
            Assert.AreEqual(3683288, enumerator.State.SequenceNumber);
            Assert.True(await enumerator.MoveNext());
            Assert.AreEqual(3683289, enumerator.State.SequenceNumber);
            Assert.True(await enumerator.MoveNext());
            Assert.AreEqual(3683290, enumerator.State.SequenceNumber);
            Assert.True(await enumerator.MoveNext());
            Assert.AreEqual(3683291, enumerator.State.SequenceNumber);
            Assert.True(await enumerator.MoveNext());
            Assert.AreEqual(3683292, enumerator.State.SequenceNumber);
            Assert.True(await enumerator.MoveNext());
            Assert.AreEqual(3683293, enumerator.State.SequenceNumber);
            
            // then hours will happen.
            Assert.True(await enumerator.MoveNext());
            Assert.AreEqual(61599, enumerator.State.SequenceNumber);
            Assert.True(await enumerator.MoveNext());
            Assert.AreEqual(61600, enumerator.State.SequenceNumber);
            Assert.True(await enumerator.MoveNext());
            Assert.AreEqual(61601, enumerator.State.SequenceNumber);
            
            // then days will happen.
            Assert.True(await enumerator.MoveNext());
            Assert.AreEqual(2567, enumerator.State.SequenceNumber);
            Assert.True(await enumerator.MoveNext());
            Assert.AreEqual(2568, enumerator.State.SequenceNumber);
            Assert.True(await enumerator.MoveNext());
            Assert.AreEqual(2569, enumerator.State.SequenceNumber);
            
            // then hours again.
            Assert.True(await enumerator.MoveNext());
            Assert.AreEqual(61650, enumerator.State.SequenceNumber);
            Assert.True(await enumerator.MoveNext());
            Assert.AreEqual(61651, enumerator.State.SequenceNumber);
            Assert.True(await enumerator.MoveNext());
            Assert.AreEqual(61652, enumerator.State.SequenceNumber);
            Assert.True(await enumerator.MoveNext());
            Assert.AreEqual(61653, enumerator.State.SequenceNumber);
            Assert.True(await enumerator.MoveNext());
            Assert.AreEqual(61654, enumerator.State.SequenceNumber);
            Assert.True(await enumerator.MoveNext());
            Assert.AreEqual(61655, enumerator.State.SequenceNumber);
            Assert.True(await enumerator.MoveNext());
            Assert.AreEqual(61656, enumerator.State.SequenceNumber);
            Assert.True(await enumerator.MoveNext());
            Assert.AreEqual(61657, enumerator.State.SequenceNumber);
            Assert.True(await enumerator.MoveNext());
            Assert.AreEqual(61658, enumerator.State.SequenceNumber);
            Assert.True(await enumerator.MoveNext());
            Assert.AreEqual(61659, enumerator.State.SequenceNumber);
            Assert.True(await enumerator.MoveNext());
            Assert.AreEqual(61660, enumerator.State.SequenceNumber);
            Assert.True(await enumerator.MoveNext());
            Assert.AreEqual(61661, enumerator.State.SequenceNumber);
            Assert.True(await enumerator.MoveNext());
            Assert.AreEqual(61662, enumerator.State.SequenceNumber);
            Assert.True(await enumerator.MoveNext());
            Assert.AreEqual(61663, enumerator.State.SequenceNumber);
            
            // then minutes again.
            Assert.True(await enumerator.MoveNext());
            Assert.AreEqual(3687190, enumerator.State.SequenceNumber);
            Assert.True(await enumerator.MoveNext());
            Assert.AreEqual(3687191, enumerator.State.SequenceNumber);
            Assert.True(await enumerator.MoveNext());
            Assert.AreEqual(3687192, enumerator.State.SequenceNumber);
            Assert.True(await enumerator.MoveNext());
            Assert.AreEqual(3687193, enumerator.State.SequenceNumber);
            Assert.True(await enumerator.MoveNext());
            Assert.AreEqual(3687194, enumerator.State.SequenceNumber);
            Assert.True(await enumerator.MoveNext());
            Assert.AreEqual(3687195, enumerator.State.SequenceNumber);
            Assert.False(await enumerator.MoveNext());
        }
    }
}