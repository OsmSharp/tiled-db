using System.Threading.Tasks;
using NUnit.Framework;
using OsmSharp.Db.Tiled.Replication;

namespace OsmSharp.Db.Tiled.Tests.Replication
{
    [TestFixture]
    public class ReplicationChangesetEnumeratorTests
    {
        [Test]
        public async Task ReplicationChangeset_MoveEnumeratorTo0_ShouldReturnFalse()
        {
            var enumerator = new ReplicationChangesetEnumerator(
                Tiled.Replication.Replication.Daily);
            Assert.False(await enumerator.MoveTo(0));
        }
    }
}