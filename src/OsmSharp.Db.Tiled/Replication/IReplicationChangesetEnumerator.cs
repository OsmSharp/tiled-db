using System.Threading.Tasks;
using OsmSharp.Changesets;

namespace OsmSharp.Db.Tiled.Replication
{
    /// <summary>
    /// Abstract representation of a replication changeset enumerator.
    /// </summary>
    public interface IReplicationChangesetEnumerator
    {
        /// <summary>
        /// Moves to the next diff, returns true when it's available.
        /// </summary>
        /// <returns></returns>
        Task<bool> MoveNext();

        /// <summary>
        /// Gets the current diff.
        /// </summary>
        OsmChange Current { get; }

        /// <summary>
        /// Gets the replication state.
        /// </summary>
        ReplicationState State { get; }

        /// <summary>
        /// Gets the replication config.
        /// </summary>
        ReplicationConfig Config { get; }
    }
}