using System;
using OsmSharp.Changesets;

namespace OsmSharp.Db.Tiled
{
    /// <summary>
    /// A database, a collection of snapshots, diffs and branches.
    /// </summary>
    public class Database
    {
        private readonly DatabaseSnapshot _initial;

        /// <summary>
        /// Creates a new database.
        /// </summary>
        /// <param name="initial">The initial snapshot.</param>
        public Database(DatabaseSnapshot initial)
        {
            _initial = initial;
        }

        /// <summary>
        /// Applies the given change set.
        /// </summary>
        /// <param name="changeSet">The changes to apply.</param>
        /// <returns>True if applying the changes succeeded.</returns>
        public bool ApplyChangeSet(OsmChange changeSet)
        {
            // creates a new database diff representing the given changes.

            var latest = _initial;
            
            // deleting an existing object.
            // 1. find the tile its in.
            // 2. copy the tile.
            // 3. remove the object from that tile.
            
            // adding a new object.
            // 1. find the tile its supposed to be in.
            // 2. copy the tile.
            // 3. add the object to that tile.
            
            // updating an object.
            // 1. delete the object.
            // 2. add the object.
            
            throw new NotImplementedException();
        }
    }
}