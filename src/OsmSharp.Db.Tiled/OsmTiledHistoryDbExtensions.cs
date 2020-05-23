using System;
using OsmSharp.Db.Tiled.OsmTiled;

namespace OsmSharp.Db.Tiled
{
    /// <summary>
    /// Contains extension methods for the history db.
    /// </summary>
    public static class OsmTiledHistoryDbExtensions
    {
        /// <summary>
        /// Gets the database right before this one.
        /// </summary>
        /// <param name="historyDb">The history db.</param>
        /// <param name="db">The db.</param>
        /// <returns>The db before this one. If there are two candidates, one a snapshot and one a diff, the diff is returned with the smallest timespan.</returns>
        public static OsmTiledDbBase? GetSmallestPrevious(this OsmTiledHistoryDb historyDb, OsmTiledDbBase db)
        {
            var previousId = historyDb.Previous(db.Id);
            if (previousId == null) return null;

            return historyDb.GetSmallest(previousId.Value);
        }

        /// <summary>
        /// Gets the db smallest db containing the latest version of the data for the given tile.
        /// </summary>
        /// <param name="historyDb">The history db.</param>
        /// <param name="db">The db.</param>
        /// <param name="tile">The tile.</param>
        /// <returns>The database containing the latest version of the given tile.</returns>
        public static OsmTiledDbBase? GetDbForTile(this OsmTiledHistoryDb historyDb, OsmTiledDbBase db, (uint x, uint y) tile)
        {
            var earliest = db.GetDbForTile(tile);
            if (earliest == null) return null;
            
            // check if earliest is the smallest.
            var smallest = historyDb.GetSmallest(earliest.Id);
            while (smallest != earliest)
            {
                earliest = smallest.GetDbForTile(tile);
                if (earliest == null) return null;
                if (earliest.Id == smallest.Id) return smallest;
                
                smallest = historyDb.GetSmallest(earliest.Id);
            }

            return smallest;
        }
        
        /// <summary>
        /// Gets the database right after this one.
        /// </summary>
        /// <param name="historyDb">The history db.</param>
        /// <param name="db">The db.</param>
        /// <returns>The db after this one. If there are two candidates, one a snapshot and one a diff, the diff is returned with the smallest timespan.</returns>
        public static OsmTiledDbBase? GetSmallestNext(this OsmTiledHistoryDb historyDb, OsmTiledDbBase db)
        {
            var nextId = historyDb.Next(db.Id);
            if (nextId == null) return null;

            return historyDb.GetSmallest(nextId.Value);
        }

        /// <summary>
        /// Gets the smallest database for the given timestamp.
        ///
        /// The database that was created on or right before the given timestamp.
        /// </summary>
        /// <param name="historyDb">The history db.</param>
        /// <param name="timestamp">The timestamp.</param>
        /// <returns>The database closest to the given timestamp.</returns>
        public static OsmTiledDbBase? GetSmallestOn(this OsmTiledHistoryDb historyDb, DateTime timestamp)
        {
            return historyDb.GetListOn(timestamp)?.Smallest();
        }

        /// <summary>
        /// Gets the database for the given timestamp.
        ///
        /// The database that was created on or right before the given timestamp.
        /// </summary>
        /// <param name="historyDb">The history db.</param>
        /// <param name="timestamp">The timestamp.</param>
        /// <returns>The database closest to the given timestamp.</returns>
        public static OsmTiledDbBase? GetOn(this OsmTiledHistoryDb historyDb, DateTime timestamp)
        {
            return historyDb.GetListOn(timestamp)?.Db;
        }
    }
}