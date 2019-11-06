namespace OsmSharp.Db.Tiled.Indexes
{
    /// <summary>
    /// Abstract version of an index.
    /// </summary>
    internal interface IIndex
    {
        /// <summary>
        /// Adds a new entry in this index.
        /// </summary>
        void Add(long id, int mask);
        
        /// <summary>
        /// Tries to get the mask for the given id.
        /// </summary>
        bool TryGetMask(long id, out int mask);
    }
}