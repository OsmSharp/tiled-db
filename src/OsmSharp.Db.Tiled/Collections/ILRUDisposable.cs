using System;

namespace OsmSharp.Db.Tiled.Collections
{
    /// <summary>
    /// Abstract representation of a disposable resource part of an LRU cache.
    /// </summary>
    /// <remarks>
    /// This implements a dispose mechanism in two steps:
    /// - dispose is called, two things can happen:
    ///     - object is in the cache: dispose is not yet executed, a boolean is set disposed.
    ///     - object is not in the cache: dispose is executed immediately.
    /// - when the object is touched in the meantime:
    ///     - the dispose state is set to false.
    ///     - the in cache state is set to true.
    /// - when removed from cache:
    ///     - when dispose is true, dispose of resources.
    ///     - when dispose is false, cache state is set to false.
    ///
    /// This will make sure that a resource is disposed either when:
    /// - it's removed from the cache.
    /// - it's being disposed while not in the cache anymore.
    /// </remarks>
    internal interface ILRUDisposable : IDisposable
    {
        /// <summary>
        /// Called when the object was touched in the cache.
        /// </summary>
        void Touched();
        
        /// <summary>
        /// Called the object was removed from the cache.
        /// </summary>
        void RemovedFromCache();
    }
}