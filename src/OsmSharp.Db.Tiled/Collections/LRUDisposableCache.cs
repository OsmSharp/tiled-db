using System;
using System.Collections.Generic;
using System.IO;

namespace OsmSharp.Db.Tiled.Collections
{
    /// <summary>
    /// An LRU cache handling disposable resources.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    internal class LRUDisposableCache<TKey, TValue> 
        where TValue : class, ILRUDisposable
    {
        private readonly Dictionary<TKey, CacheEntry> _data;

        // about the linked list:
        // - oldest is always at position '0'.
        // - empty means a previous and next equal to int.maxvalue.
        // - empty space is always at the end.
        // - when oldest is empty, this means the cache is empty.
        
        /// <summary>
        /// Initializes this cache.
        /// </summary>
        /// <param name="capacity"></param>
        public LRUDisposableCache(int capacity)
        {
            this.Capacity = capacity;
            
            _data = new Dictionary<TKey, CacheEntry>(capacity);
        }

        private CacheEntry? _oldest;
        private CacheEntry? _latest;

        /// <summary>
        /// Capacity.
        /// </summary>
        public int Capacity { get; set; }

        /// <summary>
        /// Adds a new value for the given key.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        public void Add(TKey key, TValue value)
        {
            if (_data.ContainsKey(key)) throw new ArgumentException("Key already present.");

            if (this.Count == this.Capacity)
            {
                this.RemoveOldest();
            }
            
            // add the data at the end.
            var entry = new CacheEntry(key, value);
            if (_latest == null)
            {
                _latest = entry;
                _oldest = entry;
            }
            else
            {
                entry.Next = _latest;
                _latest = entry;
            }
            
            // set data.
            value.Touched();
            _data[key] = entry;
        }

        /// <summary>
        /// Returns the amount of entries in this cache.
        /// </summary>
        public int Count => _data.Count;

        /// <summary>
        /// Returns the value for this given key.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        /// <returns>True if the value is there, false otherwise.</returns>
        public bool TryGet(TKey key, out TValue value)
        {
            if (_data.TryGetValue(key, out var entry))
            {
                // set this as latest.
                if (_latest == null) throw new InvalidDataException("Latest cannot be null when there is data.");
                if (_latest != entry)
                {
                    // remove entry.
                    this.RemoveEntry(entry);

                    // add at the end.
                    entry.Next = _latest;
                    _latest = entry;
                }

                value = entry.Value;
                value.Touched();
                return true;
            }

            value = default;
            return false;
        }

        /// <summary>
        /// Returns the value for this given key but does not effect the cache.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        /// <returns>True when the key was found, false otherwise.</returns>
        public bool TryPeek(TKey key, out TValue value)
        {
            if (_data.TryGetValue(key, out var entry))
            {
                value = entry.Value;
                value.Touched();
                return true;
            }

            value = default;
            return false;
        }

        /// <summary>
        /// Clears this cache.
        /// </summary>
        public void Clear()
        {
            // call the OnRemove delegate.
            foreach (var entry in _data.Values)
            {
                entry.Value.RemovedFromCache();
            }
            
            _data.Clear();
        }

        /// <summary>
        /// Removes the value for the given key.
        /// </summary>
        /// <param name="key">The key.</param>
        public bool Remove(TKey key)
        {
            if (!_data.TryGetValue(key, out var entry)) return false;

            entry.Value.RemovedFromCache();
            this.RemoveEntry(entry);

            return true;
        }

        private void RemoveEntry(CacheEntry entry)
        {
            if (entry.Previous != null) entry.Previous.Next = entry.Next;
            if (entry.Next != null) entry.Next.Previous = entry.Previous;

            if (entry == _latest) _latest = entry.Previous;
            if (entry == _oldest) _oldest = entry.Next;
        }

        private void RemoveOldest()
        {
            if (_oldest == null) return;
            
            // report removed.
            _oldest.Value.RemovedFromCache();
            if (_oldest == _latest) _latest = null;

            // remove from data.
            _data.Remove(_oldest.Key);
            
            // remove oldest from linked list.
            _oldest = _oldest.Next;
        }

        private class CacheEntry
        {
            public CacheEntry(TKey key, TValue value)
            {
                this.Key = key;
                this.Value = value;
            }
            
            public TKey Key { get; set; }
            
            public TValue Value { get; set; }
            
            public CacheEntry? Previous { get; set; }
            
            public CacheEntry? Next { get; set; }
        }
    }
}