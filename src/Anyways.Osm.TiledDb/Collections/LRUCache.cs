﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Anyways.Osm.TiledDb.Collections
{
    /// <summary>
    /// Generic LRU cache implementation.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public class LRUCache<TKey, TValue> : IEnumerable<KeyValuePair<TKey, TValue>>
    {
        /// <summary>
        /// Holds the cached data.
        /// </summary>
        private Dictionary<TKey, CacheEntry> _data;

        /// <summary>
        /// Holds the next id.
        /// </summary>
        private long _id;

        /// <summary>
        /// Holds the last id.
        /// </summary>
        private long _lastId;

        /// <summary>
        /// A delegate to use for when an item is pushed out of the cache.
        /// </summary>
        /// <param name="item"></param>
        public delegate void OnRemoveDelegate(TValue item);

        /// <summary>
        /// Called when an item is pushed out of the cache.
        /// </summary>
        public OnRemoveDelegate OnRemove;

        /// <summary>
        /// Initializes this cache.
        /// </summary>
        /// <param name="capacity"></param>
        public LRUCache(int capacity)
        {
            _id = long.MinValue;
            _lastId = _id;
            _data = new Dictionary<TKey, CacheEntry>();

            this.Capacity = capacity;
        }

        /// <summary>
        /// Capacity.
        /// </summary>
        public int Capacity { get; set; }

        /// <summary>
        /// Adds a new value for the given key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        public void Add(TKey key, TValue value)
        {
            CacheEntry entry = new CacheEntry
            {
                Id = _id,
                Value = value
            };
            lock (_data)
            {
                _id++;
                _data[key] = entry;
            }

            this.ResizeCache();
        }

        /// <summary>
        /// Returns the amount of entries in this cache.
        /// </summary>
        public int Count
        {
            get
            {
                return _data.Count;
            }
        }

        /// <summary>
        /// Returns the value for this given key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public bool TryGet(TKey key, out TValue value)
        {
            lock (_data)
            {
                CacheEntry entry;
                _id++;
                if (_data.TryGetValue(key, out entry))
                {
                    entry.Id = _id;
                    value = entry.Value;
                    return true;
                }
            }
            value = default(TValue);
            return false;
        }

        /// <summary>
        /// Returns the value for this given key but does not effect the cache.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public bool TryPeek(TKey key, out TValue value)
        {
            lock (_data)
            {
                CacheEntry entry;
                if (_data.TryGetValue(key, out entry))
                {
                    value = entry.Value;
                    return true;
                }
            }
            value = default(TValue);
            return false;
        }

        /// <summary>
        /// Clears this cache.
        /// </summary>
        public void Clear()
        {
            lock (_data)
            {
                _data.Clear();
            }
            _lastId = _id;
            _id = long.MinValue;
        }

        /// <summary>
        /// Removes the value for the given key.
        /// </summary>
        /// <param name="id"></param>
        public void Remove(TKey id)
        {
            lock (_data)
            {
                _data.Remove(id);
            }
        }

        /// <summary>
        /// Resizes the cache.
        /// </summary>
        private void ResizeCache()
        {
            lock (_data)
            {
                while (_data.Count > this.Capacity)
                { // oops: too much data.
                    // remove the 'oldest' item.
                    // TODO: remove multiple items at once!
                    TKey minKey = default(TKey);
                    long minId = long.MaxValue;
                    foreach (KeyValuePair<TKey, CacheEntry> pair in _data)
                    {
                        if (pair.Value.Id < minId)
                        {
                            minId = pair.Value.Id;
                            minKey = pair.Key;
                        }
                    }
                    if (this.OnRemove != null)
                    { // call the OnRemove delegate.
                        this.OnRemove(_data[minKey].Value);
                    }
                    _data.Remove(minKey);
                    // update the 'last_id'
                    _lastId++;
                }
            }
        }

        /// <summary>
        /// An entry in this cache.
        /// </summary>
        private class CacheEntry
        {
            /// <summary>
            /// The id of the object.
            /// </summary>
            public long Id { get; set; }

            /// <summary>
            /// The object being cached.
            /// </summary>
            public TValue Value { get; set; }
        }

        /// <summary>
        /// Returns an enumerator that iterates through the collection.
        /// </summary>
        /// <returns></returns>
        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            return _data.Select<KeyValuePair<TKey, CacheEntry>, KeyValuePair<TKey, TValue>>(
                (source) =>
                {
                    return new KeyValuePair<TKey, TValue>(source.Key, source.Value.Value);
                }).GetEnumerator();
        }

        /// <summary>
        /// Returns an enumerator that iterates through the collection.
        /// </summary>
        /// <returns></returns>
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return _data.Select<KeyValuePair<TKey, CacheEntry>, KeyValuePair<TKey, TValue>>(
                (source) =>
                {
                    return new KeyValuePair<TKey, TValue>(source.Key, source.Value.Value);
                }).GetEnumerator();
        }
    }
}
