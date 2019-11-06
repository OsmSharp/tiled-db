using System.Collections.Generic;

namespace OsmSharp.Db.Tiled.Indexes.InMemory
{
    internal class MemoryIndex : IIndex
    {
        private readonly Dictionary<long, int> _data;

        public MemoryIndex()
        {
            _data = new Dictionary<long, int>();
        }

        public void Add(long id, int mask)
        {
            _data[id] = mask;
        }

        public bool TryGetMask(long id, out int mask)
        {
            return _data.TryGetValue(id, out mask);
        }

        public Index ToIndex()
        {
            var index = new Index();

            foreach (var data in _data)
            {
                index.Add(data.Key, data.Value);
            }

            return index;
        }

        public static MemoryIndex FromIndex(Index loadedIndex)
        {
            var memoryIndex = new MemoryIndex();

            for (var i = 0; i < loadedIndex.Count; i++)
            {
                var (id, mask) = loadedIndex[i];
                memoryIndex.Add(id, mask);
            }

            return memoryIndex;
        }
    }
}