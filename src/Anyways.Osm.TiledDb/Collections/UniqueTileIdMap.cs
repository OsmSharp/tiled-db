//namespace Anyways.Osm.TiledDb.Collections
//{
//    /// <summary>
//    /// A unique tile id map, only one tile per id.
//    /// </summary>
//    public class UniqueTileIdMap
//    {
//        private readonly HugeDictionary<long, byte[]> _blocks;
//        private readonly int _blockSize;
//        private readonly byte _defaultValue = byte.MaxValue;

//        /// <summary>
//        /// Creates a new tile id map.
//        /// </summary>
//        public UniqueTileIdMap(int blockSize = 256)
//        {
//            _blocks = new HugeDictionary<long, byte[]>();
//            _blockSize = blockSize;
//        }

//        /// <summary>
//        /// Sets a tile id.
//        /// </summary>
//        public void Set(long id, byte tileId)
//        {
//            var block = id / _blockSize;
//            var offset = id - (block * _blockSize);

//            byte[] array;
//            if (!_blocks.TryGetValue(block, out array))
//            {
//                array = new byte[_blockSize];
//                for(var i = 0; i < array.Length; i++)
//                {
//                    array[i] = _defaultValue;
//                }
//                _blocks[block] = array;
//            }
//            array[offset] = tileId;
//        }

//        /// <summary>
//        /// Gets or sets the tile id for the given id.
//        /// </summary>
//        public byte this[long id]
//        {
//            get
//            {
//                return this.Get(id);
//            }
//            set
//            {
//                this.Set(id, value);
//            }
//        }

//        /// <summary>
//        /// Gets a tile id.
//        /// </summary>
//        public byte Get(long id)
//        {
//            var block = id / _blockSize;
//            var offset = id - (block * _blockSize);

//            byte[] array;
//            if (!_blocks.TryGetValue(block, out array))
//            {
//                return _defaultValue;
//            }
//            return array[offset];
//        }
//    }
//}