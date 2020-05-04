using System.IO;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.OsmTiled.IO;

namespace OsmSharp.Db.Tiled.OsmTiled.Data
{
    internal class OsmTiledDbOsmGeoIndex
    {
        private readonly Stream _data;

        public OsmTiledDbOsmGeoIndex(Stream data)
        {
            _data = data;
        }

        public void Append(OsmGeoKey id, long pointer)
        {
            _data.Write(id);
            _data.WriteInt64(pointer);
        }

        public long? Get(OsmGeoKey id)
        {
            var encoded = OsmGeoCoder.Encode(id);
            var pointer = Find(encoded);
            if (pointer == null) return null;

            _data.Seek(pointer.Value + 8, SeekOrigin.Begin);
            return _data.ReadInt64();
        }
        
        private long? Find(long encoded)
        {
            long start = 0;
            long end = _data.Length / 16;

            long middle = (end + start) / 2;
            _data.Seek(middle * 16, SeekOrigin.Begin);
            var middleId = _data.ReadInt64();
            while (middleId != encoded)
            {
                if (middleId > encoded)
                {
                    if (end == middle) return null;
                    end = middle;
                }
                else
                {
                    if (start == middle) return null;
                    start = middle;
                }
                
                middle = (end + start) / 2;
                _data.Seek(middle * 16, SeekOrigin.Begin);
                middleId = _data.ReadInt64();
            }

            return middle * 16;
        }
    }
}