using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using OsmSharp.IO.Binary;
using Reminiscence;
using Reminiscence.Arrays;

[assembly: InternalsVisibleTo("OsmSharp.Db.Tiled.Tests")]
namespace OsmSharp.Db.Tiled.OsmTiled.Tiles
{
    internal class OsmDbTile
    {
        private readonly ArrayBase<uint> _pointers;
        private readonly ArrayBase<long> _ids;
        private readonly Stream _data;

        private OsmDbTile(Stream data)
        {
            _data = data;
            _pointers = new MemoryArray<uint>(0);
            _ids = new MemoryArray<long>(0);
            
            this.BuildPointers();
        }

        private OsmDbTile(ArrayBase<long> ids, ArrayBase<uint> pointers, Stream data)
        {
            _data = data;
            _pointers = pointers;
            _ids = ids;
        }

        private void BuildPointers()
        {
            var buffer = new byte[1024];
            var p = 0L;
            var pointer = _data.Position;
            var osmGeo = _data.ReadOsmGeo(buffer);
            while (osmGeo != null)
            {
                if (osmGeo.Id == null) throw new InvalidDataException($"Cannot store {nameof(OsmGeo)} objects without a valid id.");
                var id = Encode(osmGeo.Type, osmGeo.Id.Value);
                _ids.EnsureMinimumSize(p + 1);
                _pointers.EnsureMinimumSize(p + 1);

                _ids[p] = id;
                _pointers[p] = (uint)pointer;
                
                p++;
                pointer = _data.Position;
                osmGeo = _data.ReadOsmGeo(buffer);
            }

            _ids.Resize(p);
            _pointers.Resize(p);
        }

        public IEnumerable<OsmGeo> Get(byte[]? buffer = null)
        {
            buffer ??= new byte[1024];
            _data.Seek(0, SeekOrigin.Begin);

            var next = _data.ReadOsmGeo(buffer);
            while (next != null)
            {
                yield return next;
                next = _data.ReadOsmGeo(buffer);
            }
        }
        
        public OsmGeo? Get(OsmGeoType type, long id, byte[]? buffer = null)
        {
            buffer ??= new byte[1024];
            
            var encoded = Encode(type, id);
            var pointer = Find(encoded);
            if (pointer == null) return null;

            _data.Seek(pointer.Value, SeekOrigin.Begin);
            return _data.ReadOsmGeo(buffer);
        }

        private uint? Find(long encoded)
        {
            uint start = 0;
            uint end = (uint)_ids.Length;

            uint middle = (end + start) / 2;
            var middleId = _ids[middle];
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
                middleId = _ids[middle];
            }

            return _pointers[middle];
        }

        private const long IdTypeMask = (long) 1 << 61;

        private long Encode(OsmGeoType type, long id)
        {
            return type switch
            {
                OsmGeoType.Node => id,
                OsmGeoType.Way => (id + IdTypeMask),
                OsmGeoType.Relation => (id + (IdTypeMask * 2)),
                _ => throw new ArgumentOutOfRangeException(nameof(type), type, null)
            };
        }

        internal static async Task<OsmDbTile> BuildFromOsmBinaryStream(Stream stream)
        {
            var memoryStream = new MemoryStream();
            await stream.CopyToAsync(memoryStream);
            memoryStream.Seek(0, SeekOrigin.Begin);
            
            return new OsmDbTile(memoryStream);
        }

        internal async Task<long> Serialize(Stream stream)
        {
            var pos = stream.Position;
            stream.WriteByte(1); // the version #.
            _ids.CopyToWithSize(stream);
            _pointers.CopyToWithSize(stream);
            _data.Seek(0, SeekOrigin.Begin);
            await _data.CopyToAsync(stream);

            return stream.Position - pos;
        }

        internal static async Task<OsmDbTile> Deserialize(Stream stream)
        {
            var version = stream.ReadByte();
            if (version != 1) throw new InvalidDataException("Version number invalid, cannot read tile.");
            var ids = MemoryArray<long>.CopyFromWithSize(stream);
            var pointers = MemoryArray<uint>.CopyFromWithSize(stream);
            var memoryArray = new MemoryStream();
            await stream.CopyToAsync(memoryArray);
            
            return new OsmDbTile(ids, pointers, memoryArray);
        }
    }
}