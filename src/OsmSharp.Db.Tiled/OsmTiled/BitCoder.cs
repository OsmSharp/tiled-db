using System;
using System.IO;

namespace OsmSharp.Db.Tiled.OsmTiled
{
    internal static class BitCoder
    {
        private const byte Mask = 128 - 1;

        public static long WriteDynamicUInt32(this Stream stream, uint value)
        {
            var d0 = (byte) (value & Mask);
            value >>= 7;
            if (value == 0)
            {
                stream.WriteByte(d0);
                return 1;
            }

            d0 += 128;
            var d1 = (byte) (value & Mask);
            value >>= 7;
            if (value == 0)
            {
                stream.WriteByte(d0);
                stream.WriteByte(d1);
                return 2;
            }

            d1 += 128;
            var d2 = (byte) (value & Mask);
            value >>= 7;
            if (value == 0)
            {
                stream.WriteByte(d0);
                stream.WriteByte(d1);
                stream.WriteByte(d2);
                return 3;
            }

            d2 += 128;
            var d3 = (byte) (value & Mask);
            value >>= 7;
            if (value == 0)
            {
                stream.WriteByte(d0);
                stream.WriteByte(d1);
                stream.WriteByte(d2);
                stream.WriteByte(d3);
                return 4;
            }

            d3 += 128;
            var d4 = (byte) (value & Mask);
            stream.WriteByte(d0);
            stream.WriteByte(d1);
            stream.WriteByte(d2);
            stream.WriteByte(d3);
            stream.WriteByte(d4);
            return 5;
        }
        
        public static long ReadDynamicUInt32(this Stream stream, out uint value)
        {
            var d = stream.ReadByte();
            if (d < 128)
            {
                value = (uint)d;
                return 1;
            }
            value = (uint)d - 128;
            d = stream.ReadByte();
            if (d < 128)
            {
                value += ((uint)d << 7);
                return 2;
            }
            d -= 128;
            value += ((uint)d << 7);
            d = stream.ReadByte();
            if (d < 128)
            {
                value += ((uint)d << 14);
                return 3;
            }
            d -= 128;
            value += ((uint)d << 14);
            d = stream.ReadByte();
            if (d < 128)
            {
                value += ((uint)d << 21);
                return 4;
            }
            d -= 128;
            value += ((uint)d << 21);
            d = stream.ReadByte();
            value += ((uint) d << 28);
            return 5;
        }
        
        private const long IdTypeMask = (long) 1 << 61;

        public static long Encode(OsmGeoKey id)
        {
            return Encode(id.Type, id.Id);
        }

        public static long Encode(OsmGeoType type, long id)
        {
            return type switch
            {
                OsmGeoType.Node => id,
                OsmGeoType.Way => (id + IdTypeMask),
                OsmGeoType.Relation => (id + (IdTypeMask * 2)),
                _ => throw new ArgumentOutOfRangeException(nameof(type), type, null)
            };
        }

        public static void Write(this Stream stream, OsmGeoKey key)
        {
            var id = Encode(key.Type, key.Id);

            stream.WriteInt64(id);
        }

        public static void WriteUInt32(this Stream stream, uint value)
        {
            for (var b = 0; b < 4; b++)
            {
                stream.WriteByte((byte)(value & byte.MaxValue));
                value >>= 8;
            }
        }

        public static ulong ReadUInt32(this Stream stream)
        {
            var value = 0UL;
            for (var b = 0; b < 4; b++)
            {
                value += ((ulong)stream.ReadByte() << (b * 8));
            }

            return value;
        }
        
        public static void WriteInt64(this Stream stream, long value)
        {
            for (var b = 0; b < 8; b++)
            {
                stream.WriteByte((byte)(value & byte.MaxValue));
                value >>= 8;
            }
        }
        
        // TODO: convert this to something allocation-less.

        /// <summary>
        /// Reads an int64 for the given stream.
        /// </summary>
        /// <param name="stream">The stream.</param>
        /// <returns>The result.</returns>
        public static long ReadInt64(this Stream stream)
        {
            var longBytes = new byte[8];
            stream.Read(longBytes, 0, 8);
            return BitConverter.ToInt64(longBytes, 0);
        }
    }
}