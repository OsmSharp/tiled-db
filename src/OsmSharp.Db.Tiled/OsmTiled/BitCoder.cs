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
    }
}