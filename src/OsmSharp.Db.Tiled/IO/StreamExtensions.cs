using System;
using System.IO;

namespace OsmSharp.Db.Tiled.IO
{
    internal static class StreamExtensions
    {
        /// <summary>
        /// Writes the given value with size prefix.
        /// </summary>
        public static long WriteWithSize(this System.IO.Stream stream, string value)
        {
            byte[] bytes;
            if (value == null)
            {
                bytes = new byte [0];
            }
            else
            {
                bytes = System.Text.Encoding.Unicode.GetBytes(value);
            }
            return stream.WriteWithSize(bytes);
        }

        /// <summary>
        /// Writes the given value with size prefix.
        /// </summary>
        public static long WriteWithSize(this System.IO.Stream stream, byte[] value)
        {
            stream.Write(System.BitConverter.GetBytes((long)value.Length), 0, 8);
            stream.Write(value, 0, value.Length);
            return value.Length + 8;
        }

        /// <summary>
        /// Reads a string.
        /// </summary>
        public static string ReadWithSizeString(this System.IO.Stream stream)
        {
            var longBytes = new byte[8];
            stream.Read(longBytes, 0, 8);
            var size = BitConverter.ToInt64(longBytes, 0);
            var data = new byte[size];
            stream.Read(data, 0, (int)size);

            return System.Text.Encoding.Unicode.GetString(data, 0, data.Length);
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