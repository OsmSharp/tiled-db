using System;
using System.IO;
using OsmSharp.Db.Tiled.IO;
using Reminiscence;

namespace OsmSharp.Db.Tiled
{
    /// <summary>
    /// Describes database meta-data and settings.
    /// </summary>
    public class DatabaseMeta
    {
        /// <summary>
        /// Get or sets the zoom level.
        /// </summary>
        public uint Zoom { get; set; }
        
        /// <summary>
        /// Gets or sets the base (in case of a diff db).
        /// </summary>
        public string Base { get; set; }

        /// <summary>
        /// Gets or sets the compressed flag.
        /// </summary>
        public bool Compressed { get; set; } = true;

        /// <summary>
        /// Writes to the given stream.
        /// </summary>
        /// <param name="stream">The stream.</param>
        /// <returns>The # of bytes written.</returns>
        public long Serialize(Stream stream)
        {
            var position = stream.Position;
            
            // write zoom.
            stream.WriteWithSize("zoom");
            var zoomBytes = BitConverter.GetBytes(this.Zoom);
            stream.Write(zoomBytes, 0, 4);
            
            // write base.
            stream.WriteWithSize("base");
            stream.WriteWithSize(this.Base);

            // write compressed flag.
            stream.WriteWithSize("compressed");
            stream.WriteByte((byte)(this.Compressed ? 1 : 0));

            stream.WriteWithSize("END");

            return stream.Position - position;
        }

        /// <summary>
        /// Reads from the given stream.
        /// </summary>
        /// <param name="stream">The stream.</param>
        /// <returns>The object read from the stream.</returns>
        public static DatabaseMeta Deserialize(Stream stream)
        {
            var dbMeta = new DatabaseMeta();
            
            var property = stream.ReadWithSizeString();
            while (property != "END")
            {
                switch (property)
                {
                    case "zoom":
                        var bytes = new byte[4];
                        stream.Read(bytes, 0, 4);
                        dbMeta.Zoom = BitConverter.ToUInt32(bytes, 0);
                        break;
                    case "compressed":
                        dbMeta.Compressed = stream.ReadByte() == 1;
                        break;
                    case "base":
                        dbMeta.Base = stream.ReadWithSizeString();
                        break;
                }

                property = stream.ReadWithSizeString();
            }

            return dbMeta;
        }
    }
}