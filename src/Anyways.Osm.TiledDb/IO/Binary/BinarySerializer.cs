using OsmSharp;
using System.IO;

namespace Anyways.Osm.TiledDb.IO.PBF
{
    public static class BinarySerializer
    {
        private static System.Text.Encoder _encoder = (new System.Text.UnicodeEncoding()).GetEncoder();


        public static int Append(this Stream stream, Node node)
        {
            var size = 0;
            var beginning = stream.Position;
            stream.Seek(4, SeekOrigin.Current);

            // write data.
            stream.WriteByte((byte)1); // a node.
            size += 1;
            size += stream.AppendOsmGeo(node);

            var streamWriter = new StreamWriter(stream);
            streamWriter.Write(node.Latitude.Value);
            streamWriter.Write(node.Longitude.Value);

            // write size.
            stream.Seek(beginning, SeekOrigin.Begin);
            streamWriter.Write(size);
            stream.Seek(beginning + size, SeekOrigin.Begin);

            return size;
        }

        public static int Append(this Stream stream, Way way)
        {
            var size = 0;
            var beginning = stream.Position;
            stream.Seek(4, SeekOrigin.Current);

            // write data.
            stream.WriteByte((byte)2); // a way.
            size += 1;
            size += stream.AppendOsmGeo(way);

            var streamWriter = new StreamWriter(stream);
            if (way.Nodes == null ||
                way.Nodes.Length == 0)
            {
                streamWriter.Write(0);
                size += 4;
            }
            else
            {
                streamWriter.Write(way.Nodes.Length);
                size += 4;
                for (var i = 0; i < way.Nodes.Length; i++)
                {
                    streamWriter.Write(way.Nodes[i]);
                    size += 8;
                }
            }

            // write size.
            stream.Seek(beginning, SeekOrigin.Begin);
            streamWriter.Write(size);
            stream.Seek(beginning + size, SeekOrigin.Begin);

            return size;
        }

        public static int Append(this Stream stream, Relation relation)
        {
            var size = 0;
            var beginning = stream.Position;
            stream.Seek(4, SeekOrigin.Current);

            // write data.
            stream.WriteByte((byte)3); // a relation.
            size += 1;
            size += stream.AppendOsmGeo(relation);

            var streamWriter = new StreamWriter(stream);
            if (relation.Members == null ||
                relation.Members.Length == 0)
            {
                streamWriter.Write(0);
                size += 4;
            }
            else
            {
                streamWriter.Write(relation.Members.Length);
                size += 4;
                for (var i = 0; i < relation.Members.Length; i++)
                {
                    streamWriter.Write(relation.Members[i].Id);
                    size += 8;
                    size += stream.WriteWithSize(relation.Members[i].Role);
                    switch (relation.Members[i].Type)
                    {
                        case OsmGeoType.Node:
                            streamWriter.Write((byte)1);
                            break;
                        case OsmGeoType.Way:
                            streamWriter.Write((byte)2);
                            break;
                        case OsmGeoType.Relation:
                            streamWriter.Write((byte)3);
                            break;
                    }
                    size += 1;
                }
            }

            // write size.
            stream.Seek(beginning, SeekOrigin.Begin);
            streamWriter.Write(size);
            stream.Seek(beginning + size, SeekOrigin.Begin);

            return size;
        }

        private static int AppendOsmGeo(this Stream stream, OsmGeo osmGeo)
        {
            var size = 0;

            var streamWriter = new StreamWriter(stream);
            streamWriter.Write(osmGeo.Id.Value);
            size += 8;
            streamWriter.Write(osmGeo.ChangeSetId.Value);
            size += 8;
            streamWriter.Write(osmGeo.TimeStamp.Value.Ticks);
            size += 8;
            streamWriter.Write(osmGeo.UserId.Value);
            size += 8;
            size += stream.WriteWithSize(osmGeo.UserName);
            streamWriter.Write(osmGeo.Version.Value);
            size += 4;

            if (osmGeo.Tags == null ||
                osmGeo.Tags.Count == 0)
            {
                streamWriter.Write((byte)0);
                size += 1;
            }
            else
            {
                streamWriter.Write((byte)osmGeo.Tags.Count);
                size += 1;
                foreach (var t in osmGeo.Tags)
                {
                    size += stream.WriteWithSize(t.Key);
                    size += stream.WriteWithSize(t.Value);
                }
            }
            return size;
        }

        public static int WriteWithSize(this Stream stream, string value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                stream.WriteByte(0);
                return 1;
            }
            else
            {
                var bytes = System.Text.Encoding.Unicode.GetBytes(value);
                stream.WriteByte((byte)bytes.Length);
                stream.Write(bytes, 0, bytes.Length);
                return bytes.Length + 1;
            }
        }

        public static int Read(this Stream stream, Node node)
        {

        }

        private static int Read(this Stream stream, OsmGeo osmGeo)
        {
            var streamReader = new StreamReader(stream);

            osmGeo.Id = streamReader.Read()

            streamWriter.Write(osmGeo.Id.Value);
            size += 8;
            streamWriter.Write(osmGeo.ChangeSetId.Value);
            size += 8;
            streamWriter.Write(osmGeo.TimeStamp.Value.Ticks);
            size += 8;
            streamWriter.Write(osmGeo.UserId.Value);
            size += 8;
            size += stream.WriteWithSize(osmGeo.UserName);
            streamWriter.Write(osmGeo.Version.Value);
            size += 4;

            if (osmGeo.Tags == null ||
                osmGeo.Tags.Count == 0)
            {
                streamWriter.Write((byte)0);
                size += 1;
            }
            else
            {
                streamWriter.Write((byte)osmGeo.Tags.Count);
                size += 1;
                foreach (var t in osmGeo.Tags)
                {
                    size += stream.WriteWithSize(t.Key);
                    size += stream.WriteWithSize(t.Value);
                }
            }
        }
    }
}