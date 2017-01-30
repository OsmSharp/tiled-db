using OsmSharp.Streams;
using OsmSharp;
using System.IO;

namespace Anyways.Osm.TiledDb.IO.Binary
{
    /// <summary>
    /// A stream source that just reads objects in binary format.
    /// </summary>
    public class BinaryOsmStreamSource : OsmStreamSource
    {
        private readonly Stream _stream;
        private readonly byte[] _buffer;

        /// <summary>
        /// Creates a new binary stream source.
        /// </summary>
        public BinaryOsmStreamSource(Stream stream)
        {
            _stream = stream;
            _buffer = new byte[1024];
        }

        /// <summary>
        /// Returns true if this source can be reset.
        /// </summary>
        public override bool CanReset
        {
            get
            {
                return _stream.CanSeek;
            }
        }

        /// <summary>
        /// Returns the current object.
        /// </summary>
        /// <returns></returns>
        public override OsmGeo Current()
        {
            return _current;
        }

        private OsmGeo _current;

        /// <summary>
        /// Move to the next object in this stream source.
        /// </summary>
        public override bool MoveNext(bool ignoreNodes, bool ignoreWays, bool ignoreRelations)
        {
            if (_stream.Length == _stream.Position + 1)
            {
                return false;
            }

            var osmGeo = this.DoMoveNext();
            while(osmGeo != null)
            {
                switch(osmGeo.Type)
                {
                    case OsmGeoType.Node:
                        if (!ignoreNodes)
                        {
                            _current = osmGeo;
                            return true;
                        }
                        break;
                    case OsmGeoType.Way:
                        if (!ignoreWays)
                        {
                            _current = osmGeo;
                            return true;
                        }
                        break;
                    case OsmGeoType.Relation:
                        if (!ignoreRelations)
                        {
                            _current = osmGeo;
                            return true;
                        }
                        break;
                }
                osmGeo = this.DoMoveNext();
            }
            return false;
        }

        private OsmGeo DoMoveNext()
        {
            return PBF.BinarySerializer.ReadOsmGeo(_stream, _buffer);
        }

        /// <summary>
        /// Resets this stream.
        /// </summary>
        public override void Reset()
        {
            _current = null;

            _stream.Seek(0, SeekOrigin.Begin);
        }
    }
}
