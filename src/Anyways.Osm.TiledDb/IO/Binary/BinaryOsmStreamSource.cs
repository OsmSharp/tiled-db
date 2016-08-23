using OsmSharp.Streams;
using System;
using OsmSharp;
using System.IO;

namespace Anyways.Osm.TiledDb.IO.Binary
{
    public class BinaryOsmStreamSource : OsmStreamSource
    {
        private readonly Stream _stream;

        public BinaryOsmStreamSource(Stream stream)
        {
            _stream = stream;
        }

        public override bool CanReset
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public override OsmGeo Current()
        {
            throw new NotImplementedException();
        }

        public override bool MoveNext(bool ignoreNodes, bool ignoreWays, bool ignoreRelations)
        {
            throw new NotImplementedException();
        }

        public override void Reset()
        {
            throw new NotImplementedException();
        }
    }
}
