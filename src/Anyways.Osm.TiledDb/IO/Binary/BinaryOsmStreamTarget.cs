using OsmSharp;
using System.IO;
using Anyways.Osm.TiledDb.IO.PBF;

namespace Anyways.Osm.TiledDb.IO.Binary
{
    /// <summary>
    /// A stream target that just writes objects in binary format.
    /// </summary>
    public class BinaryOsmStreamTarget : OsmSharp.Streams.OsmStreamTarget
    {
        private readonly Stream _stream;

        /// <summary>
        /// Creates a new stream target.
        /// </summary>
        public BinaryOsmStreamTarget(Stream stream)
        {
            _stream = stream;
        }

        /// <summary>
        /// Adds a node.
        /// </summary>
        /// <param name="node"></param>
        public override void AddNode(Node node)
        {
            _stream.Append(node);
        }

        /// <summary>
        /// Adds a relation.
        /// </summary>
        /// <param name="relation"></param>
        public override void AddRelation(Relation relation)
        {
            _stream.Append(relation);
        }

        /// <summary>
        /// Adds a way.
        /// </summary>
        /// <param name="way"></param>
        public override void AddWay(Way way)
        {
            _stream.Append(way);
        }

        /// <summary>
        /// Initializes this target.
        /// </summary>
        public override void Initialize()
        { 

        }
    }
}