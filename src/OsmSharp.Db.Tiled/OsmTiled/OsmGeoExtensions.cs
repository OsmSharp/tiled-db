using System;
using System.Collections.Generic;
using System.IO;
using OsmSharp.Db.Tiled.Tiles;

namespace OsmSharp.Db.Tiled.OsmTiled
{
    internal static class OsmGeoExtensions
    {
        public static (uint x, uint y) GetTile(this Node node, uint zoom)
        {
            if (node.Latitude == null || node.Longitude == null)
                throw new InvalidDataException("Cannot store node without a valid location.");

            return Tile.FromWorld(node.Longitude.Value, node.Latitude.Value, zoom);
        }

        public static IEnumerable<(uint x, uint y)> GetTile(this OsmGeo osmGeo, uint zoom, Func<OsmGeoKey, IEnumerable<(uint x, uint y)>> getTile)
        {
            if (osmGeo is Node node)
            {
                yield return node.GetTile(zoom);
            }
            else if (osmGeo is Way way)
            {
                if (way.Nodes == null) yield break;

                foreach (var n in way.Nodes)
                {
                    foreach (var tile in getTile(new OsmGeoKey(OsmGeoType.Node, n)))
                    {
                        yield return tile;
                    }
                }
            }
            else if (osmGeo is Relation relation)
            {
                if (relation.Members == null) yield break;

                foreach (var m in relation.Members)
                {
                    foreach (var tile in getTile(new OsmGeoKey(m.Type, m.Id)))
                    {
                        yield return tile;
                    }
                }
            }
        }
    }
}