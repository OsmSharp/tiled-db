using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using OsmSharp.Db.Tiled.Indexes.TileMaps;
using OsmSharp.Db.Tiled.Tiles;

namespace OsmSharp.Db.Tiled.OsmTiled
{
    internal static class OsmGeoTiledExtensions
    {
        public static IEnumerable<(OsmGeo osmGeo, IEnumerable<(uint x, uint y)> tiles)> ToTiledStream(
            this IEnumerable<OsmGeo> source, uint zoom)
        {      
            var nodeToTile = new TileMap();
            var wayToTiles = new TilesMap();
            var relationToTiles = new TilesMap();      
            
            var tileSet = new HashSet<uint>();
            var mode = OsmGeoType.Node;
            var singleTile = new uint[1];
            foreach (var osmGeo in source)
            {
                switch (mode)
                {
                    case OsmGeoType.Way when osmGeo.Type == OsmGeoType.Node:
                    case OsmGeoType.Relation when (osmGeo.Type == OsmGeoType.Node || osmGeo.Type == OsmGeoType.Way):
                        throw new InvalidDataException("Source stream has to be sorted.");
                }

                mode = osmGeo.Type;

                if (mode == OsmGeoType.Node)
                {
                    if (!(osmGeo is Node node)) throw new InvalidDataException("Could not cast node to node.");
                    if (node.Id == null) throw new InvalidDataException("Cannot store nodes without an id.");
                    if (node.Version == null) throw new InvalidDataException("Cannot store node without a valid version.");
                    if (!node.Latitude.HasValue || !node.Longitude.HasValue)
                        throw new InvalidDataException("Cannot store nodes without a location.");

                    var tile = Tile.FromWorld(node.Longitude.Value, node.Latitude.Value, zoom);
                    var localId = Tile.ToLocalId(tile.x, tile.y, zoom);

                    nodeToTile.EnsureMinimumSize(node.Id.Value);
                    nodeToTile[node.Id.Value] = localId;

                    singleTile[0] = localId;
                    yield return (node, singleTile.Select(x => Tile.FromLocalId(zoom, x)));
                }
                else if (osmGeo is Way way)
                {
                    if (way.Nodes == null) continue;
                    if (way.Id == null) throw new InvalidDataException("Cannot store ways without an id.");
                    if (way.Version == null) throw new InvalidDataException("Cannot store way without a valid version.");

                    tileSet.Clear();
                    foreach (var n in way.Nodes)
                    {
                        if (nodeToTile.Length <= n) continue;
                        var tile = nodeToTile[n];
                        if (tile == 0) continue;

                        tileSet.Add(tile);
                    }

                    wayToTiles.Add(way.Id.Value, tileSet);

                    yield return (way, tileSet.Select(x => Tile.FromLocalId(zoom, x)));
                }
                else if (osmGeo is Relation relation)
                {
                    if (relation.Members == null) continue;
                    if (relation.Id == null) throw new InvalidDataException("Cannot store relations without an id.");
                    if (relation.Version == null) throw new InvalidDataException("Cannot store relation without a valid version.");

                    tileSet.Clear();
                    foreach (var member in relation.Members)
                    {
                        switch (member.Type)
                        {
                            case OsmGeoType.Node:
                                if (nodeToTile.Length <= member.Id) continue;
                                var tile = nodeToTile[member.Id];
                                if (tile == 0) continue;

                                tileSet.Add(tile);
                                break;
                            case OsmGeoType.Way:
                                foreach (var t in wayToTiles.Get(member.Id))
                                {
                                    tileSet.Add(t);
                                }

                                break;
                        }
                    }

                    relationToTiles.Add(relation.Id.Value, tileSet);

                    yield return (relation, tileSet.Select(x => Tile.FromLocalId(zoom, x)));
                }
            }
        }
        
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