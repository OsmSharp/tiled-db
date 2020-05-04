using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using OsmSharp.Db.Tiled.Indexes.TileMaps;
using OsmSharp.Db.Tiled.Tiles;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.OsmTiled.Changes;
using OsmSharp.Db.Tiled.OsmTiled.Data;
using OsmSharp.Db.Tiled.OsmTiled.IO;

namespace OsmSharp.Db.Tiled.OsmTiled.Build
{
    /// <summary>
    /// Builds an OSM tiled db from an OSM stream.
    /// </summary>
    internal static class OsmTiledDbBuilder
    {
        /// <summary>
        /// Builds a new database and write the structure to the given path.
        /// </summary>
        /// <param name="source">The source stream.</param>
        /// <param name="path">The path to store the db at.</param>
        /// <param name="zoom">The zoom.</param>
        /// <param name="settings">The settings.</param>
        /// <param name="timeStamp">The timestamp from the diff meta-data override the timestamps in the data.</param>
        public static OsmTiledDbMeta Build(this IEnumerable<OsmGeo> source, string path, uint zoom = 14,
            OsmTiledDbBuildSettings? settings = null, DateTime? timeStamp = null)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));
            if (path == null) throw new ArgumentNullException(nameof(path));
            if (!FileSystemFacade.FileSystem.DirectoryExists(path)) throw new ArgumentException("Output path does not exist.");

            settings ??= new OsmTiledDbBuildSettings();

            var buffer = new byte[1024];

            var dataLatestTimeStamp = DateTime.MinValue;
            var nodeToTile = new TileMap();
            var wayToTiles = new TilesMap();
            var relationToTiles = new TilesMap();

            using var dataBase = FileSystemFacade.FileSystem.Open(
                OsmTiledDbOperations.PathToData(path), FileMode.Create);
            var data = new HugeBufferedStream(dataBase);
            using var dataTilesIndex = FileSystemFacade.FileSystem.Open(
                OsmTiledDbOperations.PathToTileIndex(path), FileMode.Create);
            using var dataIdIndex = FileSystemFacade.FileSystem.Open(
                OsmTiledDbOperations.PathToIdIndex(path), FileMode.Create);

            var tiledStream =
                new OsmTiledLinkedStream(data, pointersCacheSize: OsmTiledLinkedStream.PointerCacheSizeDefault);
            var idIndex = new OsmTiledDbOsmGeoIndex(dataIdIndex);
            var tileSet = new HashSet<uint>();
            var mode = OsmGeoType.Node;
            foreach (var osmGeo in source)
            {
                switch (mode)
                {
                    case OsmGeoType.Way when osmGeo.Type == OsmGeoType.Node:
                    case OsmGeoType.Relation when (osmGeo.Type == OsmGeoType.Node || osmGeo.Type == OsmGeoType.Way):
                        throw new InvalidDataException("Source stream has to be sorted.");
                }

                // update timestamp.
                if (osmGeo.TimeStamp.HasValue &&
                    osmGeo.TimeStamp > dataLatestTimeStamp)
                {
                    dataLatestTimeStamp = osmGeo.TimeStamp.Value;
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

                    settings?.Prepare(node);
                    var location = tiledStream.Append(localId, node, buffer);
                    idIndex.Append(new OsmGeoKey(node), location);
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

                    settings?.Prepare(way);
                    var location = tiledStream.Append(tileSet, way, buffer);
                    idIndex.Append(new OsmGeoKey(way), location);
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

                    settings?.Prepare(relation);
                    var location = tiledStream.Append(tileSet, relation, buffer);
                    idIndex.Append(new OsmGeoKey(relation), location);
                }
            }

            // reverse indexed data and save tile index.
            tiledStream.Flush();
            tiledStream.SerializeIndex(dataTilesIndex);

            // choose proper timestamp.
            timeStamp ??= dataLatestTimeStamp;

            // save the meta-data.
            var meta = new OsmTiledDbMeta
            {
                Id = timeStamp.Value.ToUnixTime(),
                Base = null, // this is a full db.
                Type = OsmTiledDbType.Full,
                Zoom = zoom
            };
            OsmTiledDbOperations.SaveDbMeta(path, meta);
            return meta;
        }

    }
}