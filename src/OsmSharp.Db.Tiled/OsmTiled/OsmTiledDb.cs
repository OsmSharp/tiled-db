using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using OsmSharp.Db.Tiled.Indexes.TileMaps;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.OsmTiled.IO;
using OsmSharp.Db.Tiled.OsmTiled.Tiles;
using OsmSharp.Db.Tiled.Tiles;

namespace OsmSharp.Db.Tiled.OsmTiled
{
    /// <summary> 
    /// Represents a snapshot of OSM data at a given point in time represented by a full copy of the data.
    /// </summary>
    public class OsmTiledDb : OsmTiledDbBase
    {
        private readonly TileMap _nodeTileMap;
        private readonly TilesMap _wayTileMap;
        private readonly TilesMap _relationTileMap;
        
        /// <summary>
        /// Creates a new db using the data at the given path.
        /// </summary>
        public OsmTiledDb(string path)
            : base(path)
        {
            (_nodeTileMap, _wayTileMap, _relationTileMap) = LoadIndexes();
        }

        internal OsmTiledDb(string path, OsmTiledDbMeta meta)
            : base(path, meta)
        {
            (_nodeTileMap, _wayTileMap, _relationTileMap) = LoadIndexes();
        }

        private (TileMap nodeTileMap, TilesMap wayTileMap, TilesMap relationTileMap) LoadIndexes()
        {
            TileMap nodeTileMap = null;
            TilesMap wayTileMap = null;
            TilesMap relationTileMap = null;
            var nodeMapFile = OsmTiledDbOperations.PathToIndex(this.Path, OsmGeoType.Node);
            if (FileSystemFacade.FileSystem.Exists(nodeMapFile))
            {
                using var nodeMapStream = FileSystemFacade.FileSystem.OpenRead(nodeMapFile);
                nodeTileMap = TileMap.Deserialize(nodeMapStream);
            }
            
            var wayMapFile = OsmTiledDbOperations.PathToIndex(this.Path, OsmGeoType.Way);
            if (FileSystemFacade.FileSystem.Exists(nodeMapFile))
            {
                using var wayMapStream = FileSystemFacade.FileSystem.OpenRead(wayMapFile);
                wayTileMap = TilesMap.Deserialize(wayMapStream);
            }
            
            var relationMapFile = OsmTiledDbOperations.PathToIndex(this.Path, OsmGeoType.Relation);
            if (FileSystemFacade.FileSystem.Exists(relationMapFile))
            {
                using var relationMapStream = FileSystemFacade.FileSystem.OpenRead(relationMapFile);
                relationTileMap = TilesMap.Deserialize(relationMapStream);
            }

            return (nodeTileMap, wayTileMap, relationTileMap);
        }

        /// <inheritdoc/>
        public override async Task<OsmGeo> Get(OsmGeoType type, long id)
        {
            OsmDbTile dataTile = null;
            switch (type)
            {
                case OsmGeoType.Node:
                    var tileId = _nodeTileMap[id];
                    if (tileId == 0) return null;
                    var tile = Tile.FromLocalId(this.Zoom, tileId);
                    dataTile = await this.GetTile((tile.x, tile.y));
                    break;
                case OsmGeoType.Way:
                    var wayTiles = _wayTileMap.Get(id);
                    foreach (var wayTileId in wayTiles)
                    {
                        var wayTile = Tile.FromLocalId(this.Zoom, wayTileId);
                        dataTile = await this.GetTile((wayTile.x, wayTile.y));
                        break;
                    }
                    break;
                case OsmGeoType.Relation:
                    var relationTiles = _relationTileMap.Get(id);
                    foreach (var relationTileId in relationTiles)
                    {
                        var relationTile = Tile.FromLocalId(this.Zoom, relationTileId);
                        dataTile = await this.GetTile((relationTile.x, relationTile.y));
                        break;
                    }
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(type), type, null);
            }
            
            return dataTile?.Get(type, id);
        }

        /// <inheritdoc/>
        public override async Task<IEnumerable<OsmGeo>> Get((uint x, uint y) tile)
        {
            var dataTile = await this.GetTile(tile);

            return dataTile.Get();
        }
    }
}