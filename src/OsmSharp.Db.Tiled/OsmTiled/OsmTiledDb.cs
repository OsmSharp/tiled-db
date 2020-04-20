using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using OsmSharp.Db.Tiled.Indexes.TileMap;
using OsmSharp.Db.Tiled.IO;
using OsmSharp.Db.Tiled.OsmTiled.Tiles;
using OsmSharp.Db.Tiled.Tiles;

namespace OsmSharp.Db.Tiled.OsmTiled
{
    /// <summary> 
    /// Represents a snapshot of OSM data at a given point in time represented by a full copy of the data.
    /// </summary>
    public class OsmTiledDb : OsmTiledDbBase
    {
        private readonly SparseArray _nodeTileMap;
        private readonly OsmGeoIdToTileMap _wayTileMap;
        private readonly OsmGeoIdToTileMap _relationTileMap;
        
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

        private (SparseArray nodeTileMap, OsmGeoIdToTileMap wayTileMap, OsmGeoIdToTileMap relationTileMap) LoadIndexes()
        {
            SparseArray nodeTileMap = null;
            OsmGeoIdToTileMap wayTileMap = null;
            OsmGeoIdToTileMap relationTileMap = null;
            var nodeMapFile = FileSystemFacade.FileSystem.Combine(this.Path, ".nodes.idx");
            if (FileSystemFacade.FileSystem.Exists(nodeMapFile))
            {
                using var nodeMapStream = FileSystemFacade.FileSystem.OpenRead(nodeMapFile);
                nodeTileMap = SparseArray.Deserialize(nodeMapStream);
            }
            
            var wayMapFile = FileSystemFacade.FileSystem.Combine(this.Path, ".ways.idx");
            if (FileSystemFacade.FileSystem.Exists(nodeMapFile))
            {
                using var wayMapStream = FileSystemFacade.FileSystem.OpenRead(wayMapFile);
                wayTileMap = OsmGeoIdToTileMap.Deserialize(wayMapStream);
            }
            
            var relationMapFile = FileSystemFacade.FileSystem.Combine(this.Path, ".relations.idx");
            if (FileSystemFacade.FileSystem.Exists(relationMapFile))
            {
                using var relationMapStream = FileSystemFacade.FileSystem.OpenRead(relationMapFile);
                relationTileMap = OsmGeoIdToTileMap.Deserialize(relationMapStream);
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
                    dataTile = await this.GetTile((tile.x, tile.y, this.Zoom));
                    break;
                case OsmGeoType.Way:
                    var wayTiles = _wayTileMap.Get(id);
                    var wayTileId = wayTiles?.FirstOrDefault();
                    if (wayTileId != null)
                    {
                        var wayTile = Tile.FromLocalId(this.Zoom, wayTileId.Value);
                        dataTile = await this.GetTile((wayTile.x, wayTile.y, this.Zoom));
                    }
                    break;
                case OsmGeoType.Relation:
                    var relationTiles = _relationTileMap.Get(id);
                    var relationTileId = relationTiles?.FirstOrDefault();
                    if (relationTileId != null)
                    {
                        var relationTile = Tile.FromLocalId(this.Zoom, relationTileId.Value);
                        dataTile = await this.GetTile((relationTile.x, relationTile.y, this.Zoom));
                    }
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(type), type, null);
            }
            
            return dataTile?.Get(type, id);
        }
    }
}