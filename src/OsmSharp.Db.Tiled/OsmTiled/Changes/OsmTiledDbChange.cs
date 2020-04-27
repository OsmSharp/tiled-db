using System.Collections.Generic;

namespace OsmSharp.Db.Tiled.OsmTiled.Changes
{
    internal class OsmTiledDbChange
    {
        
        
        public Dictionary<OsmGeoKey, OsmGeo> Mutations { get; } = new Dictionary<OsmGeoKey, OsmGeo>();
    }
}