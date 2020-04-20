using System;

namespace OsmSharp.Db.Tiled.Tiles
{
    internal static class Tile
    {
        public static (uint x, uint y) FromWorld(double longitude, double latitude, uint zoom)
        {
            var n = (int) Math.Floor(Math.Pow(2, zoom)); // replace by bit shifting?

            var rad = (latitude / 180d) * System.Math.PI;

            var x = (uint) ((longitude + 180.0f) / 360.0f * n);
            var y = (uint) (
                (1.0f - Math.Log(Math.Tan(rad) + 1.0f / Math.Cos(rad))
                 / Math.PI) / 2f * n);
            
            return (x, y);
        }
        
        public static uint ToLocalId(uint x, uint y, uint zoom)
        {
            var xMax = (1 << (int) zoom);
            return (uint)(y * xMax + x);
        }
        
        public static (uint x, uint y) FromLocalId(uint zoom, uint tileId)
        {
            var xMax = (ulong) (1 << (int)zoom);

            return ((uint) (tileId % xMax), (uint) (tileId / xMax));
        }
    }
}