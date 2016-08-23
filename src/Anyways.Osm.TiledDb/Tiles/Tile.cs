namespace Anyways.Osm.TiledDb.Tiles
{
    /// <summary>
    /// Represents a tile.
    /// </summary>
    public class Tile
    {
        private ulong _id;

        /// <summary>
        /// Creates a new tile from a given id.
        /// </summary>
        /// <param name="id"></param>
        public Tile(ulong id)
        {
            _id = id;

            Tile tile = Tile.CalculateTile(id);
            this.X = tile.X;
            this.Y = tile.Y;
            this.Zoom = tile.Zoom;
            this.CalculateBounds();
        }

        /// <summary>
        /// Creates a new tile.
        /// </summary>
        public Tile(int x, int y, int zoom)
        {
            this.X = x;
            this.Y = y;
            this.Zoom = zoom;

            _id = Tile.CalculateTileId(zoom, x, y);
            this.CalculateBounds();
        }

        private void CalculateBounds()
        {
            var n = System.Math.PI - ((2.0 * System.Math.PI * (double)this.Y) / System.Math.Pow(2.0, (double)this.Zoom));

            this.Top = (float)(((double)this.X / System.Math.Pow(2.0, (double)this.Zoom) * 360.0) - 180.0);
            this.Left = (float)(180.0 / System.Math.PI * System.Math.Atan(System.Math.Sinh(n)));
            this.Bottom = (float)(((this.X + 1) / System.Math.Pow(2.0, this.Zoom) * 360.0) - 180.0);
            this.Right = (float)(180.0 / System.Math.PI * System.Math.Atan(System.Math.Sinh(n)));
        }

        /// <summary>
        /// The X position of the tile.
        /// </summary>
        public int X { get; private set; }

        /// <summary>
        /// The Y position of the tile.
        /// </summary>
        public int Y { get; private set; }

        /// <summary>
        /// The zoom level for this tile.
        /// </summary>
        public int Zoom { get; private set; }

        /// <summary>
        /// Gets the top.
        /// </summary>
        public float Top { get; private set; }

        /// <summary>
        /// Get the bottom.
        /// </summary>
        public float Bottom { get; private set; }

        /// <summary>
        /// Get the left.
        /// </summary>
        public float Left { get; private set; }

        /// <summary>
        /// Gets the right.
        /// </summary>
        public float Right { get; private set; }

        /// <summary>
        /// Gets the parent tile.
        /// </summary>
        public Tile Parent
        {
            get
            {
                return new Tile(this.X / 2, this.Y / 2, this.Zoom - 1);
            }
        }

        /// <summary>
        /// Returns a hashcode for this tile position.
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return this.X.GetHashCode() ^
                   this.Y.GetHashCode() ^
                   this.Zoom.GetHashCode();
        }

        /// <summary>
        /// Returns true if the given object represents the same tile.
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
        {
            var other = (obj as Tile);
            if (other != null)
            {
                return other.X == this.X &&
                    other.Y == this.Y &&
                    other.Zoom == this.Zoom;
            }
            return false;
        }

        /// <summary>
        /// Returns a description for this tile.
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return string.Format("{0}x-{1}y@{2}z", this.X, this.Y, this.Zoom);
        }

        /// <summary>
        /// Calculates the tile id of the tile at position (0, 0) for the given zoom.
        /// </summary>
        /// <param name="zoom"></param>
        /// <returns></returns>
        private static ulong CalculateTileId(int zoom)
        {
            if (zoom == 0)
            { // zoom level 0: {0}.
                return 0;
            }
            else if (zoom == 1)
            {
                return 1;
            }
            else if (zoom == 2)
            {
                return 5;
            }
            else if (zoom == 3)
            {
                return 21;
            }
            else if (zoom == 4)
            {
                return 85;
            }
            else if (zoom == 5)
            {
                return 341;
            }
            else if (zoom == 6)
            {
                return 1365;
            }
            else if (zoom == 7)
            {
                return 5461;
            }
            else if (zoom == 8)
            {
                return 21845;
            }
            else if (zoom == 9)
            {
                return 87381;
            }
            else if (zoom == 10)
            {
                return 349525;
            }
            else if (zoom == 11)
            {
                return 1398101;
            }
            else if (zoom == 12)
            {
                return 5592405;
            }
            else if (zoom == 13)
            {
                return 22369621;
            }
            else if (zoom == 14)
            {
                return 89478485;
            }
            else if (zoom == 15)
            {
                return 357913941;
            }
            else if (zoom == 16)
            {
                return 1431655765;
            }
            else if (zoom == 17)
            {
                return 5726623061;
            }
            else if (zoom == 18)
            {
                return 22906492245;
            }

            ulong size = (ulong)System.Math.Pow(2, 2 * (zoom - 1));
            var tileId = Tile.CalculateTileId(zoom - 1) + size;
            return tileId;
        }

        /// <summary>
        /// Calculates the tile id of the tile at position (x, y) for the given zoom.
        /// </summary>
        /// <param name="zoom"></param>
        /// <param name="x"></param>
        /// <param name="y"></param>
        /// <returns></returns>
        private static ulong CalculateTileId(int zoom, int x, int y)
        {
            ulong id = Tile.CalculateTileId(zoom);
            long width = (long)System.Math.Pow(2, zoom);
            return id + (ulong)x + (ulong)(y * width);
        }

        /// <summary>
        /// Calculate the tile given the id.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        private static Tile CalculateTile(ulong id)
        {
            // find out the zoom level first.
            int zoom = 0;
            if (id > 0)
            { // only if the id is at least at zoom level 1.
                while (id >= Tile.CalculateTileId(zoom))
                {
                    // move to the next zoom level and keep searching.
                    zoom++;
                }
                zoom--;
            }

            // calculate the x-y.
            ulong local = id - Tile.CalculateTileId(zoom);
            ulong width = (ulong)System.Math.Pow(2, zoom);
            int x = (int)(local % width);
            int y = (int)(local / width);

            return new Tile(x, y, zoom);
        }

        /// <summary>
        /// Returns the id of this tile.
        /// </summary>
        public ulong Id
        {
            get { return _id; }
        }

        /// <summary>
        /// Returns true if this tile is valid.
        /// </summary>
        public bool IsValid
        {
            get
            {
                if (this.X >= 0 &&
                    this.Y >= 0 &&
                    this.Zoom >= 0)
                { // some are negative.
                    var size = System.Math.Pow(2, this.Zoom);
                    return this.X < size && this.Y < size;
                }
                return false;
            }
        }

        /// <summary>
        /// Returns the tile at the given location at the given zoom.
        /// </summary>
        public static Tile CreateAroundLocation(double latitude, double longitude, int zoom)
        {
            var n = (int)System.Math.Floor(System.Math.Pow(2, zoom));

            var rad = latitude * System.Math.PI / 180.0;

            var x = (int)(((longitude + 180.0f) / 360.0f) * (double)n);
            var y = (int)(
                (1.0f - (System.Math.Log(System.Math.Tan(rad) + (1.0f / System.Math.Cos(rad))))
                / System.Math.PI) / 2f * (double)n);

            return new Tile(x, y, zoom);
        }
    }
}