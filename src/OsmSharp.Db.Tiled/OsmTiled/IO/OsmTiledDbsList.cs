namespace OsmSharp.Db.Tiled.OsmTiled.IO
{
    internal class OsmTiledDbsList
    {
        public OsmTiledDbsList(OsmTiledDbBase db, OsmTiledDbsList? smallerDb)
        {
            this.Db = db;
            this.SmallerDb = smallerDb;
        }
        
        public OsmTiledDbBase Db { get; }
        
        public OsmTiledDbsList? SmallerDb { get; private set; }

        public OsmTiledDbsList Add(OsmTiledDbBase db)
        {
            switch (this.Db)
            {
                case OsmTiledDbSnapshot _:
                    switch (db)
                    {
                        case OsmTiledDb _:
                        case OsmTiledDbSnapshot _ when db.Timespan > this.Db.Timespan:
                            return new OsmTiledDbsList(db, this);
                    }

                    break;
                case OsmTiledDbDiff _:
                    switch (db)
                    {
                        case OsmTiledDb _:
                        case OsmTiledDbSnapshot _:
                        case OsmTiledDbDiff _ when db.Timespan > this.Db.Timespan:
                            return new OsmTiledDbsList(db, this);
                    }

                    break;
            }

            this.SmallerDb = this.SmallerDb == null ? new OsmTiledDbsList(db, this.SmallerDb) : this.SmallerDb.Add(db);
            return this;
        }

        public OsmTiledDbBase Smallest()
        {
            if (this.SmallerDb != null)
            {
                return this.SmallerDb.Smallest();
            }

            return this.Db;
        }
    }
}