using Anyways.Osm.TiledDb.Collections;
using OsmSharp.Db;
using System;
using System.Collections.Generic;
using OsmSharp;
using OsmSharp.Changesets;
using System.IO;

namespace Anyways.Osm.TiledDb
{
    public class TileDb : IHistoryDb
    {
        private readonly int _zoomLevel;
        private readonly IdMap _nodeMap;
        private readonly IdMap _wayMap;
        private readonly IdMap _relationMap;
        private readonly DirectoryInfo _baseDirectory;

        public TileDb(DirectoryInfo baseDirectory)
        {
            _nodeMap = new IdMap();
            _wayMap = new IdMap();
            _relationMap = new IdMap();

            _baseDirectory = baseDirectory;
        }

        public void Clear()
        {
            throw new NotSupportedException();
        }

        public void Add(IEnumerable<OsmGeo> osmGeos)
        {
            throw new NotSupportedException();
        }

        public void Add(Changeset meta, OsmChange changes)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<OsmGeo> Get()
        {
            throw new NotImplementedException();
        }

        public IEnumerable<OsmGeo> Get(IEnumerable<OsmGeoKey> keys)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<OsmGeo> Get(IEnumerable<OsmGeoVersionKey> keys)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<OsmGeo> Get(float minLatitude, float minLongitude, float maxLatitude, float maxLongitude)
        {
            throw new NotImplementedException();
        }

        public long OpenChangeset(Changeset info)
        {
            throw new NotImplementedException();
        }

        public DiffResultResult ApplyChangeset(long id, OsmChange changeset)
        {
            throw new NotImplementedException();
        }

        public void UpdateChangesetInfo(Changeset info)
        {
            throw new NotImplementedException();
        }

        public bool CloseChangeset(long id)
        {
            throw new NotImplementedException();
        }

        public Changeset GetChangeset(long id)
        {
            throw new NotImplementedException();
        }

        public OsmChange GetChanges(long id)
        {
            throw new NotImplementedException();
        }

        public OsmGeo Get(OsmGeoType type, long id)
        {
            throw new NotImplementedException();
        }
    }
}