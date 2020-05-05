# tiled-db

[![Build status](https://build.anyways.eu/app/rest/builds/buildType:(id:Osmsharp_TiledDb)/statusIcon)](https://build.anyways.eu/viewType.html?buildTypeId=Osmsharp_OsmBinary)   [![Visit our website](https://img.shields.io/badge/website-osmsharp.com-020031.svg) ](http://www.osmsharp.com/) [![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/OsmSharp/core/blob/develop/LICENSE.md)  

A tiled OSM database that supports:

- Optimized access :fire: by ID: query nodes/ways/relations by their ID.
- Optimized access :fire: by bbox/tile: query nodes/ways/relations by their location.
- Handles the entire :earth_africa: if that's your thing!

Next to that applying diffs from the OSM replication system is fast enough to catch up planet file. Minutely diffs can be applied within 1 second on a decent machine.

This was built within the [open planner team](https://github.com/openplannerteam) by [ANYWAYS](https://www.anyways.eu/) to provide the awesome [routeable tiles](https://github.com/openplannerteam/routable-tiles) with live updates.

# Usage

You can use the [replication console application](https://github.com/OsmSharp/tiled-db/tree/master/src/OsmSharp.Db.Tiled.Replication) to keep a local version up to date. 

Adding the database as a package to an existing .NET project is also possible:

    PM> Install-Package OsmSharp.Db.Tiled
    
## Building a new database

The most common usecase is creating a new database from a stream of OSM objects. The following code will create a new database from an OSM PBF file: 

```csharp
var dbPath = @"path/to/db-folder/";
var source = new PBFOsmStreamSource(@"path/to/file.osm.pbf");

db = OsmTiledHistoryDb.Create(dbPath, source);
```

## Updating a database

Keeping a local OSM database up to date can be done by using the [replication package](https://github.com/OsmSharp/replication).  

```csharp
// get a diff enumerator, already positioned for the given timestamp.
// null is returned when there is no diff available.
var hourEnumerator = await ReplicationConfig.Hourly.GetDiffEnumerator(db.Latest.EndTimestamp);
if (hourEnumerator != null)
{
    // move to the next diff.
    if (await hourEnumerator.MoveNext())
    {
        // download the diff.
        var diff = await hourEnumerator.Diff();

        // apply the diff.
        db.ApplyDiff(diff);
    }
}
```

# Design

The database is designed around a stream of OSM objects prefixed with the tile(s) they are found in. For nodes this is always one tile, for ways and relations this be multiple. The stream is one big linked list per tile id. 

The database consists of three data files:
- data.db : the _tiled stream_ of OSM objects.
- data.id.idx : pointers to the position of all OSM objects sorted by their id and type.
- data.tile.idx : pointers to the OSM object with the lowest id in each tile sorted by tile id.

Getting a tile is just a matter of looking up the pointer to the first object in the tile by using a binary search and then following the linked list. Getting multiple tiles at once can be done by getting all the start pointers and using a priority queue when following the linked list. 

Getting a single object is a binary search for its pointer and then returning the data. It's also possible to get the tiles for an object in the same way as each object is prefaced by its tiles.