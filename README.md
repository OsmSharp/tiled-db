tiled-osm-db
============

Build a tile-osm-db where instances depend on eachother.

- 0000: Base instance at time X.
  - 0000-0001: changeset 0001 on top of 0000.
  - 0000-0002: changeset 0002 on top of 0000.
  - 0000-0003: changeset 0003 on top of 0000.
- 0001: Base instance at time X+1.
  - 0001-0001: changeset 0001 on top of 0001.
  - 0001-0002: changeset 0002 on top of 0001.
  - 0001-0003: changeset 0003 on top of 0001.
  
The goal of this database is to make version 0000 share most of it's on-disk data with 0000-XXXX. Same for the differences between 0000 and 0001.

A database version is basically represented by a base version, '0000' for example, and a series of changesets on top of that version. The database can always be reconstructed using these two pieces of data.

### Directory structure

- 0000: The timestamp of the base database.
 - 0000-{guid1}: A custom version based on edits on top of '0000'.
 - 0000-{guid2}: A custom version based on edits on top of '0000'.
- 1111: The timestamp of the database that has changed with changes from OSM since 0000.
 - 1111-{guid1}: A custom version based on edits on top of '1111'.
 - 1111-{guid2}: A custom version based on edits on top of '1111'.

In each directory there are tiles. Each tile contains:

- Nodes within the tile.
- Ways that have at least one node within the tile.
- Relations that have at least one member withing the tile.

There is an index containing:

- Matching nodeids -> tileids: one-to-one.
- Matching wayids -> tileids: one-to-many.
- Matching relationids -> tileids: one-to-many.

(optional) for storage efficiency we could store objects that are present in more than x tiles in a seperate index.

### Differences

Basically only differences are stored compared to the version the current version is based on except for performance purposes:

- Only the changed tiles are store. 
- Only the additions to the index are stored.
- (optional) Only the new objects are stored in the seperate indexes.

TODO:

- [x] Build a tile splitter, splits one file in a folder.
  - [x] Build a tile splitter that includes nodes, ways and relations with one stage.
  - [ ] Build a tile splitter that includes relations with as many iterations as it takes.
- [x] Enhance OsmSharp to be able to reset a stream to an object type.
