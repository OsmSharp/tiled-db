tiled-osm-db
============

Build a tile-osm-db where instances depend on eachother. The idea is to share as much data as possible between instances by splitting the world into tiles.

There are two types of _concepts_ that represent states of the OSM data:
1. An instance: This is a snapshot of OSM data that can contain multiple changesets.
  - Complete: A complete instance contains tiles for the entire world.
  - Diff: A diff instance contains only tiles that have been modified compare to it's _parent_. A _parent_ can be an state of the OSM data either represented by an _instance_ or a _changeset instance_.
2. A changeset instance : This is one changeset on top of an instance.

The entire structure is saved to disk as follows:

- {changesetid}: Complete instance, containing the state of OSM up until _changesetid_.
	- {changesetid + 1} : a changeset after time X, contains only the modified data relative to the complete instance.
	- {changesetid + 2} : a changeset after time X, contains only the modified data relative to the complete instance.
	- {changesetid + 3} : a changeset after time X, contains only the modified data relative to the complete instance.
		- {instanceid} : A diff instance based on {changesetid + 3}.

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

### Status:

- [DONE] A splitter: Splits an OSM-file into tiles as described above.
- [DONE] An indexer that builds the indexed based on the tiles.
- [TODO] A basic db implementation (see editing-api for reqs).
- [TODO] Implement a way to apply changesets coming from OSM.

##### Details:

- [x] Build a tile splitter, splits one file in a folder.
  - [x] Build a tile splitter that includes nodes, ways and relations with one stage.
  - [X] Build a tile splitter that includes relations with as many iterations as it takes.
- [x] Enhance OsmSharp to be able to reset a stream to an object type.
  - [ ] Enhance the OsmSharp binary format to be able to reset a stream to an object type.
- [ ] Build an indexer that takes a folder of tiles and builds:
  - [x] An index of nodes -> tile, ways -> tile and relations -> tile.
  - [X] Build the actual indexer indexing tile per tile.
  - [X] Build a merger to merge the indexes together.
- [ ] Build an OSM db implementation based on the index and the tiled folder.
- [ ] Build a way to apply changesets and generate diff indexes and tiles.
