# tiled-osm-db

A tiled OSM database that supports:

- Access by ID: query nodes/ways/relations by their ID/version.
- Access by changeset: query changesets by their ID.
- Applying a changeset:
  - A changeset from OSM, used for synchronization.
  - A changeset from another source.
- Query by geographical area.

On top of that this database supports:

- Easy _branching_, it should be easy to:
  - Create a copy from a snapshot without actually copying anything.
  - Apply changesets to a _branch_.
  
## Design

The database is just a collection of files and folders. We define a few key principles:

- A _snapshot_: A snapshot contains all data.
- A _diff_: Contains only the changes on top of another view. 
- A _view_: Either a _diff_ or _snapshot_ that represents a fixed state in time.
- A _branch_: Can also be either a _snapshot_ or a _view_ but this is a database that contains data not from OSM.

The folder structure on disk should look like this:

- `initial`: A folder with the initial snapshot created a the time of creation of the db.
- `{timestamp}-snapshot`: A snapshot at the given containing all data at the timestamp. This snapshot does not need a pointer to any previous instance.
- `{timestamp}-diff`: A diff relative to the any previous snapshot. This references a previous stable snapshot and stores only the differences.

### Snapshot

The structure of a snapshot consists of three different file types:

- `{y}.{type}.idx`: Index files per tile indicating where objects are in the sub tiles. For example `/4/8/4.nodes.idx` contains data on what nodes is in what sub tile of tile `4/8/4`.
- `{y}.{type}.osm.bin{.zip}`: An actual tile with data. For example `14/7936/5555.ways.osm.bin.zip` contains all ways in the tile `14/7936/5555`.
- `{id}.{type}.osm.bin{.zip}`: A single object that doesn't belong in any tile. For example `/0/0/0/2323309.relation.osm.bin.zip` contains a single relation.

### Diff

The structure of a diff consists of three different file types, a bit different from snapshots because they need to be able to represents deletes.

- `{y}.{type}.idx.delete`: Index files per tile indicating where objects are in the sub tiles but with an additional bit indicating deletions. An object not in the index or an index that doesn't exist means that nothing has changed.
- `{y}.{type}.osm.bin{.zip}`: An actual tile with data. This is exactly the same format as in the snapshots but it contains only updated/new data.
- `{id}.{type}.osm.bin{.zip}`: A single object that doesn't belong in any tile. This is exactly the same format as in the snapshots but it contains only updated/new data.

### Applying changes

Changesets can be applied to any _view_, resulting in a new _diff_. 

## API

On top of the core database, we have designed an API to make it instantly usable for different scenarios.

We use the following structure:

- One default _branch_ that keeps up with OSM data.
- One or more branches that are create when applying non-OSM sourced changes. 

### Meta data & status

There are a few calls to get global information on the data.

`GET /status`  

Gets the status of the default fork. Returns a minimum the following data about the database:
 - The date of the latest change.
 - The zoom level of the tiling in the db.

`GET /view/{date}`  
`GET /{branch}/view/{date}`  

Gets the view that represents the state of the database at the given date/time. There are a few parameters:

- `{date}`: A data/time.
- `{branch}`: A branch, if any.

### Getting data

There are a few calls to get data:

##### By tile:

`GET /{view}/{zoom}/{x}/{y}`  
`GET /{branch}/{view}/{zoom}/{x}/{y}`  
`GET /{view}/{zoom}/{x}/{y}/complete`  
`GET /{branch}/{view}/{zoom}/{x}/{y}/complete`  

Gets all data in the given tile. There are a few parameters:

- `{view}`: The name of the view. `latest` is used as a shortcut to the latest version of the data.
- `{branch}`: Optionally a branch can be defined.
- `{zoom}`: The zoom level of the tile to get.
- `{x}/{y}`: The coordinates of the tile to get.
- `complete`: When complete is defined, all nodes in ways are returned even if outside of the tile.

### Updating data

Updating data is done via OSM changesets. A changeset can contain several delete/update/modify operations on one or more nodes/ways or relations.

`PUT /{view}`    
`PUT /{branch}/{view}`      

The body of this request needs to an XML OSM changeset. 

- `{view}`: The name of the view. The `{view}` should not exist yet.
- `{branch}`: Optionally a branch can be defined.