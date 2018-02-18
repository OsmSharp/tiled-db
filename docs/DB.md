Database Design
---------------

The tiled-osm-db has the following priorities:

- Consistency: Applying a changeset applies everyting or nothing, unless specified otherwise.
- Read performance: 
	- Reading OSM-data in a tiled format or boundingbox should be fast!
	- Reading individual Nodes, Ways or Relations in a similar geographical area should be fast.
- Forkable:
  - It should be trivial to fork a db.
  - A db with only changes should be possible.
    - A reference db with all the data is defined.
	- Only the changeset are described by the database.
	
# Lifetime

1. Creation: A database is create either empty or from an OSM file.
2. Active: From now on all ID's are determined by the DB and only CRUD operations are possible.
  
# Data Structure

## Full Database

This describes the datastructure for a full database containing all data.

- Hierarchial:
  - Zoom '0': has only indexes.
  - Zoom '0' + n: has indexes per tile.
  - Zoom '0' + 2n: has index per tile containing exact subsets of their parent tiles.
  - Zoom 'highest': 
	- has the full objects for all objects in these tiles.
  
What do the indexes look like:
- Index: [id][mask]
 with 
  - id: the id of the object sorted.
  - mask: point to subtiles
	- 4 bits for 4 subtiles.
	- 16 bits for 16 subtiles.
	- ...

(OPTIONAL) What about duplicate data?

Some objects will appear in multiple tiles, it may be needed to represent them globally or at a higher level. To be tested if this is needed.

# Methods and relation to data structure

## Common Methods

### Find tile(s) for Node, Way or Relation

Name: FIND TILES FOR (NODE | WAY | RELATION)

Read indexes and masks, travelling down, until 'highest' zoom, keep all tile id's and return them.

### Find Node, Way or Relation

Name: FIND (NODE | WAY | RELATION)

1. FIND TILES FOR (NODE | WAY | RELATION).
2. GET (NODE | WAY | RELATION) FROM A TILE.
  
### Find Tile by location

Name: FIND TILE BY LOCATION

Travel down the tile tree until 'highest' zoom, return that tile id.

# CRUD Scenario's

## CRUD for NODES
  
### Create Node

- FIND TILE BY LOCATION:
 - NOT FOUND:
  - CREATE TILE.
 - ADD NODE TO TILE.
 - UPDATE INDEXES.

### Read NODE

- FIND NODE:
 - NO: nothing to return.
 - YES: return Node.

### Update Node

- FIND NODE:
 - NO: Can't update, error.
 - YES: 
   - UPDATE 
     - IF NODE CHANGES TILE:
	   - REMOVE FROM OLD TILE.
	   - ADD NODE TO NEW TILE.
	   - UPDATE PARENT WAYS INDEX.
	   - UPDATE PARENT RELATIONS INDEX.

### Delete Node

(validation is done before, no ways or relations with node)

- FIND NODE:
 - NO: Can' delete, error.
 - YES:
	- DELETE FROM TILE
	- (OPTIONAL) UPDATE INDEXES
 
## CRUD for Ways

### Create Way

- FIND TILES FOR ALL NODES.
- FOR EACH TILE:
	- ADD WAY TO TILES
- UPDATE WAY INDEX WITH TILES

### Read Way

- FIND WAY:
 - NO: nothing to return.
 - YES: return Way.
 
### Update WAY

- FIND TILES FOR WAY ID
  - FIND TILES FOR NEW NODES LIST.
	- IF TILES NOT SAME:
	  - UPDATE INDEXES.
	  - REMOVE WHERE APPROPRIATE.
	- UPDATE WHERE APPROPRIATE.
	- UPDATE PARENT RELATIONS INDEX.
	
## CRUD for Relations

(Identical as for ways)
