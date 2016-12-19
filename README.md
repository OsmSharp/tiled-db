# tiled-osm-db

Try to complete a full implementation (implementing a full single db IHistoryDb is not possible).

We need:
- A node index [nodeid, tileid]
- A way index [wayid, tileid]
- A relation index [relationid, tileid]
- A changeset collection [changesetid, changeset]
- A collection of tiles.

Questions:

- How are tile structured?

Directory Structure:

- One directory per changeset '{changesetid}' and one based directory called 'base'
- 