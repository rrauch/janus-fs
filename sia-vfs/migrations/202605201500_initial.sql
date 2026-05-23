CREATE TABLE entity
(
    id              BLOB PRIMARY KEY NOT NULL CHECK (TYPEOF(id) = 'blob' AND
                                                     LENGTH(id) = 16),
    ref_count       INTEGER          NOT NULL DEFAULT 0 CHECK (ref_count >= 0),
    name            TEXT             NOT NULL CHECK (LENGTH(name) > 0 AND
                                                     LENGTH(name) <= 255 AND
                                                     LENGTH(TRIM(name)) = LENGTH(name) AND
                                                     name NOT LIKE '%/%'),
    created         TIMESTAMP        NOT NULL,
    last_modified   TIMESTAMP        NOT NULL,

    blob_id         BLOB REFERENCES blob (id),

    entity_type     TEXT             NOT NULL CHECK (entity_type IN ('R', 'D', 'F')),
    mode            TEXT             NOT NULL CHECK (mode IN ('S', 'L')),
    remote_location TEXT,
    data            BLOB,

    -- Make sure Files (and only Files) have a blob_id
    CHECK (
        (entity_type = 'F' AND blob_id IS NOT NULL) OR
        (entity_type != 'F' AND blob_id IS NULL)
        ),

    -- Make sure synced entities have a remote_location
    CHECK (
        (mode = 'S' AND remote_location IS NOT NULL) OR
        (mode = 'L' AND remote_location IS NULL)
        )
);

CREATE INDEX entity_blob_id_idx ON entity (blob_id);
CREATE INDEX entity_mode_idx ON entity (mode);

-- allows only mode transition or change of ref_count
CREATE TRIGGER entity_update_only_local_to_synced_or_refcount
    BEFORE UPDATE
    ON entity
    FOR EACH ROW
    WHEN OLD.mode IS NOT NEW.mode
        OR OLD.remote_location IS NOT NEW.remote_location
        OR OLD.data IS NOT NEW.data
        OR OLD.blob_id IS NOT NEW.blob_id
        OR OLD.id IS NOT NEW.id
        OR OLD.name IS NOT NEW.name
        OR OLD.created IS NOT NEW.created
        OR OLD.last_modified IS NOT NEW.last_modified
        OR OLD.entity_type IS NOT NEW.entity_type
BEGIN
    SELECT RAISE(ABORT, 'entity update: only L->S mode transition allowed')
    WHERE NOT (
        OLD.mode = 'L' AND NEW.mode = 'S'
            AND OLD.remote_location IS NULL
            AND NEW.remote_location IS NOT NULL
            AND OLD.id = NEW.id
            AND OLD.name = NEW.name
            AND OLD.created = NEW.created
            AND OLD.last_modified = NEW.last_modified
            AND OLD.entity_type = NEW.entity_type
            AND OLD.blob_id IS NEW.blob_id
            AND OLD.data IS NEW.data
        );
END;

-- Automatic GC of local entries
CREATE TRIGGER entity_gc_on_local_zero_refcount
    AFTER UPDATE OF ref_count
    ON entity
    FOR EACH ROW
    WHEN NEW.ref_count = 0
        AND NEW.mode = 'L'
BEGIN
    DELETE FROM entity WHERE id = NEW.id;
END;

-- blob ref_count
CREATE TRIGGER entity_insert_blob_refcount
    AFTER INSERT
    ON entity
    FOR EACH ROW
    WHEN NEW.blob_id IS NOT NULL
BEGIN
    UPDATE blob SET ref_count = ref_count + 1 WHERE id = NEW.blob_id;
END;

CREATE TRIGGER entity_delete_blob_refcount
    AFTER DELETE
    ON entity
    FOR EACH ROW
    WHEN OLD.blob_id IS NOT NULL
BEGIN
    UPDATE blob SET ref_count = ref_count - 1 WHERE id = OLD.blob_id;
END;


CREATE TABLE blob
(
    id              BLOB PRIMARY KEY NOT NULL CHECK (TYPEOF(id) = 'blob' AND
                                                     LENGTH(id) = 32),
    ref_count       INTEGER          NOT NULL DEFAULT 0 CHECK (ref_count >= 0),
    size            INTEGER          NOT NULL CHECK (size >= 0),
    mode            TEXT             NOT NULL CHECK (mode IN ('S', 'L')),
    remote_location TEXT,

    -- Make sure synced blobs have a remote_location
    CHECK (
        (mode = 'S' AND remote_location IS NOT NULL) OR
        (mode = 'L' AND remote_location IS NULL)
        )
);

CREATE INDEX blob_mode_idx ON blob (mode);

-- allows only mode transition or change of ref_count
CREATE TRIGGER blob_update_only_local_to_synced_or_refcount
    BEFORE UPDATE
    ON blob
    FOR EACH ROW
    WHEN OLD.mode IS NOT NEW.mode
        OR OLD.remote_location IS NOT NEW.remote_location
        OR OLD.id IS NOT NEW.id
        OR OLD.size IS NOT NEW.size
BEGIN
    SELECT RAISE(ABORT, 'blob update: only L->S mode transition allowed')
    WHERE NOT (
        OLD.mode = 'L' AND NEW.mode = 'S'
            AND OLD.id = NEW.id
            AND OLD.size = NEW.size
            AND OLD.remote_location IS NULL
            AND NEW.remote_location IS NOT NULL
        );
END;

-- Automatic GC of local entries
CREATE TRIGGER blob_gc_on_local_zero_refcount
    AFTER UPDATE OF ref_count
    ON blob
    FOR EACH ROW
    WHEN NEW.ref_count = 0
        AND NEW.mode = 'L'
BEGIN
    DELETE FROM blob WHERE id = NEW.id;
END;


CREATE TABLE chunk
(
    id              BLOB PRIMARY KEY NOT NULL CHECK (TYPEOF(id) = 'blob' AND
                                                     LENGTH(id) = 32),
    ref_count       INTEGER          NOT NULL DEFAULT 0 CHECK (ref_count >= 0),
    mode            TEXT             NOT NULL CHECK (mode IN ('S', 'L')),
    remote_location TEXT,
    data            BLOB,

    -- Make sure synced chunks have a remote_location while local ones have data
    CHECK (
        (mode = 'S' AND remote_location IS NOT NULL AND data IS NULL) OR
        (mode = 'L' AND remote_location IS NULL AND data IS NOT NULL)
        )
);

CREATE INDEX chunk_mode_idx ON chunk (mode);

-- allows only mode transition or change of ref_count
CREATE TRIGGER chunk_update_only_local_to_synced_or_refcount
    BEFORE UPDATE
    ON chunk
    FOR EACH ROW
    WHEN OLD.mode IS NOT NEW.mode
        OR OLD.remote_location IS NOT NEW.remote_location
        OR OLD.id IS NOT NEW.id
        OR OLD.data IS NOT NEW.data
BEGIN
    SELECT RAISE(ABORT, 'chunk update: only L->S mode transition allowed')
    WHERE NOT (
        OLD.mode = 'L' AND NEW.mode = 'S'
            AND OLD.id = NEW.id
            AND OLD.remote_location IS NULL
            AND NEW.remote_location IS NOT NULL
            AND OLD.data IS NOT NULL
            AND NEW.data IS NULL
        );
END;

-- Automatic GC of local entries
CREATE TRIGGER chunk_gc_on_local_zero_refcount
    AFTER UPDATE OF ref_count
    ON chunk
    FOR EACH ROW
    WHEN NEW.ref_count = 0
        AND NEW.mode = 'L'
BEGIN
    DELETE FROM chunk WHERE id = NEW.id;
END;


CREATE TABLE chunk_map
(
    blob_id      BLOB    NOT NULL REFERENCES blob (id) ON DELETE CASCADE,
    offset       INTEGER NOT NULL CHECK (offset >= 0),
    len          INTEGER NOT NULL CHECK (len >= 0),
    chunk_id     BLOB    NOT NULL REFERENCES chunk (id),
    chunk_offset INTEGER NOT NULL CHECK (chunk_offset >= 0),

    PRIMARY KEY (blob_id, offset)
);

CREATE INDEX chunk_map_chunk_id_idx ON chunk_map (chunk_id);

CREATE TRIGGER chunk_map_no_update
    BEFORE UPDATE
    ON chunk_map
    FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'chunk_map: updates are not allowed');
END;

-- chunk ref_count
CREATE TRIGGER chunk_map_insert_chunk_refcount
    AFTER INSERT
    ON chunk_map
    FOR EACH ROW
BEGIN
    UPDATE chunk SET ref_count = ref_count + 1 WHERE id = NEW.chunk_id;
END;

CREATE TRIGGER chunk_map_delete_chunk_refcount
    AFTER DELETE
    ON chunk_map
    FOR EACH ROW
BEGIN
    UPDATE chunk SET ref_count = ref_count - 1 WHERE id = OLD.chunk_id;
END;

CREATE TABLE vfs
(
    inode_id   INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL CHECK (inode_id >= 1000 OR inode_id = 1),
    inode_type TEXT                              NOT NULL CHECK (inode_type IN ('R', 'D', 'F')),
    entity_id  BLOB                              NOT NULL,
    name       TEXT                              NOT NULL CHECK (LENGTH(name) > 0 AND
                                                                 LENGTH(name) <= 255 AND
                                                                 LENGTH(TRIM(name)) = LENGTH(name) AND
                                                                 name NOT LIKE '%/%'),
    parent     INTEGER CHECK (parent IS NULL OR parent >= 1),
    path       TEXT,
    is_dirty   BOOLEAN                           NOT NULL DEFAULT 0,

    FOREIGN KEY (entity_id) REFERENCES entity (id),
    FOREIGN KEY (parent) REFERENCES vfs (inode_id) ON DELETE CASCADE,
    UNIQUE (parent, name)
);

CREATE INDEX vfs_path_idx ON vfs (path);

-- Ensure the vfs id sequence starts at 1000
INSERT INTO sqlite_sequence (name, seq)
VALUES ('vfs', 999);


-- Ensure VFS inode type matches entity type (on insert)
CREATE TRIGGER vfs_insert_type_match
    BEFORE INSERT
    ON vfs
    FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'vfs: inode_type must match entity_type')
    WHERE NEW.inode_type != (SELECT entity_type
                             FROM entity
                             WHERE id = NEW.entity_id);
END;

-- Ensure VFS inode type matches entity type (on update of entity reference)
CREATE TRIGGER vfs_update_type_match
    BEFORE UPDATE OF entity_id
    ON vfs
    FOR EACH ROW
    WHEN OLD.entity_id IS NOT NEW.entity_id
BEGIN
    SELECT RAISE(ABORT, 'vfs: inode_type must match entity_type')
    WHERE NEW.inode_type != (SELECT entity_type
                             FROM entity
                             WHERE id = NEW.entity_id);
END;

-- Ensure inode_id & inode_type cannot be changed
CREATE TRIGGER vfs_immutable_fields_update
    BEFORE UPDATE
    ON vfs
    FOR EACH ROW
    WHEN OLD.inode_id != NEW.inode_id OR OLD.inode_type != NEW.inode_type
BEGIN
    SELECT RAISE(ABORT, 'vfs: inode_id and inode_type cannot be changed');
END;

-- Ensure root inode_id is 1
CREATE TRIGGER vfs_root_inode_id_insert
    BEFORE INSERT
    ON vfs
    FOR EACH ROW
    WHEN (NEW.inode_type = 'R' AND NEW.inode_id != 1) OR (NEW.inode_type != 'R' AND NEW.inode_id = 1)
BEGIN
    SELECT RAISE(ABORT, 'vfs: root must have inode_id 1');
END;

-- Ensure root cannot be deleted
CREATE TRIGGER vfs_root_undeletable
    BEFORE DELETE
    ON vfs
    FOR EACH ROW
    WHEN OLD.inode_type = 'R'
BEGIN
    SELECT RAISE(ABORT, 'vfs: root cannot be deleted');
END;

-- Ensure only directories or root can be parents, and that only root has no parent (on insert)
CREATE TRIGGER vfs_parent_insert
    BEFORE INSERT
    ON vfs
    FOR EACH ROW
    WHEN (NEW.inode_type = 'R' AND NEW.parent IS NOT NULL)
        OR (NEW.inode_type != 'R' AND NEW.parent IS NULL)
        OR (NEW.inode_type != 'R' AND NEW.parent IS NOT NULL AND
            (SELECT inode_type
             FROM vfs
             WHERE inode_id = NEW.parent) NOT IN ('R', 'D'))
BEGIN
    SELECT RAISE(ABORT, 'vfs: invalid parent: root must have NULL parent, others must have a parent of type R or D');
END;

-- Ensure only directories or root can be parents, and that only root has no parent (on update)
CREATE TRIGGER vfs_parent_update
    BEFORE UPDATE
    ON vfs
    FOR EACH ROW
    WHEN (NEW.inode_type = 'R' AND NEW.parent IS NOT NULL)
        OR (NEW.inode_type != 'R' AND NEW.parent IS NULL)
        OR (NEW.inode_type != 'R' AND NEW.parent IS NOT NULL AND
            (SELECT inode_type
             FROM vfs
             WHERE inode_id = NEW.parent) NOT IN ('R', 'D'))
BEGIN
    SELECT RAISE(ABORT, 'vfs: invalid parent: root must have NULL parent, others must have a parent of type R or D');
END;

-- Prevent an inode from becoming its own parent (on insert)
CREATE TRIGGER vfs_self_parent_insert
    BEFORE INSERT
    ON vfs
    FOR EACH ROW
    WHEN NEW.parent = NEW.inode_id
BEGIN
    SELECT RAISE(ABORT, 'vfs: inode cannot be its own parent');
END;

-- Prevent an inode from becoming its own parent (on update)
CREATE TRIGGER vfs_self_parent_update
    BEFORE UPDATE
    ON vfs
    FOR EACH ROW
    WHEN NEW.parent = NEW.inode_id
BEGIN
    SELECT RAISE(ABORT, 'vfs: inode cannot be its own parent');
END;

-- Make sure an inode cannot become its own grandparent

-- SQLite does not currently support CTEs within triggers, so this view is
-- created to recursively resolve all ancestors of an inode
CREATE VIEW vfs_inode_ancestors AS
WITH RECURSIVE vfs_ancestor_path(inode_id, ancestor) AS (SELECT inode_id, parent
                                                         FROM vfs
                                                         WHERE parent IS NOT NULL
                                                         UNION ALL
                                                         SELECT o.inode_id, a.ancestor
                                                         FROM vfs o
                                                                  JOIN vfs_ancestor_path a ON o.parent = a.inode_id)
SELECT inode_id, ancestor
FROM vfs_ancestor_path;

-- Prevent loops in the parent-child relationships
CREATE TRIGGER vfs_prevent_loops_on_update
    BEFORE UPDATE
    ON vfs
    FOR EACH ROW
    WHEN NEW.parent IS NOT NULL
        AND (OLD.parent IS NULL OR NEW.parent != OLD.parent)
        AND NEW.parent != NEW.inode_id
BEGIN
    SELECT RAISE(ABORT, 'Loop detected in hierarchy - don''t be your own grandparent!')
    WHERE EXISTS (SELECT 1
                  FROM vfs_inode_ancestors
                  WHERE ancestor = NEW.inode_id
                    AND inode_id = NEW.parent);
END;

-- Mark the inodes parent as dirty on insert
CREATE TRIGGER vfs_mark_inode_parent_on_insert
    AFTER INSERT
    ON vfs
    FOR EACH ROW
BEGIN
    UPDATE vfs
    SET is_dirty = 1
    WHERE inode_id = NEW.parent
      AND inode_id != NEW.inode_id;
END;

-- Mark the inodes parent as dirty on delete
CREATE TRIGGER vfs_mark_inode_parent_on_delete
    AFTER DELETE
    ON vfs
    FOR EACH ROW
BEGIN
    UPDATE vfs
    SET is_dirty = 1
    WHERE inode_id = OLD.parent;
END;

-- Mark inode as dirty if something significant changes
CREATE TRIGGER vfs_mark_inode_parent_dirty_on_update
    AFTER UPDATE OF name, parent, entity_id
    ON vfs
    FOR EACH ROW
    WHEN OLD.name != NEW.name
        OR OLD.parent IS NULL AND NEW.parent IS NOT NULL
        OR OLD.parent IS NOT NULL AND NEW.parent IS NULL
        OR OLD.parent != NEW.parent
        OR OLD.entity_id != NEW.entity_id
BEGIN
    UPDATE vfs
    SET is_dirty = 1
    WHERE (inode_id = NEW.parent OR inode_id = OLD.parent)
      AND inode_id != NEW.inode_id;
END;

-- Recursively mark all dirty inodes
CREATE TRIGGER vfs_mark_dirty_inodes_on_update_recursive
    AFTER UPDATE OF is_dirty
    ON vfs
    FOR EACH ROW
    WHEN NEW.is_dirty = 1
BEGIN
    UPDATE vfs
    SET is_dirty = 1
    WHERE (inode_id = NEW.parent OR inode_id = OLD.parent)
      AND inode_id != NEW.inode_id;
END;

-- Mark the inode path for recalculation on insert
CREATE TRIGGER vfs_clear_inode_path_on_insert
    AFTER INSERT
    ON vfs
    FOR EACH ROW
BEGIN
    UPDATE vfs
    SET path = NULL
    WHERE inode_id = NEW.inode_id;
END;

-- An inode's path is always updated automatically.

-- Mark the inode path for recalculation if name or parent changes
CREATE TRIGGER vfs_update_inode_path_on_update
    AFTER UPDATE OF name, parent
    ON vfs
    FOR EACH ROW
    WHEN OLD.name != NEW.name
        OR OLD.parent IS NULL AND NEW.parent IS NOT NULL
        OR OLD.parent IS NOT NULL AND NEW.parent IS NULL
        OR OLD.parent != NEW.parent
BEGIN
    UPDATE vfs
    SET path = NULL
    WHERE inode_id = NEW.inode_id;
END;

-- Recursively update the paths of marked objects
CREATE TRIGGER vfs_update_inode_path_on_update_recursive
    AFTER UPDATE OF path
    ON vfs
    FOR EACH ROW
    WHEN NEW.path IS NULL
BEGIN
    UPDATE vfs
    SET path = CASE
                   WHEN NEW.inode_type = 'R' THEN '/'
                   WHEN (SELECT inode_type FROM vfs WHERE inode_id = NEW.parent) = 'R' THEN '/' || NEW.name
                   ELSE (SELECT path FROM vfs WHERE inode_id = NEW.parent) || '/' || NEW.name
        END
    WHERE inode_id = NEW.inode_id;

    UPDATE vfs
    SET path = NULL
    WHERE parent = NEW.inode_id
      AND inode_id != NEW.inode_id;
END;

-- Increment entity ref_count when a VFS inode is inserted
CREATE TRIGGER vfs_insert_entity_refcount
    AFTER INSERT
    ON vfs
    FOR EACH ROW
BEGIN
    UPDATE entity
    SET ref_count = ref_count + 1
    WHERE id = NEW.entity_id;
END;

-- Decrement entity ref_count when a VFS inode is deleted
CREATE TRIGGER vfs_delete_entity_refcount
    AFTER DELETE
    ON vfs
    FOR EACH ROW
BEGIN
    UPDATE entity
    SET ref_count = ref_count - 1
    WHERE id = OLD.entity_id;
END;

-- Handle re-pointing a VFS inode to a different entity
CREATE TRIGGER vfs_update_entity_refcount
    AFTER UPDATE OF entity_id
    ON vfs
    FOR EACH ROW
    WHEN OLD.entity_id IS NOT NEW.entity_id
BEGIN
    UPDATE entity
    SET ref_count = ref_count + 1
    WHERE id = NEW.entity_id;

    UPDATE entity
    SET ref_count = ref_count - 1
    WHERE id = OLD.entity_id;
END;

CREATE TABLE temp_file_handle
(
    id       INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL CHECK (id >= 0),
    inode_id INTEGER REFERENCES vfs (inode_id) -- NULL in case of new file
);

CREATE TRIGGER temp_file_handle_no_updates
    BEFORE UPDATE
    ON temp_file_handle
    FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'temp_file_handle: updating rows is not allowed');
END;

CREATE TABLE temp_file_chunks
(
    file_handle INTEGER NOT NULL REFERENCES temp_file_handle (id) ON DELETE CASCADE,
    chunk_id    BLOB    NOT NULL REFERENCES chunk (id),

    PRIMARY KEY (file_handle, chunk_id)
);

CREATE TRIGGER temp_file_chunks_no_updates
    BEFORE UPDATE
    ON temp_file_chunks
    FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'temp_file_chunks: updating rows is not allowed');
END;

-- Increment chunk ref_count when temp_file_chunks is inserted
CREATE TRIGGER temp_file_chunks_insert_refcount
    AFTER INSERT
    ON temp_file_chunks
    FOR EACH ROW
BEGIN
    UPDATE chunk
    SET ref_count = ref_count + 1
    WHERE id = NEW.chunk_id;
END;

-- Decrement chunk ref_count when temp_file_chunks is deleted
CREATE TRIGGER temp_file_chunks_delete_refcount
    AFTER DELETE
    ON temp_file_chunks
    FOR EACH ROW
BEGIN
    UPDATE chunk
    SET ref_count = ref_count - 1
    WHERE id = OLD.chunk_id;
END;

CREATE TABLE sync_job
(
    created         TIMESTAMP NOT NULL,
    root_entity_id  BLOB      NOT NULL,
    synced_blobs    INTEGER   NOT NULL DEFAULT 0 CHECK (synced_blobs >= 0),
    synced_chunks   INTEGER   NOT NULL DEFAULT 0 CHECK (synced_chunks >= 0),
    synced_entities INTEGER   NOT NULL DEFAULT 0 CHECK (synced_entities >= 0),
    uploaded_data   INTEGER   NOT NULL DEFAULT 0 CHECK (uploaded_data >= 0),
    num_uploads     INTEGER   NOT NULL DEFAULT 0 CHECK (num_uploads >= 0),

    FOREIGN KEY (root_entity_id) REFERENCES entity (id)
);

CREATE TRIGGER single_sync_job_only
    BEFORE INSERT
    ON sync_job
    FOR EACH ROW
    WHEN (SELECT COUNT(*)
          FROM sync_job) > 0
BEGIN
    SELECT RAISE(ABORT, 'sync_job: only a single row allowed at a time');
END;

CREATE TRIGGER sync_job_root_entity_type_insert
    BEFORE INSERT
    ON sync_job
    FOR EACH ROW
    WHEN (SELECT entity_type
          FROM entity
          WHERE id = NEW.root_entity_id) != 'R'
BEGIN
    SELECT RAISE(ABORT, 'sync_job root must reference an entity of type R');
END;

CREATE TRIGGER sync_job_immutable
    BEFORE UPDATE
    ON sync_job
    FOR EACH ROW
    WHEN OLD.created <> NEW.created
        OR OLD.root_entity_id <> NEW.root_entity_id
BEGIN
    SELECT RAISE(ABORT, 'sync_job: created, root_entity_id and root_revision are immutable');
END;


-- Ensure there aren't any dangling sync job queue rows
CREATE TRIGGER sync_job_gc
    AFTER DELETE
    ON sync_job
    FOR EACH ROW
    WHEN (SELECT COUNT(*)
          FROM sync_job) = 0
BEGIN
    DELETE FROM sync_job_queue;
END;

-- Increment entity ref_count when sync_job is inserted
CREATE TRIGGER sync_job_insert_refcount
    AFTER INSERT
    ON sync_job
    FOR EACH ROW
BEGIN
    UPDATE entity
    SET ref_count = ref_count + 1
    WHERE id = NEW.root_entity_id;
END;

-- Decrement entity ref_count when sync_job is deleted
CREATE TRIGGER sync_job_delete_refcount
    AFTER DELETE
    ON sync_job
    FOR EACH ROW
BEGIN
    UPDATE entity
    SET ref_count = ref_count - 1
    WHERE id = OLD.root_entity_id;
END;

CREATE TABLE sync_job_queue
(
    id             INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    type           TEXT                              NOT NULL CHECK (type IN ('B', 'C', 'E')), -- Blob, Chunk or Entity
    blob_id        BLOB REFERENCES blob (id),
    chunk_id       BLOB REFERENCES chunk (id),
    entity_id      BLOB,
    estimated_size INTEGER                           NOT NULL CHECK (estimated_size > 0),

    FOREIGN KEY (entity_id) REFERENCES entity (id),

    CHECK (
        (type = 'B' AND blob_id IS NOT NULL AND chunk_id IS NULL AND entity_id IS NULL) OR
        (type = 'C' AND blob_id IS NULL AND chunk_id IS NOT NULL AND entity_id IS NULL) OR
        (type = 'E' AND blob_id IS NULL AND chunk_id IS NULL AND entity_id IS NOT NULL)
        )
);

CREATE TRIGGER sync_job_queue_no_updates
    BEFORE UPDATE
    ON sync_job_queue
    FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'sync_job_queue: updating rows is not allowed');
END;


-- Increment blob ref_count when sync_job_queue row is inserted
CREATE TRIGGER sync_job_queue_blob_insert_refcount
    AFTER INSERT
    ON sync_job_queue
    FOR EACH ROW
    WHEN NEW.blob_id IS NOT NULL
BEGIN
    UPDATE blob
    SET ref_count = ref_count + 1
    WHERE id = NEW.blob_id;
END;

-- Decrement blob ref_count when sync_job_queue row is deleted
CREATE TRIGGER sync_job_queue_blob_delete_refcount
    AFTER DELETE
    ON sync_job_queue
    FOR EACH ROW
    WHEN OLD.blob_id IS NOT NULL
BEGIN
    UPDATE blob
    SET ref_count = ref_count - 1
    WHERE id = OLD.blob_id;
END;

-- Increment chunk ref_count when sync_job_queue row is inserted
CREATE TRIGGER sync_job_queue_chunk_insert_refcount
    AFTER INSERT
    ON sync_job_queue
    FOR EACH ROW
    WHEN NEW.chunk_id IS NOT NULL
BEGIN
    UPDATE chunk
    SET ref_count = ref_count + 1
    WHERE id = NEW.chunk_id;
END;

-- Decrement chunk ref_count when sync_job_queue row is deleted
CREATE TRIGGER sync_job_queue_chunk_delete_refcount
    AFTER DELETE
    ON sync_job_queue
    FOR EACH ROW
    WHEN OLD.chunk_id IS NOT NULL
BEGIN
    UPDATE chunk
    SET ref_count = ref_count - 1
    WHERE id = OLD.chunk_id;
END;

-- Increment entity ref_count when sync_job_queue row is inserted
CREATE TRIGGER sync_job_queue_entity_insert_refcount
    AFTER INSERT
    ON sync_job_queue
    FOR EACH ROW
    WHEN NEW.entity_id IS NOT NULL
BEGIN
    UPDATE entity
    SET ref_count = ref_count + 1
    WHERE id = NEW.entity_id;
END;

-- Decrement entity ref_count when sync_job_queue row is deleted
CREATE TRIGGER sync_job_queue_entity_delete_refcount
    AFTER DELETE
    ON sync_job_queue
    FOR EACH ROW
    WHEN OLD.entity_id IS NOT NULL
BEGIN
    UPDATE entity
    SET ref_count = ref_count - 1
    WHERE id = OLD.entity_id;
END;
