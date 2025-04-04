# CRDT in SQLite

This repo contains a set of extensions meant to be used together with SQLite to implement CRDTs. The extensions are written in C and are loaded into SQLite as dynamic libraries.

- uuid.c (comes from official [SQLite3 source code](https://sqlite.org/src/file/ext/misc/uuid.c))
- hlc.c ([HLC implementation](https://cse.buffalo.edu/tech-reports/2014-04.pdf))
- crdt.c ([CRDT implementation](https://www.dotconferences.com/2019/12/james-long-crdts-for-mortals))

## Building

```bash
make all
```

## Loading

```bash
sqlite3
.load uuid
.load hlc
.load crdt
```

## Extensions

### UUID

Generates a new UUID.

```sql
SELECT uuid();
```

### HLC

Generates a new HLC timestamp.

```sql
SELECT hlc_now(uuid());
```

    The `uuid()` would be a static node_id you would generate once per client.

### CRDT

Initializes the CRDT tables.

```sql
SELECT crdt_create(uuid());
```

Create a new crdt table.

```sql
SELECT crdt_create_table('table_name', uuid());
```

    The `uuid()` would be a static node_id you would generate once per client.

The table that get created is a virtual table but has INSTEAD OF triggers to handle CRUD operations.

```sql
SELECT crdt_create_table('people', uuid());

INSERT INTO people (id, data, hlc)
VALUES ('1', '{"name": "Rody Davis"}', hlc_now('3afeb0e0-d9a6-424b-b60d-af86c06a4799'));

UPDATE people SET
    data = '{"name": "Rody"}',
    hlc = hlc_now('3afeb0e0-d9a6-424b-b60d-af86c06a4799')
WHERE id = '1';

SELECT * FROM people;

DELETE FROM people WHERE id = '1';
```

To delete the a table you need to call `crdt_remove_table`.

```sql
SELECT crdt_remove_table('people');
```

To delete the core crdt tables you need to call `crdt_remove`. You will need to call `crdt_remove_table` for each table before calling this.

```sql
SELECT crdt_remove();
```

To call the setup again you will need to call `crdt_create`. This is need to create the core tables again.

```sql
SELECT crdt_create(uuid());
```

    If the data is NULL it is considered a tombstone and will be removed from the CRDT.

#### Get CRDT Changes

```sql
SELECT * FROM crdt_changes
WHERE tbl = 'people';
```

### Overriding Operations

This supports the path operation for JSON objects in addition to a operator (defaults to '=').

```sql
-- Set the age to 30
UPDATE people SET
    data = '{"age": 30}',
    path = '$.age',
    hlc = hlc_now('3afeb0e0-d9a6-424b-b60d-af86c06a4799')
WHERE id = '1';

-- Add 1 to the age
UPDATE people SET
    data = '{"age": 30}',
    op = '+',
    path = '$.age',
    hlc = hlc_now('3afeb0e0-d9a6-424b-b60d-af86c06a4799')
WHERE id = '1'
```

The following operators are supported:
- `=` (assignment)
- `+` (addition)
- `-` (subtraction)
- `*` (multiplication)
- `/` (division)
- `%` (modulus)
- `&` (bitwise AND)
- `|` (bitwise OR)
- `||` (concatenation)
- `patch` ([json_patch](https://www.sqlite.org/json1.html#jpatch))
- `remove` ([json_remove](https://www.sqlite.org/json1.html#jrm))
- `replace` ([json_replace](https://www.sqlite.org/json1.html#jrepl))
- `set` ([json_set](https://www.sqlite.org/json1.html#jset))

For the path it needs to be a valid [JSON path](https://www.sqlite.org/json1.html) used in the functions.

    This extension uses jsonb to store the data in the CRDT which is a BLOB and is more efficient than storing as TEXT.
