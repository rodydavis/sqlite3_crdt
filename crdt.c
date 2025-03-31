#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1

#include <string.h> // Keep for potential future use, though not strictly needed by mprintf/exec
#include <stdlib.h> // Needed for sqlite3_free used indirectly by sqlite3_mprintf
#include <stdio.h>  // Keep for potential debugging printf statements if uncommented

#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

// Helper to execute SQL and handle errors, freeing the SQL string
static int execute_sql(sqlite3_context *context, sqlite3 *db, char *sql) {
    char *err_msg = NULL;
    int rc = SQLITE_OK;

    if (sql == NULL) {
        sqlite3_result_error_nomem(context);
        return SQLITE_NOMEM;
    }

    // printf("Executing SQL:\n%s\n", sql); // Uncomment for debugging

    rc = sqlite3_exec(db, sql, NULL, NULL, &err_msg);
    sqlite3_free(sql); // Free the SQL string regardless of success or failure

    if (rc != SQLITE_OK) {
        // Combine the specific SQLite error with a generic message if needed
        // Using mprintf here is safe as it uses sqlite3_malloc internally
        char *result_err = sqlite3_mprintf("SQL execution failed: %s", err_msg);
        sqlite3_result_error(context, result_err ? result_err : err_msg, -1);
        sqlite3_free(result_err); // Free the combined message if allocated
        sqlite3_free(err_msg);    // Free the original error from sqlite3_exec
        return rc;
    }

    sqlite3_result_int(context, 0); // Success
    return SQLITE_OK;
}


static void crdt_create(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc != 1) {
        sqlite3_result_error(context, "crdt_create requires 1 argument", -1);
        return;
    }
    const char *node_id = (const char *)sqlite3_value_text(argv[0]);

    if (node_id == NULL) {
        sqlite3_result_error(context, "node_id cannot be NULL", -1);
        return;
    }

    // Use %Q for SQL string literals - it handles NULL and escapes quotes.
    // Escape the literal SQL modulo operator % as %%
    char *sql = sqlite3_mprintf(
        "CREATE TABLE IF NOT EXISTS crdt_changes (\n"
        "    id TEXT NOT NULL PRIMARY KEY DEFAULT (hlc_now(%Q)),\n" // Use %Q for node_id literal
        "    pk TEXT NOT NULL,\n"
        "    tbl TEXT NOT NULL,\n"
        "    data BLOB,\n"
        "    path TEXT NOT NULL DEFAULT ('$'),\n"
        "    op TEXT NOT NULL DEFAULT ('='),\n"
        "    deleted BOOLEAN GENERATED ALWAYS AS (data IS NULL) VIRTUAL,\n"
        "    hlc TEXT NOT NULL,\n"
        "    json GENERATED ALWAYS AS (json_extract(data,'$')) VIRTUAL,\n"
        "    node_id TEXT NOT NULL GENERATED ALWAYS AS (hlc_node_id(hlc)) VIRTUAL\n"
        ");\n"
        "\n"
        "CREATE TABLE IF NOT EXISTS crdt_kv (\n"
        "    key TEXT NOT NULL PRIMARY KEY ON CONFLICT REPLACE,\n"
        "    value\n"
        ");\n"
        "\n"
        "CREATE TABLE IF NOT EXISTS crdt_records (\n"
        "    id TEXT NOT NULL PRIMARY KEY,\n"
        "    tbl TEXT NOT NULL,\n"
        "    data BLOB,\n"
        "    deleted BOOLEAN GENERATED ALWAYS AS (data IS NULL) VIRTUAL,\n"
        "    hlc TEXT NOT NULL,\n"
        "    path TEXT,\n"
        "    op TEXT,\n"
        "    json GENERATED ALWAYS AS (json_extract(data,'$')) VIRTUAL,\n"
        "    node_id TEXT NOT NULL GENERATED ALWAYS AS (hlc_node_id(hlc)) VIRTUAL\n"
        ");\n"
        "\n"
        "DROP TRIGGER IF EXISTS crdt_changes_trigger;\n"
        "CREATE TRIGGER crdt_changes_trigger\n"
        "AFTER INSERT ON crdt_changes\n"
        "BEGIN\n"
        "    INSERT INTO crdt_records (id, tbl, data, hlc, op, path)\n"
        "    VALUES (\n"
        "            NEW.pk,\n"
        "            NEW.tbl,\n"
        "            jsonb(NEW.data), \n"
        "            NEW.hlc,\n"
        "            IFNULL(NEW.op, '='),\n"
        "            IFNULL(NEW.path, '$')\n"
        "        ) ON CONFLICT (id) DO\n"
        "    UPDATE\n"
        "    SET data = (\n"
        "        CASE\n"
        "            WHEN NEW.deleted THEN NULL \n"
        "            WHEN NEW.op = 'set' THEN jsonb_set(data, NEW.path, jsonb(NEW.data))\n"
        "            WHEN NEW.op = 'insert' THEN jsonb_insert(data, NEW.path, jsonb(NEW.data))\n"
        "            WHEN NEW.op = 'patch' THEN jsonb_patch(data, jsonb(NEW.data))\n"
        "            WHEN NEW.op = 'remove' THEN jsonb_remove(data, NEW.path)\n"
        "            WHEN NEW.op = 'replace' THEN jsonb_replace(data, NEW.path, jsonb(NEW.data))\n"
        "            WHEN NEW.op = '=' THEN jsonb_set(data, NEW.path, jsonb(NEW.data))\n"
        "            WHEN NEW.op = '+' THEN jsonb_set(data, NEW.path, jsonb(json_extract(data, NEW.path) + json_extract(NEW.data, '$')))\n"
        "            WHEN NEW.op = '-' THEN jsonb_set(data, NEW.path, jsonb(json_extract(data, NEW.path) - json_extract(NEW.data, '$')))\n"
        "            WHEN NEW.op = '*' THEN jsonb_set(data, NEW.path, jsonb(json_extract(data, NEW.path) * json_extract(NEW.data, '$')))\n"
        "            WHEN NEW.op = '/' THEN jsonb_set(data, NEW.path, jsonb(json_extract(data, NEW.path) / json_extract(NEW.data, '$')))\n"
        "            WHEN NEW.op = '%%' THEN jsonb_set(data, NEW.path, jsonb(json_extract(data, NEW.path) %% json_extract(NEW.data, '$')))\n" // ESCAPED % -> %%
        "            WHEN NEW.op = '&' THEN jsonb_set(data, NEW.path, jsonb(json_extract(data, NEW.path) & json_extract(NEW.data, '$')))\n"
        "            WHEN NEW.op = '|' THEN jsonb_set(data, NEW.path, jsonb(json_extract(data, NEW.path) | json_extract(NEW.data, '$')))\n"
        "            WHEN NEW.op = '||' THEN jsonb_set(data, NEW.path, jsonb(json_extract(data, NEW.path) || json_extract(NEW.data, '$')))\n"
        "            ELSE data \n"
        "        END\n"
        "    ),\n"
        "    hlc = NEW.hlc,\n"
        "    path = IFNULL(NEW.path, '$'),\n"
        "    op = IFNULL(NEW.op, '=')\n"
        "    WHERE hlc_compare(NEW.hlc, crdt_records.hlc) > 0;\n"
        "END;\n",
        node_id // Argument for the %Q in hlc_now default
    );

    sqlite3 *db = sqlite3_context_db_handle(context);
    execute_sql(context, db, sql); // Use helper to execute and handle errors/freeing
}

static void crdt_create_table(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc != 2) {
        sqlite3_result_error(context, "crdt_create_table requires 2 arguments", -1);
        return;
    }
    // Use sqlite3_value_dup if you need the value beyond the callback scope,
    // but text is fine here as we use it immediately.
    const char *tbl = (const char *)sqlite3_value_text(argv[0]);
    const char *node_id = (const char *)sqlite3_value_text(argv[1]);

    if (tbl == NULL || node_id == NULL) {
        sqlite3_result_error(context, "tbl or node_id cannot be NULL", -1);
        return;
    }
    // Basic validation: Ensure table name doesn't contain quotes to avoid issues
    // with %w or manual quoting if used. A more robust check would disallow spaces, etc.
    if (strchr(tbl, '"') != NULL || strchr(tbl, '\'') != NULL) {
         sqlite3_result_error(context, "Table name cannot contain quotes", -1);
         return;
    }


    // Use sqlite3_mprintf for dynamic allocation.
    // Use %w for identifiers (table names, trigger names) - handles quoting if necessary.
    // Use %Q for SQL string literals (values inside quotes).
    char *sql = sqlite3_mprintf(
        // Drop existing view/triggers first for idempotency
        "DROP VIEW IF EXISTS %w;\n"
        "DROP TRIGGER IF EXISTS %w_insert;\n"
        "DROP TRIGGER IF EXISTS %w_update;\n"
        "DROP TRIGGER IF EXISTS %w_delete;\n"
        "\n"
        // Create View
        "CREATE VIEW %w AS\n"
        "SELECT\n"
        "  id,\n"
        "  data,\n"
        "  deleted,\n"
        "  hlc,\n"
        "  path,\n"
        "  op,\n"
        "  json,\n"
        "  node_id\n"
        "FROM crdt_records\n"
        "WHERE tbl = %Q\n" // %Q for table name literal in WHERE clause
        "AND deleted = 0;\n"
        "\n"
        // Insert Trigger
        "CREATE TRIGGER %w_insert INSTEAD OF\n" // %w for trigger name
        "INSERT ON %w BEGIN\n" // %w for view name
        "INSERT INTO crdt_changes (id, pk, tbl, data, op, path, hlc)\n"
        "VALUES (\n"
        "        hlc_now(uuid()), -- node_id was %Q\n" // Comment updated, value removed from args
        "        NEW.id,\n"
        "        %Q,\n" // %Q for table name literal
        "        jsonb(NEW.data),\n" // Assuming jsonb exists
        "        IFNULL(NEW.op, '='),\n"
        "        IFNULL(NEW.path, '$'),\n"
        "        IFNULL(NEW.hlc, hlc_now(%Q))\n" // %Q for node_id literal
        "    );\n"
        "END;\n"
        "\n"
        // Update Trigger
        "CREATE TRIGGER %w_update INSTEAD OF\n" // %w for trigger name
        "UPDATE ON %w BEGIN\n" // %w for view name
        "INSERT INTO crdt_changes (id, pk, tbl, data, op, path, hlc)\n"
        "VALUES (\n"
        "        hlc_now(uuid()), -- node_id was %Q\n" // Comment updated
        "        NEW.id,\n"
        "        %Q,\n" // %Q for table name literal
        "        jsonb(NEW.data),\n" // Assuming jsonb exists
        "        IFNULL(NEW.op, 'patch'),\n" // Default op for UPDATE is 'patch'
        "        IFNULL(NEW.path, '$'),\n"
        "        IFNULL(NEW.hlc, hlc_now(%Q))\n" // %Q for node_id literal
        "    );\n"
        "END;\n"
        "\n"
        // Delete Trigger
        "CREATE TRIGGER %w_delete INSTEAD OF DELETE ON %w BEGIN\n" // %w trigger, %w view
        "INSERT INTO crdt_changes (id, pk, tbl, data, op, path, hlc)\n"
        "VALUES (\n"
        "        hlc_now(uuid()), -- node_id was %Q\n" // Comment updated
        "        OLD.id,\n"
        "        %Q,\n" // %Q for table name literal
        "        NULL,\n" // Data is NULL for delete
        "        '=',\n"  // Op is '=' for delete (semantically replaces with NULL)
        "        '$',\n"  // Path is '$' for delete (affects whole object)
        "        hlc_now(%Q)\n" // %Q for node_id literal
        "    );\n"
        "END;\n",
        // Arguments for %w and %Q specifiers IN ORDER:
        tbl, tbl, tbl, tbl, // DROP statements (%w)
        tbl,               // CREATE VIEW %w
        tbl,               // WHERE tbl = %Q
        tbl,               // CREATE TRIGGER %w_insert
        tbl,               // INSERT ON %w
        node_id,           // comment node_id %Q (now just illustrative)
        tbl,               // VALUES tbl = %Q
        node_id,           // VALUES hlc_now(%Q)
        tbl,               // CREATE TRIGGER %w_update
        tbl,               // UPDATE ON %w
        node_id,           // comment node_id %Q (now just illustrative)
        tbl,               // VALUES tbl = %Q
        node_id,           // VALUES hlc_now(%Q)
        tbl,               // CREATE TRIGGER %w_delete
        tbl,               // DELETE ON %w
        node_id,           // comment node_id %Q (now just illustrative)
        tbl,               // VALUES tbl = %Q
        node_id            // VALUES hlc_now(%Q)
    );

    sqlite3 *db = sqlite3_context_db_handle(context);
    execute_sql(context, db, sql); // Use helper to execute and handle errors/freeing
}


static void crdt_remove_table(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc != 1) {
        sqlite3_result_error(context, "crdt_remove_table requires 1 argument", -1);
        return;
    }

    const char *tbl = (const char *)sqlite3_value_text(argv[0]);
    if (tbl == NULL) {
        sqlite3_result_error(context, "tbl cannot be NULL", -1);
        return;
    }
    // Basic validation
    if (strchr(tbl, '"') != NULL || strchr(tbl, '\'') != NULL) {
         sqlite3_result_error(context, "Table name cannot contain quotes", -1);
         return;
    }


    // Use %w for identifiers (view/trigger names)
    char *sql = sqlite3_mprintf(
        "DROP VIEW IF EXISTS %w;\n"
        "DROP TRIGGER IF EXISTS %w_insert;\n"
        "DROP TRIGGER IF EXISTS %w_update;\n"
        "DROP TRIGGER IF EXISTS %w_delete;\n"
        // Optionally, remove records associated with this table?
        // "DELETE FROM crdt_changes WHERE tbl = %Q;\n"
        // "DELETE FROM crdt_records WHERE tbl = %Q;\n"
        , tbl, tbl, tbl, tbl // Arguments for the 4 %w specifiers
        // If uncommenting DELETE lines, add: , tbl, tbl
    );

    sqlite3 *db = sqlite3_context_db_handle(context);
    execute_sql(context, db, sql); // Use helper
}

static void crdt_remove(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc != 0) {
        sqlite3_result_error(context, "crdt_remove requires 0 arguments", -1);
        return;
    }

    // No dynamic parts, but use mprintf for consistency and ease of modification
    // Or just use a static string literal directly with execute_sql if preferred
    char *sql = sqlite3_mprintf(
        "DROP TRIGGER IF EXISTS crdt_changes_trigger;\n" // Drop trigger before table
        "DROP TABLE IF EXISTS crdt_changes;\n"
        "DROP TABLE IF EXISTS crdt_kv;\n"
        "DROP TABLE IF EXISTS crdt_records;\n"
        // Note: This does NOT drop the individual table views/triggers created by crdt_create_table
        // A more complete removal might involve querying sqlite_master for related views/triggers.
    );

    sqlite3 *db = sqlite3_context_db_handle(context);
    execute_sql(context, db, sql); // Use helper
}

#ifdef _WIN32
DLLEXPORT // Macro already defines __declspec(dllexport)
#endif
int sqlite3_crdt_init(
  sqlite3 *db,
  char **pzErrMsg, // Assign error messages here if init fails
  const sqlite3_api_routines *pApi
){
    int rc = SQLITE_OK;
    // char *err_msg = NULL; // Not typically used here, pzErrMsg is for init errors
    SQLITE_EXTENSION_INIT2(pApi);

    // Check for required functions (optional but good practice)
    // Example: Check if json functions are available if needed by triggers
    // sqlite3_stmt *stmt = NULL;
    // rc = sqlite3_prepare_v2(db, "SELECT json('{}')", -1, &stmt, NULL);
    // if (rc != SQLITE_OK) {
    //     *pzErrMsg = sqlite3_mprintf("JSON1 extension not available or enabled.");
    //     return rc;
    // }
    // sqlite3_finalize(stmt);
    // Similarly check for hlc_now, hlc_compare, hlc_node_id, uuid if they are separate extensions

    rc = sqlite3_create_function(db, "crdt_create", 1, SQLITE_UTF8 | SQLITE_DIRECTONLY, NULL, crdt_create, NULL, NULL);
    if (rc != SQLITE_OK) {
         *pzErrMsg = sqlite3_mprintf("Failed to create function crdt_create: %s", sqlite3_errstr(rc));
         return rc;
    }

    rc = sqlite3_create_function(db, "crdt_create_table", 2, SQLITE_UTF8 | SQLITE_DIRECTONLY, NULL, crdt_create_table, NULL, NULL);
    if (rc != SQLITE_OK) {
         *pzErrMsg = sqlite3_mprintf("Failed to create function crdt_create_table: %s", sqlite3_errstr(rc));
         // Clean up previously registered function if needed
         sqlite3_create_function(db, "crdt_create", 1, SQLITE_UTF8 | SQLITE_DIRECTONLY, NULL, NULL, NULL, NULL);
         return rc;
    }

    rc = sqlite3_create_function(db, "crdt_remove_table", 1, SQLITE_UTF8 | SQLITE_DIRECTONLY, NULL, crdt_remove_table, NULL, NULL);
    if (rc != SQLITE_OK) {
         *pzErrMsg = sqlite3_mprintf("Failed to create function crdt_remove_table: %s", sqlite3_errstr(rc));
         // Clean up previously registered functions
         sqlite3_create_function(db, "crdt_create", 1, SQLITE_UTF8 | SQLITE_DIRECTONLY, NULL, NULL, NULL, NULL);
         sqlite3_create_function(db, "crdt_create_table", 2, SQLITE_UTF8 | SQLITE_DIRECTONLY, NULL, NULL, NULL, NULL);
         return rc;
    }

    rc = sqlite3_create_function(db, "crdt_remove", 0, SQLITE_UTF8 | SQLITE_DIRECTONLY, NULL, crdt_remove, NULL, NULL);
    if (rc != SQLITE_OK) {
         *pzErrMsg = sqlite3_mprintf("Failed to create function crdt_remove: %s", sqlite3_errstr(rc));
         // Clean up previously registered functions
         sqlite3_create_function(db, "crdt_create", 1, SQLITE_UTF8 | SQLITE_DIRECTONLY, NULL, NULL, NULL, NULL);
         sqlite3_create_function(db, "crdt_create_table", 2, SQLITE_UTF8 | SQLITE_DIRECTONLY, NULL, NULL, NULL, NULL);
         sqlite3_create_function(db, "crdt_remove_table", 1, SQLITE_UTF8 | SQLITE_DIRECTONLY, NULL, NULL, NULL, NULL);
         return rc;
    }

    // Add SQLITE_DIRECTONLY flag to prevent use in triggers/views if desired
    // Add SQLITE_INNOCUOUS flag if the functions don't read/write files or have side effects outside DB

    return SQLITE_OK; // Return OK on success
}

// Optional: Entry point for DLL cleanup if needed (usually not required for simple extensions)
// #ifdef _WIN32
// BOOL WINAPI DllMain(HINSTANCE hinstDLL, DWORD fdwReason, LPVOID lpReserved ) {
//     switch (fdwReason) {
//         case DLL_PROCESS_ATTACH: break;
//         case DLL_THREAD_ATTACH: break;
//         case DLL_THREAD_DETACH: break;
//         case DLL_PROCESS_DETACH: break;
//     }
//     return TRUE;
// }
// #endif