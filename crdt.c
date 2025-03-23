#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#ifdef _WIN32
#define DLLEXPORT __declspec(dllexport)
#else
#define DLLEXPORT
#endif

static void crdt_create(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc != 2) {
        sqlite3_result_error(context, "crdt_create requires 2 arguments", -1);
        return;
    }
    const char *table_name = (const char *)sqlite3_value_text(argv[0]);
    const char *node_id = (const char *)sqlite3_value_text(argv[1]);

    if (table_name == NULL || node_id == NULL) {
        sqlite3_result_error(context, "table_name or node_id cannot be NULL", -1);
        return;
    }

    char view_sql[512];
    snprintf(view_sql, sizeof(view_sql), "CREATE VIEW %s AS SELECT *, '=' AS op, '$' AS path, json_extract(data, '$') AS json FROM crdt_records WHERE table_name = '%s' AND deleted = FALSE;", table_name, table_name);

    char trigger_insert_sql[2048];
    snprintf(trigger_insert_sql, sizeof(trigger_insert_sql),
        "CREATE TRIGGER IF NOT EXISTS %s_insert_trigger INSTEAD OF INSERT ON %s FOR EACH ROW BEGIN "
        "INSERT INTO crdt_changes (pk, table_name, data, op, path, hlc) VALUES (NEW.id, '%s', jsonb(NEW.data), IFNULL(NEW.op, '='), IFNULL(NEW.path, '$'), hlc_now('%s')); END;",
        table_name, table_name, table_name, node_id);

    char trigger_update_sql[2048];
    snprintf(trigger_update_sql, sizeof(trigger_update_sql),
        "CREATE TRIGGER IF NOT EXISTS %s_update_trigger INSTEAD OF UPDATE ON %s FOR EACH ROW BEGIN "
        "INSERT INTO crdt_changes (pk, table_name, data, op, path, hlc) VALUES (NEW.id, '%s', jsonb(NEW.data), IFNULL(NEW.op, 'patch'), IFNULL(NEW.path, '$'), hlc_now('%s')); END;",
        table_name, table_name, table_name, node_id);

    char trigger_delete_sql[2048];
    snprintf(trigger_delete_sql, sizeof(trigger_delete_sql),
        "CREATE TRIGGER IF NOT EXISTS %s_delete_trigger INSTEAD OF DELETE ON %s FOR EACH ROW BEGIN "
        "INSERT INTO crdt_changes (pk, table_name, data, op, path, hlc) VALUES (OLD.id, '%s', NULL, '=', '$', hlc_now('%s')); END;",
        table_name, table_name, table_name, node_id);

    sqlite3 *db = sqlite3_context_db_handle(context);
    char *err_msg = NULL;

    if (sqlite3_exec(db, view_sql, NULL, NULL, &err_msg) != SQLITE_OK) {
        sqlite3_result_error(context, err_msg, -1);
        sqlite3_free(err_msg);
        return;
    }

    if (sqlite3_exec(db, trigger_insert_sql, NULL, NULL, &err_msg) != SQLITE_OK) {
        sqlite3_result_error(context, err_msg, -1);
        sqlite3_free(err_msg);
        return;
    }
    if (sqlite3_exec(db, trigger_update_sql, NULL, NULL, &err_msg) != SQLITE_OK) {
        sqlite3_result_error(context, err_msg, -1);
        sqlite3_free(err_msg);
        return;
    }
    if (sqlite3_exec(db, trigger_delete_sql, NULL, NULL, &err_msg) != SQLITE_OK) {
        sqlite3_result_error(context, err_msg, -1);
        sqlite3_free(err_msg);
        return;
    }

    sqlite3_result_int(context, 0);
}

static void crdt_remove(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc != 1) {
        sqlite3_result_error(context, "crdt_remove requires 1 argument", -1);
        return;
    }
    const char *table_name = (const char *)sqlite3_value_text(argv[0]);
    if (table_name == NULL) {
        sqlite3_result_error(context, "table_name name cannot be NULL", -1);
        return;
    }

    char view_sql[512];
    snprintf(view_sql, sizeof(view_sql), "DROP VIEW IF EXISTS %s;", table_name);

    char trigger_insert_sql[512];
    snprintf(trigger_insert_sql, sizeof(trigger_insert_sql), "DROP TRIGGER IF EXISTS %s_insert_trigger;", table_name);

    char trigger_update_sql[512];
    snprintf(trigger_update_sql, sizeof(trigger_update_sql), "DROP TRIGGER IF EXISTS %s_update_trigger;", table_name);

    char trigger_delete_sql[512];
    snprintf(trigger_delete_sql, sizeof(trigger_delete_sql), "DROP TRIGGER IF EXISTS %s_delete_trigger;", table_name);

    sqlite3 *db = sqlite3_context_db_handle(context);
    char *err_msg = NULL;

    if (sqlite3_exec(db, view_sql, NULL, NULL, &err_msg) != SQLITE_OK) {
        sqlite3_result_error(context, err_msg, -1);
        sqlite3_free(err_msg);
        return;
    }

    if (sqlite3_exec(db, trigger_insert_sql, NULL, NULL, &err_msg) != SQLITE_OK) {
        sqlite3_result_error(context, err_msg, -1);
        sqlite3_free(err_msg);
        return;
    }

    if (sqlite3_exec(db, trigger_update_sql, NULL, NULL, &err_msg) != SQLITE_OK) {
        sqlite3_result_error(context, err_msg, -1);
        sqlite3_free(err_msg);
        return;
    }

    if (sqlite3_exec(db, trigger_delete_sql, NULL, NULL, &err_msg) != SQLITE_OK) {
        sqlite3_result_error(context, err_msg, -1);
        sqlite3_free(err_msg);
        return;
    }

    sqlite3_result_int(context, 0);
}

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_crdt_init(
  sqlite3 *db,
  char **pzErrMsg,
  const sqlite3_api_routines *pApi
){
    int rc = SQLITE_OK;
    char *err_msg = NULL;
    SQLITE_EXTENSION_INIT2(pApi);

    rc = sqlite3_create_function(db, "crdt_create", 2, SQLITE_UTF8, NULL, crdt_create, NULL, NULL);
    if (rc != SQLITE_OK) return rc;

    rc = sqlite3_create_function(db, "crdt_remove", 1, SQLITE_UTF8, NULL, crdt_remove, NULL, NULL);
    if (rc != SQLITE_OK) return rc;

    char table_sql[] = "CREATE TABLE IF NOT EXISTS crdt_records (id NOT NULL DEFAULT (hlc_now(uuid())), table_name TEXT NOT NULL, data BLOB, deleted BOOLEAN AS (data IS NULL), hlc TEXT NOT NULL, node_id TEXT NOT NULL AS (hlc_node_id(hlc)), PRIMARY KEY (id, table_name)); CREATE TABLE IF NOT EXISTS crdt_changes (id NOT NULL PRIMARY KEY DEFAULT (hlc_now(uuid())), pk TEXT NOT NULL, table_name TEXT NOT NULL, data BLOB, path TEXT NOT NULL DEFAULT ('$'), op TEXT NOT NULL DEFAULT ('='), deleted BOOLEAN AS (data IS NULL), hlc TEXT NOT NULL, node_id TEXT NOT NULL AS (hlc_node_id(hlc))); CREATE TRIGGER IF NOT EXISTS crdt_changes_after_insert AFTER INSERT ON crdt_changes FOR EACH ROW BEGIN INSERT INTO crdt_records (id, table_name, data, hlc) VALUES (NEW.pk, NEW.table_name, jsonb(NEW.data), NEW.hlc) ON CONFLICT (id, table_name) DO UPDATE SET data = (CASE WHEN NEW.deleted THEN NULL WHEN NEW.op = 'set' THEN jsonb_set(data, NEW.path, excluded.data) WHEN NEW.op = 'insert' THEN jsonb_insert(data, NEW.path, excluded.data) WHEN NEW.op = 'patch' THEN jsonb_patch(data, excluded.data) WHEN NEW.op = 'remove' THEN jsonb_remove(data, NEW.path) WHEN NEW.op = 'replace' THEN jsonb_replace(data, NEW.path, excluded.data) WHEN NEW.op = '=' THEN jsonb_set(data, NEW.path, excluded.data) WHEN NEW.op = '+' THEN jsonb_set(data, NEW.path, jsonb(jsonb_extract(data, NEW.path) + jsonb_extract(excluded.data, '$'))) WHEN NEW.op = '-' THEN jsonb_set(data, NEW.path, jsonb(jsonb_extract(data, NEW.path) - jsonb_extract(excluded.data, '$'))) WHEN NEW.op = '*' THEN jsonb_set(data, NEW.path, jsonb(jsonb_extract(data, NEW.path) * jsonb_extract(excluded.data, '$'))) WHEN NEW.op = '/' THEN jsonb_set(data, NEW.path, jsonb(jsonb_extract(data, NEW.path) / jsonb_extract(excluded.data, '$'))) WHEN NEW.op = '%' THEN jsonb_set(data, NEW.path, jsonb(jsonb_extract(data, NEW.path) % jsonb_extract(excluded.data, '$'))) WHEN NEW.op = '&' THEN jsonb_set(data, NEW.path, jsonb(jsonb_extract(data, NEW.path) & jsonb_extract(excluded.data, '$'))) WHEN NEW.op = '|' THEN jsonb_set(data, NEW.path, jsonb(jsonb_extract(data, NEW.path) | jsonb_extract(excluded.data, '$'))) WHEN NEW.op = '||' THEN jsonb_set(data, NEW.path, jsonb(jsonb_extract(data, NEW.path) || jsonb_extract(excluded.data, '$'))) END), hlc = NEW.hlc WHERE id = NEW.pk AND table_name = NEW.table_name AND hlc_compare(NEW.hlc, hlc) > 0; END;";

     if (sqlite3_exec(db, table_sql, NULL, NULL, &err_msg) != SQLITE_OK) {
        if (pzErrMsg) {
            *pzErrMsg = sqlite3_mprintf("%s", err_msg); // Copy the error message.
        }
        sqlite3_free(err_msg); // Free the error message allocated by sqlite3_exec.
        return SQLITE_ERROR;
    }

    return rc;
}