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
    if (argc != 1) {
        sqlite3_result_error(context, "crdt_create requires 1 arguments", -1);
        return;
    }
    const char *node_id = (const char *)sqlite3_value_text(argv[0]);

    if (node_id == NULL) {
        sqlite3_result_error(context, "node_id cannot be NULL", -1);
        return;
    }

    char table_sql[] =
    "CREATE TABLE crdt_changes (\n"
    "    id TEXT NOT NULL PRIMARY KEY DEFAULT (hlc_now('%s')),\n"
    "    pk TEXT NOT NULL,\n"
    "    tbl TEXT NOT NULL,\n"
    "    data BLOB,\n"
    "    path TEXT NOT NULL DEFAULT ('$'),\n"
    "    op TEXT NOT NULL DEFAULT ('='),\n"
    "    deleted BOOLEAN AS (data IS NULL),\n"
    "    hlc TEXT NOT NULL,\n"
    "    json AS (data->'$'),\n"
    "    node_id TEXT NOT NULL AS (hlc_node_id(hlc))\n"
    ");\n"
    "\n"
    "CREATE TABLE crdt_kv (\n"
    "    key TEXT NOT NULL PRIMARY KEY ON CONFLICT REPLACE,\n"
    "    value\n"
    ");\n"
    "\n"
    "CREATE TABLE crdt_records (\n"
    "    id TEXT NOT NULL PRIMARY KEY,\n"
    "    tbl TEXT NOT NULL,\n"
    "    data BLOB,\n"
    "    deleted BOOLEAN AS (data IS NULL),\n"
    "    hlc TEXT NOT NULL,\n"
    "    path TEXT DEFAULT ('$'),\n"
    "    op TEXT DEFAULT ('='),\n"
    "    json AS (data->'$'),\n"
    "    node_id TEXT NOT NULL AS (hlc_node_id(hlc))\n"
    ");\n"
    "\n"
    "CREATE TRIGGER crdt_changes_trigger\n"
    "BEFORE INSERT ON crdt_changes\n"
    "BEGIN\n"
    "    INSERT INTO crdt_records (id, tbl, data, hlc, op, path)\n"
    "    VALUES (\n"
    "            NEW.pk,\n"
    "            NEW.tbl,\n"
    "            jsonb(NEW.data),\n"
    "            NEW.hlc,\n"
    "            IFNULL(NEW.op, '='),\n"
    "            IFNULL(NEW.path, '$')\n"
    "        ) ON CONFLICT (id) DO\n"
    "    UPDATE\n"
    "    SET data = (\n"
    "        CASE\n"
    "            WHEN NEW.deleted THEN NULL\n"
    "            WHEN excluded.op = 'set' THEN jsonb_set(\n"
    "                data,\n"
    "                NEW.path,\n"
    "                excluded.data\n"
    "            )\n"
    "            WHEN excluded.op = 'insert' THEN jsonb_insert(\n"
    "                data,\n"
    "                NEW.path,\n"
    "                excluded.data\n"
    "            )\n"
    "            WHEN excluded.op = 'patch' THEN jsonb_patch(\n"
    "                data,\n"
    "                excluded.data\n"
    "            )\n"
    "            WHEN excluded.op = 'remove' THEN jsonb_remove(\n"
    "                data,\n"
    "                NEW.path\n"
    "            )\n"
    "            WHEN excluded.op = 'replace' THEN jsonb_replace(\n"
    "                data,\n"
    "                NEW.path,\n"
    "                excluded.data\n"
    "            )\n"
    "            WHEN excluded.op = '=' THEN jsonb_set(\n"
    "                data,\n"
    "                NEW.path,\n"
    "                excluded.data\n"
    "            )\n"
    "            WHEN excluded.op = '+' THEN jsonb_set(\n"
    "                data,\n"
    "                NEW.path,\n"
    "                jsonb(\n"
    "                    jsonb_extract(data, NEW.path) + jsonb_extract(excluded.data, '$')\n"
    "                )\n"
    "            )\n"
    "            WHEN excluded.op = '-' THEN jsonb_set(\n"
    "                data,\n"
    "                NEW.path,\n"
    "                jsonb(\n"
    "                    jsonb_extract(data, NEW.path) - jsonb_extract(excluded.data, '$')\n"
    "                )\n"
    "            )\n"
    "            WHEN excluded.op = '*' THEN jsonb_set(\n"
    "                data,\n"
    "                NEW.path,\n"
    "                jsonb(\n"
    "                    jsonb_extract(data, NEW.path) * jsonb_extract(excluded.data, '$')\n"
    "                )\n"
    "            )\n"
    "            WHEN excluded.op = '/' THEN jsonb_set(\n"
    "                data,\n"
    "                NEW.path,\n"
    "                jsonb(\n"
    "                    jsonb_extract(data, NEW.path) / jsonb_extract(excluded.data, '$')\n"
    "                )\n"
    "            )\n"
    "            WHEN excluded.op = '%%' THEN jsonb_set(\n"
    "                data,\n"
    "                NEW.path,\n"
    "                jsonb(\n"
    "                    jsonb_extract(data, NEW.path) %% jsonb_extract(excluded.data, '$')\n"
    "                )\n"
    "            )\n"
    "            WHEN excluded.op = '&' THEN jsonb_set(\n"
    "                data,\n"
    "                NEW.path,\n"
    "                jsonb(\n"
    "                    jsonb_extract(data, NEW.path) & jsonb_extract(excluded.data, '$')\n"
    "                )\n"
    "            )\n"
    "            WHEN excluded.op = '|' THEN jsonb_set(\n"
    "                data,\n"
    "                NEW.path,\n"
    "                jsonb(\n"
    "                    jsonb_extract(data, NEW.path) | jsonb_extract(excluded.data, '$')\n"
    "                )\n"
    "            )\n"
    "            WHEN excluded.op = '||' THEN jsonb_set(\n"
    "                data,\n"
    "                NEW.path,\n"
    "                jsonb(\n"
    "                    jsonb_extract(data, NEW.path) || jsonb_extract(excluded.data, '$')\n"
    "                )\n"
    "            )\n"
    "        END\n"
    "    ),\n"
    "    hlc = NEW.hlc,\n"
    "    path = IFNULL(NEW.path, '$'),\n"
    "    op = IFNULL(NEW.op, '=')\n"
    "    WHERE hlc_compare(NEW.hlc, hlc) > 0;\n"
    "END;\n";

    char sql[sizeof(table_sql) + 512];
    
    snprintf(sql, sizeof(sql), table_sql, node_id);

    // printf("%s\n", sql);

    sqlite3 *db = sqlite3_context_db_handle(context);
    char *err_msg = NULL;

    if (sqlite3_exec(db, sql, NULL, NULL, &err_msg) != SQLITE_OK) {
        sqlite3_result_error(context, err_msg, -1);
        sqlite3_free(err_msg);
        return;
    }
    
    sqlite3_result_int(context, 0);
}

static void crdt_create_table(sqlite3_context *context, int argc, sqlite3_value **argv) {
    if (argc != 2) {
        sqlite3_result_error(context, "crdt_create_table requires 2 arguments", -1);
        return;
    }
    const char *tbl = (const char *)sqlite3_value_text(argv[0]);
    const char *node_id = (const char *)sqlite3_value_text(argv[1]);

    if (tbl == NULL || node_id == NULL) {
        sqlite3_result_error(context, "tbl or node_id cannot be NULL", -1);
        return;
    }

    char table_sql[] =
    "CREATE VIEW %s AS\n"
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
    "WHERE tbl = '%s'\n"
    "AND deleted = 0;\n"
    "\n"
    "CREATE TRIGGER %s_insert INSTEAD OF\n"
    "INSERT ON %s BEGIN\n"
    "INSERT INTO crdt_changes (id, pk, tbl, data, op, path, hlc)\n"
    "VALUES (\n"
    "        hlc_now(uuid()), -- '%s'\n"
    "        NEW.id,\n"
    "        '%s',\n"
    "        jsonb(NEW.data),\n"
    "        IFNULL(NEW.op, '='),\n"
    "        IFNULL(NEW.path, '$'),\n"
    "        IFNULL(NEW.hlc, hlc_now('%s'))\n"
    "    );\n"
    "END;\n"
    "\n"
    "CREATE TRIGGER %s_update INSTEAD OF\n"
    "UPDATE ON %s BEGIN\n"
    "INSERT INTO crdt_changes (id, pk, tbl, data, op, path, hlc)\n"
    "VALUES (\n"
    "        hlc_now(uuid()), -- '%s'\n"
    "        NEW.id,\n"
    "        '%s',\n"
    "        jsonb(NEW.data),\n"
    "        IFNULL(NEW.op, 'patch'),\n"
    "        IFNULL(NEW.path, '$'),\n"
    "        IFNULL(NEW.hlc, hlc_now('%s'))\n"
    "    );\n"
    "END;\n"
    "\n"
    "CREATE TRIGGER %s_delete INSTEAD OF DELETE ON %s BEGIN\n"
    "INSERT INTO crdt_changes (id, pk, tbl, data, op, path, hlc)\n"
    "VALUES (\n"
    "        hlc_now(uuid()), -- '%s'\n"
    "        OLD.id,\n"
    "        '%s',\n"
    "        NULL,\n"
    "        '=',\n"
    "        '$',\n"
    "        hlc_now('%s')\n"
    "    );\n"
    "END;\n";

    char sql[sizeof(table_sql) + 512];
    
    snprintf(sql, sizeof(sql), table_sql, tbl, tbl, tbl, tbl, node_id, tbl, node_id, tbl, tbl, node_id, tbl, node_id, tbl, tbl, node_id, tbl, node_id);

    // printf("%s\n", sql);

    sqlite3 *db = sqlite3_context_db_handle(context);
    char *err_msg = NULL;

    if (sqlite3_exec(db, sql, NULL, NULL, &err_msg) != SQLITE_OK) {
        sqlite3_result_error(context, err_msg, -1);
        sqlite3_free(err_msg);
        return;
    }
    
    sqlite3_result_int(context, 0);
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

    char table_sql[] =
    "DROP VIEW IF EXISTS %s;\n"
    "DROP TRIGGER IF EXISTS %s_insert;\n"
    "DROP TRIGGER IF EXISTS %s_update;\n"
    "DROP TRIGGER IF EXISTS %s_delete;\n";

    char sql[sizeof(table_sql) + 512];
    
    snprintf(sql, sizeof(sql), table_sql, tbl, tbl, tbl, tbl);

    // printf("%s\n", sql);

    sqlite3 *db = sqlite3_context_db_handle(context);
    char *err_msg = NULL;

    if (sqlite3_exec(db, sql, NULL, NULL, &err_msg) != SQLITE_OK) {
        sqlite3_result_error(context, err_msg, -1);
        sqlite3_free(err_msg);
        return;
    }
    
    sqlite3_result_int(context, 0);
}

static void crdt_remove(sqlite3_context *context, int argc, sqlite3_value **argv) { 
    if (argc != 0) {
        sqlite3_result_error(context, "crdt_remove requires 0 arguments", -1);
        return;
    }

    char sql[] =
    "DROP TABLE IF EXISTS crdt_changes;\n"
    "DROP TABLE IF EXISTS crdt_kv;\n"
    "DROP TABLE IF EXISTS crdt_records;\n"
    "DROP TRIGGER IF EXISTS crdt_changes_trigger;\n";

    // printf("%s\n", sql);

    sqlite3 *db = sqlite3_context_db_handle(context);
    char *err_msg = NULL;

    if (sqlite3_exec(db, sql, NULL, NULL, &err_msg) != SQLITE_OK) {
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

    rc = sqlite3_create_function(db, "crdt_create", 1, SQLITE_UTF8, NULL, crdt_create, NULL, NULL);
    if (rc != SQLITE_OK) return rc;
    
    rc = sqlite3_create_function(db, "crdt_create_table", 2, SQLITE_UTF8, NULL, crdt_create_table, NULL, NULL);
    if (rc != SQLITE_OK) return rc;

    rc = sqlite3_create_function(db, "crdt_remove_table", 1, SQLITE_UTF8, NULL, crdt_remove_table, NULL, NULL);
    if (rc != SQLITE_OK) return rc;

    rc = sqlite3_create_function(db, "crdt_remove", 0, SQLITE_UTF8, NULL, crdt_remove, NULL, NULL);
    if (rc != SQLITE_OK) return rc;

    return rc;
}