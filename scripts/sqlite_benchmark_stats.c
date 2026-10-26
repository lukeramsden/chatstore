#include <sqlite3ext.h>
/* Apple's SDK disables the extension macros; use the supplied API table. */
static const sqlite3_api_routines *benchmark_api;

static void db_status(sqlite3_context *context, int argc, sqlite3_value **argv) {
    (void)argc;
    int current = 0, highwater = 0;
    int result = benchmark_api->db_status(benchmark_api->context_db_handle(context),
                                          benchmark_api->value_int(argv[0]), &current, &highwater, 0);
    if (result != SQLITE_OK) {
        benchmark_api->result_error(context, "SQLite benchmark status unavailable", -1);
        return;
    }
    benchmark_api->result_int(context, current);
}

int sqlite3_extension_init(sqlite3 *db, char **error, const sqlite3_api_routines *api) {
    (void)error;
    benchmark_api = api;
    return benchmark_api->create_function(db, "chatstore_benchmark_db_status", 1,
                                          SQLITE_UTF8 | SQLITE_DIRECTONLY, 0, db_status, 0, 0);
}
