#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <rocksdb/c.h>
#include <janet.h>

/* logging */
#define DEBUG 0
#define LOG_PREFIX "janet-rocksdb: "
#define log(msg, ...) \
  do { if (DEBUG) {fprintf(stderr, LOG_PREFIX); fprintf(stderr, msg, ##__VA_ARGS__); } } while (0)

#define FLAG_CLOSED 1

typedef struct {
  rocksdb_t* handle;
  rocksdb_options_t *opts;
  rocksdb_readoptions_t *ropts;
  rocksdb_writeoptions_t *wopts;
  int flags;
} Db;

typedef struct {
  rocksdb_iterator_t* handle;
  Db* db;
  int flags;
} Iter;


static int janet_rocksdb_conn_get(void *p, Janet key, Janet *out);
static int janet_rocksdb_iter_get(void *p, Janet key, Janet *out);

/* Close an iterator */
static void janet_closeiter(Iter *iter) {
  if (!(iter->flags & FLAG_CLOSED)) {
    iter->flags |= FLAG_CLOSED;
    log("closing iterator\n");
    if (iter->db != NULL && !(iter->db->flags & FLAG_CLOSED)) {
      if (iter->handle != NULL) {
        rocksdb_iter_destroy(iter->handle);
      }
    }
  }
}

/* Close a db, noop if already closed */
static void janet_closedb(Db *db) {
  if (!(db->flags & FLAG_CLOSED)) {
    db->flags |= FLAG_CLOSED;
    log("closing db\n");
    rocksdb_close(db->handle);
    db->handle = NULL;
    if (db->ropts) {
      rocksdb_readoptions_destroy(db->ropts);
    }
    if (db->wopts) {
      rocksdb_writeoptions_destroy(db->wopts);
    }
    if (db->opts) {
      rocksdb_options_destroy(db->opts);
    }
  }
}

/* Garbage collection */
static int janet_rocksdb_gcdb(void *p, size_t s) {
  (void) s;
  Db *db = (Db *)p;
  janet_closedb(db);
  return 0;
}

static int janet_rocksdb_gciter(void *p, size_t s) {
  (void) s;
  Iter *iter = (Iter *)p;
  janet_closeiter(iter);
  return 0;
}                           

/* Janet Abstract types */
static const JanetAbstractType janet_rocksdb_conn_type = {
  "rocksdb.connection",
  janet_rocksdb_gcdb,
  NULL,
  janet_rocksdb_conn_get,
};

static const JanetAbstractType janet_rocksdb_iter_type = {
  "rocksdb.iterator",
  janet_rocksdb_gciter,
  NULL,
  janet_rocksdb_iter_get,
};

/* Open a database connection */
/* TODO set compression rocksdb_options_set_compression(opts, rocksdb_snappy_compression); */
static Janet janet_rocksdb_open(int32_t argc, Janet *argv) {
  janet_fixarity(argc, 1);
  int initial_capacity = 8;
  const uint8_t *db_name = janet_getstring(argv, 0);
  log("opening db: %s\n", db_name);
  rocksdb_options_t *opts = rocksdb_options_create();
  rocksdb_writeoptions_t *wopts = rocksdb_writeoptions_create();
  rocksdb_readoptions_t *ropts = rocksdb_readoptions_create();
  rocksdb_options_set_create_if_missing(opts, 1);
  rocksdb_options_set_error_if_exists(opts, 0);
  char *err = NULL;
  rocksdb_t *rconn = rocksdb_open(opts, (const char*) db_name, &err);
  if (err != NULL) {
    janet_panic(err);
  }
  free(err);
  Db *db = (Db *) janet_abstract(&janet_rocksdb_conn_type, sizeof(Db));
  db->handle = rconn;
  db->opts = opts;
  db->wopts = wopts;
  db->ropts = ropts;
  db->flags = 0;
  return janet_wrap_abstract(db);
}

/* Close a database connection */
static Janet janet_rocksdb_close(int32_t argc, Janet *argv) {
  janet_fixarity(argc, 1);
  Db *db = janet_getabstract(argv, 0, &janet_rocksdb_conn_type);
  janet_closedb(db);
  return janet_wrap_nil();
}

static Janet janet_rocksdb_iter_close(int32_t argc, Janet *argv) {
  janet_fixarity(argc, 1);
  Iter* iter = janet_getabstract(argv, 0, &janet_rocksdb_iter_type);
  janet_closeiter(iter);
  return janet_wrap_nil();
}

/* Destroy a database connection */
static Janet janet_rocksdb_destroy(int32_t argc, Janet *argv) {
  janet_fixarity(argc, 1);
  const uint8_t *db_name = janet_getstring(argv, 0);
  log("destroying db: %s\n", db_name);
  rocksdb_options_t *opts = rocksdb_options_create();
  char *err = NULL;
  rocksdb_destroy_db(opts, (char *) db_name, &err);
  if (err != NULL) {
    janet_panic(err);
  }
  rocksdb_options_destroy(opts);
  free(err);
  return janet_wrap_nil();
}

static Janet janet_rocksdb_delete(int32_t argc, Janet *argv) {
  janet_fixarity(argc, 2);
  Db *db = janet_getabstract(argv, 0, &janet_rocksdb_conn_type);
  const char *key = (char *) janet_getstring(argv, 1);
  char *err = NULL;

  rocksdb_delete(db->handle, db->wopts, key, strlen(key), &err);
  if (err != NULL) {
    janet_panic(err);
  }
  free(err);
  return janet_wrap_nil();
}

static Janet janet_rocksdb_put(int32_t argc, Janet *argv) {
  janet_fixarity(argc, 3);
  Db *db = janet_getabstract(argv, 0, &janet_rocksdb_conn_type);
  const char *key = (char *) janet_getstring(argv, 1);
  const char *value = (char *) janet_getstring(argv, 2);
  char *err = NULL;

  rocksdb_put(db->handle, db->wopts, key, strlen(key), value, strlen(value), &err);
  if (err != NULL) {
    janet_panic(err);
    free(err);
  }
  return janet_wrap_nil();
}

static Janet janet_rocksdb_get(int32_t argc, Janet *argv) {
  janet_fixarity(argc, 2);
  Db *db = janet_getabstract(argv, 0, &janet_rocksdb_conn_type);
  const char *key = (char *) janet_getstring(argv, 1);
  char *err = NULL;
  char *value = NULL;

  size_t rlen;
  value = rocksdb_get(db->handle, db->ropts, key, strlen(key), &rlen, &err);
  if (err != NULL) {
    janet_panic(err);
  }
  free(err);

  uint8_t *str = janet_string_begin(rlen);
  memcpy(str, value, rlen);
  free(value);
  return janet_wrap_string(janet_string_end(str));
}

static Janet janet_rocksdb_iter_seek_first(int32_t argc, Janet *argv) {
  janet_fixarity(argc, 1);
  Iter *iter = janet_getabstract(argv, 0, &janet_rocksdb_iter_type);
  rocksdb_iter_seek_to_first(iter->handle);
  return janet_wrap_nil();
}

static Janet janet_rocksdb_iter_seek_last(int32_t argc, Janet *argv) {
  janet_fixarity(argc, 1);
  Iter *iter = janet_getabstract(argv, 0, &janet_rocksdb_iter_type);
  rocksdb_iter_seek_to_last(iter->handle);
  return janet_wrap_nil();
}

static Janet janet_rocksdb_iter_seek(int32_t argc, Janet *argv) {
  janet_fixarity(argc, 2);
  Iter *iter = janet_getabstract(argv, 0, &janet_rocksdb_iter_type);
  const uint8_t *k = janet_getstring(argv, 1);
  size_t klen = strlen((char *) k);
  rocksdb_iter_seek(iter->handle, (char *) k, klen);
  return janet_wrap_nil();
}

static Janet janet_rocksdb_iter_next(int32_t argc, Janet *argv) {
  janet_fixarity(argc, 1);
  Iter *iter = janet_getabstract(argv, 0, &janet_rocksdb_iter_type);
  rocksdb_iter_next(iter->handle);
  return janet_wrap_nil();
}

static Janet janet_rocksdb_iter_key(int32_t argc, Janet *argv) {
  janet_fixarity(argc, 1);
  Iter *iter = janet_getabstract(argv, 0, &janet_rocksdb_iter_type);
  if (!rocksdb_iter_valid(iter->handle)) {
    janet_panic("invalid iterator");
  }
  size_t klen;
  const char* key = rocksdb_iter_key(iter->handle, &klen);
  uint8_t *str = janet_string_begin(klen);
  memcpy(str, key, klen);
  return janet_wrap_string(janet_string_end(str));
}

static Janet janet_rocksdb_iter_value(int32_t argc, Janet *argv) {
  janet_fixarity(argc, 1);
  Iter *iter = janet_getabstract(argv, 0, &janet_rocksdb_iter_type);
  if (!rocksdb_iter_valid(iter->handle)) {
    janet_panic("invalid iterator");
  }
  size_t vlen;
  const char* value = rocksdb_iter_value(iter->handle, &vlen);
  uint8_t *str = janet_string_begin(vlen);
  memcpy(str, value, vlen);
  return janet_wrap_string(janet_string_end(str));
}

static Janet janet_rocksdb_iter_create(int32_t argc, Janet *argv) {
  janet_fixarity(argc, 1);
  Db *db = janet_getabstract(argv, 0, &janet_rocksdb_conn_type);
  rocksdb_iterator_t *riter = rocksdb_create_iterator(db->handle, db->ropts);

  Iter *iter = (Iter *) janet_abstract(&janet_rocksdb_iter_type, sizeof(Iter));
  iter->handle = riter;
  iter->flags = 0;
  iter->db = db;

  return janet_wrap_abstract(iter);
}

static JanetMethod janet_rocksdb_conn_methods[] = {
  {"close", janet_rocksdb_close},
  {"get", janet_rocksdb_get},
  {"put", janet_rocksdb_put},
  {NULL, NULL}
};

static JanetMethod janet_rocksdb_iter_methods[] = {
  {"close", janet_rocksdb_iter_close},
  {NULL, NULL}
};

static int janet_rocksdb_conn_get(void *p, Janet key, Janet *out) {
  (void) p;
  if (!janet_checktype(key, JANET_KEYWORD)) {
    janet_panicf("expected keyword, got %v", key);
  }
  return janet_getmethod(janet_unwrap_keyword(key), janet_rocksdb_conn_methods, out);
}

static int janet_rocksdb_iter_get(void *p, Janet key, Janet *out) {
  (void) p;
  if (!janet_checktype(key, JANET_KEYWORD)) {
    janet_panicf("expected keyword, got %v", key);
  }
  return janet_getmethod(janet_unwrap_keyword(key), janet_rocksdb_iter_methods, out);
}


static JanetReg cfuns[] = {
  {"open", janet_rocksdb_open, "(rocksdb/open path)\n\nOpens or creates a rocksdb database. Returns a rocksdb connection."},
  {"close", janet_rocksdb_close, "(rocksdb/close db)\n\nCloses a rocksdb database"},
  {"put", janet_rocksdb_put, "(rocksdb/put db key value)\n\nPut a key/value pair"},
  {"get", janet_rocksdb_get, "(rocksdb/get db key value)\n\nGet a key/value pair"},
  {"delete", janet_rocksdb_delete, "(rocksdb/delete db key)\n\nDeletes a key/value pair"},
  {"destroy", janet_rocksdb_destroy, "(rocksdb/destroy path)\n\nDestroys a rocksdb database"},
  {"iter-create", janet_rocksdb_iter_create, "(rocksdb/iter-create db)\n\nCreates a new rocksdb iterator"},
  {"iter-seek", janet_rocksdb_iter_seek, "(rocksdb/iter-seek iter key)\n\nSeek iterator to key"},
  {"iter-seek-first", janet_rocksdb_iter_seek_first, "(rocksdb/iter-seek-first iter)\n\nSeek iterator to first key"},
  {"iter-seek-last", janet_rocksdb_iter_seek_last, "(rocksdb/iter-seek-last iter)\n\nSeek iterator to last key"},
  {"iter-value", janet_rocksdb_iter_value, "(rocksdb/iter-value iter value)\n\nIterate to value"},
  {"iter-key", janet_rocksdb_iter_key, "(rocksdb/iter-key iter key)\n\nIterate to key"},
  {"iter-next", janet_rocksdb_iter_next, "(rocksdb/iter-next iter)\n\nIterate to next key"},
  {"iter/close", janet_rocksdb_iter_close, "(rocksdb/iter-close iter)\n\nClose an iterator"},
  {NULL, NULL, NULL}
};

JANET_MODULE_ENTRY(JanetTable *env) {
  janet_cfuns(env, "rocksdb", cfuns);
}
