# CockroachDB Driver for Journey

Modeled after @twrobel3's CockroachDB driver for [mattes/migrate](https://github.com/mattes/migrate).

All db connection params are defined in the URL string, [see this to know what's supported](https://github.com/mattes/migrate/tree/2570d5866d87da6ab95d0fe9aac10cbd5641418d/database/cockroachdb).

Migration script names should take the form `<int_version>_<title>.up.sql` and `<int_version>_<title>.down.sql`. I recommend using a timestamp (seconds since epoch) for `<int_version>` values.

By default, each migration script will run within a transaction, but you can opt-out of this by including the comment `-- disable_ddl_transaction` as the first line in your script. Due to the [nature of CockroachDB](https://github.com/cockroachdb/cockroach/issues/14548), disabling transactions may be necessary for scripts that need to alter the schema *and* insert reference/seed data.

Note that [advisory locks are not supported in CockroachDB](https://github.com/cockroachdb/cockroach/issues/13546); therefore a manual lock (with default name `schema_lock`) is used. This driver will acquire the lock and release it at the beginning and end of every migration script.
