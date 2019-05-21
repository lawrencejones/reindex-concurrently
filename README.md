# reindex-concurrently

Until Postgres 12 lands, we don't have the ability to do non-blocking reindexes.
This tool allows quick and easy reindexing without taking long-lived exclusive
locks.

```
$ go run main.go --dbname payments-development --host localhost index_payment_actions_on_payment_id_and_created_at
msg="connecting to database" dbname=payments-development host=localhost port=5432 user=postgres
msg="setting maintenance work mem" maintenance_work_mem=1GB
msg="creating new index" index=index_payment_actions_on_payment_id_and_created_at_working definition="CREATE INDEX index_payment_actions_on_payment_id_and_created_at_working ON public.payment_actions USING btree (payment_id, created_at DESC)"
msg="successfully created new index" index=index_payment_actions_on_payment_id_and_created_at_working duration=0.002816122
msg="dropping old index" index=index_payment_actions_on_payment_id_and_created_at
msg="setting lock timeout" lock_timeout=250ms
msg="rename new index to old" from=index_payment_actions_on_payment_id_and_created_at_working to=index_payment_actions_on_payment_id_and_created_at
msg=done index=index_payment_actions_on_payment_id_and_created_at old_size="8192 bytes" new_size="8192 bytes"
```

## What locks are taken?

- Creating working index takes no critical locks
- Dropping old index takes no critical locks
- Renaming new to old takes an access exclusive lock on parent table, bounded by
  the `--lock-timeout`
