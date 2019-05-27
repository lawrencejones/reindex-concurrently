# reindex-concurrently

Until Postgres 12 lands this tool supports easy reindexing without taking strong
locks that would block reads and writes.

```
$ go run main.go --dbname gc_paysvc_live access_tokens_pkey
msg="connecting to database" dbname=gc_paysvc_live host=/tmp port=5432 user=postgres                                                                                                     
event=pg_setting setting=maintenance_work_mem value=1GB                                                                                                                                  
event=pg_setting setting=temp_file_limit value=100GB                                                                                                                                      
event=pg_setting setting=synchronous_commit value=on                                                                                                                                     
event=pg_setting setting=max_parallel_maintenance_workers value=4                                                                                                                        
msg="creating new index" index=access_tokens_pkey_working definition="CREATE UNIQUE INDEX CONCURRENTLY access_tokens_pkey_working ON public.access_tokens USING btree (id)"
msg="successfully created new index" index=access_tokens_pkey_working duration=88.907740356
event=pg_setting setting=lock_timeout value=5s
msg="beginning transaction"
msg="dropping foreign key constraint" constraint=events_source_access_token_fk query="alter table events drop constraint events_source_access_token_fk;"
msg="altering primary constraint to use new index" constraint=access_tokens_pkey query="alter table access_tokens drop constraint access_tokens_pkey, add constraint access_tokens_pkey PRIMARY KEY using index access_tokens_pkey_working;"
msg="re-creating deleted constraint" constraint=events_source_access_token_fk query="alter table events add constraint events_source_access_token_fk FOREIGN KEY (source_access_token_id) REFERENCES access_tokens(id) not valid;"
msg="commiting transaction"
constraint=events_source_access_token_fk msg="validating constraint" query="alter table events validate constraint events_source_access_token_fk;"
constraint=events_source_access_token_fk msg="validated constraint" duration=342.316521969
event=pg_setting_reset setting=lock_timeout
msg=done index=access_tokens_pkey old_size="22 GB" new_size="965 MB"
```

## What locks are taken?

- Creating working index takes no critical locks
- Dropping old index takes no critical locks
- Renaming new to old takes an access exclusive lock on parent table, bounded by
  the `--lock-timeout`
- Altering table to drop constraints takes an AccessExclusive lock but we do
  this within a transaction that has a strict `lock_timeout` applied
