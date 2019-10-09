

### Setup ###

The test setup includes two Postgres databases with [sportsdb](https://www.thesportsdb.com/) schema and data, and two GraphQL engines running on the Postgres databases. Then one of the GraphQL engines is added as a remote schema to another GraphQL engine.

The data will be same in both the databases. But the tables will reside in different database schema in-order to avoid GraphQL schema conflicts.

The Python script `test_with_sportsdb.py` will help in setting up the databases, starting the Hasura GraphQL engines, and setting up relationships (both local and remote). This script will run databases on docker, and the GraphQL engines are run with `stack exec`.

#### Setup GraphQL Engines ####

Inorder to start GraphQL engines with sportsdb, run
```sh
python3 test_with_sportsdb.py
```

This will setup Postgres databases and runs the main and remote GraphQL servers
Pressing enter will teardown both Postgres database

The initial setup will take some time. The subsequent ones will be faster. The Postgres data is bind mounted from a volume in the host, which will be reused.

### Benchmarking ###

We may employ https://github.com/hasura/graphql-bench to do the benchmarks.

Create the `bench.yaml` file
```
- name: events_remote_affilications
  warmup_duration: 60
  duration: 300
  candidates:
  - name: hge-with-remote
    url: http://127.0.0.1:8081/v1/graphql
    query: events_remote_affiliations
    queries_file: queries.graphql
  rps:
  - 20
  - 40
```

To run the benchmark, do
```sh
cat bench.yaml | docker run -i --rm -p 8050:8050 -v $(pwd)/queries.graphql:/graphql-bench/ws/queries.graphql hasura/graphql-bench:v0.3
```
