import os
import subprocess
import requests
import inflection
import json
import time
import signal
from requests.exceptions import ConnectionError
from colorama import Fore, Style

class HGEError(Exception):
    pass


class HGE:

    default_graphql_env = {
        'HASURA_GRAPHQL_ENABLE_TELEMETRY': 'false',
        'EVENT_WEBHOOK_HEADER': "MyEnvValue",
        'HASURA_GRAPHQL_STRINGIFY_NUMERIC_TYPES': 'true',
        'HASURA_GRAPHQL_CONSOLE_ASSETS_DIR' :  '../../../../console/static/dist/',
        'HASURA_GRAPHQL_ENABLE_CONSOLE' : 'true'
    }

    def __init__(self, pg, port_allocator, admin_secret, log_file='hge.log'):
        self.pg = pg
        self.log_file = log_file
        self.admin_secret = admin_secret
        self.introspection = None
        self.obj_fk_rels = set()
        self.arr_fk_rels = set()
        self.port_allocator = port_allocator
        self.url = None

    def run_with_stack(self):
        self.port = self.port_allocator.allocate_port(8080)
        self.tix_file = self.log_file[:-4] + '.tix'
        hge_env = {
            **os.environ,
            **self.default_graphql_env.copy(),
            'HASURA_GRAPHQL_DATABASE_URL': self.pg.url,
            'HASURA_GRAPHQL_SERVER_PORT': str(self.port),
            'HPCTIXFILE' : self.tix_file
        }
        if self.admin_secret:
            hge_env['HASURA_GRAPHQL_ADMIN_SECRET'] = self.admin_secret

        process_args = ['stack', 'exec', 'graphql-engine', '--', 'serve']
        self.log_fp = open(self.log_file, 'w')
        self.proc = subprocess.Popen(
            process_args,
            env=hge_env,
            shell=False,
            bufsize=-1,
            stdout=self.log_fp,
            stderr=subprocess.STDOUT
        )
        self.url = 'http://localhost:' + str(self.port)
        print("Waiting for GraphQL Engine to be running.", end='')
        self.wait_for_start()

    def wait_for_start(self, timeout=60):
        if timeout <= 0:
            raise HGEError("Timeout waiting for graphql process to start")
        if self.proc.poll() is not None:
            with open(self.log_file) as fr:
                raise HGEError(
                        "GraphQL engine failed with error: " + fr.read())
        try:
            q = { 'query': 'query { __typename }' }
            r = requests.post(self.url + '/v1/graphql',json.dumps(q),headers=self.admin_auth_headers())
            if r.status_code == 200:
                print()
                return
        except ConnectionError:
            pass
        print(".", end="", flush=True),
        sleep_time = 0.5
        time.sleep(sleep_time)
        self.wait_for_start(timeout - sleep_time)

    def teardown(self):
        if getattr(self, 'log_fp', None):
            self.log_fp.close()
            self.log_fp = None
        if getattr(self, 'proc', None):
            print(Fore.YELLOW + "Stopping graphql engine at port:", self.port, Style.RESET_ALL)
            self.proc.send_signal(signal.SIGINT)
            self.proc.wait()
            self.proc = None

    def admin_auth_headers(self):
        headers = {}
        if self.admin_secret:
            headers['X-Hasura-Admin-Secret'] = self.admin_secret
        return headers

    def v1q(self, q, exp_status=200):
        resp = requests.post(self.url + '/v1/query', json.dumps(q), headers=self.admin_auth_headers())
        assert resp.status_code == exp_status, (resp.status_code, resp.json())
        return resp.json()

    def graphql_admin_q(self, q, exp_status = 200):
        resp = requests.post(self.url + '/v1/graphql', q, headers=self.admin_auth_headers())
        assert resp.status_code == exp_status, (resp.status_code, resp.json())
        return resp.json()

    def track_all_tables_in_schema(self, schema='public'):
        print("Track all tables in schema ", schema)
        all_tables = self.pg.get_all_tables_in_a_schema(schema)
        all_tables = [ {'schema': schema, 'name': t}
                       for t in all_tables ]
        return self.track_tables(all_tables)

    def run_bulk(self, queries, exp_status = 200):
        bulk_q = {
            'type': 'bulk',
            'args': queries
        }
        return self.v1q(bulk_q, exp_status)

    def track_tables(self, tables, exp_status=200):
        queries = []
        for table in tables:
            q = {
                'type' : 'track_table',
                'args' : table
            }
            queries.append(q)
        return self.run_bulk(queries, exp_status)

    def track_table(self, table, exp_status=200):
        q = self.mk_track_table_q(table)
        return self.v1q(q, exp_status)

    def mk_track_table_q(self, table):
        return {
            'type' : 'track_table',
            'args' : table
        }

    def add_remote_schema(self, name, remote_url, headers={}, client_hdrs=False):
        def hdr_name_val_pair(headers):
            nvp = []
            for (k,v) in headers.items():
                nvp.append({'name': k, 'value': v})
            return nvp
        if len(headers) > 0:
            client_hdrs = True
        q = {
            'type' : 'add_remote_schema',
            'args': {
                'name': name,
                'comment': name,
                'definition': {
                    'url': remote_url,
                    'headers':  hdr_name_val_pair(headers),
                    'forward_client_headers': client_hdrs
                }
            }
        }
        return self.v1q(q)


    def create_remote_obj_rel_to_itself(self, tables_schema, remote, remote_tables_schema):
        print("Creating remote relationship to the tables in schema {} to itself using remote {}".format(tables_schema, remote))
        fk_constrnts = self.pg.get_all_fk_constraints(tables_schema)
        for (s,_,t,c,fs,ft,fc) in fk_constrnts:
            table_cols = self.pg.get_all_columns_of_a_table(t, s)
            if not 'id' in table_cols:
                continue
            rel_name = 'remote_' + inflection.singularize(t) + '_via_' + c
            query ={
                'type': 'create_remote_relationship',
                'args' : {
                    'name' : rel_name,
                    'table' : {
                        'schema': s,
                        'name': t
                    },
                    'remote_schema': remote,
                    'hasura_fields': ['id', c],
                    'remote_field': {
                        remote_tables_schema + '_' + ft + '_by_pk' : {
                            'arguments': {
                                'id':  '$' + c
                            },
                            'field': {
                                inflection.pluralize(t) + '_by_' + c: {
                                    'arguments' : {
                                        'where': {
                                            c : {
                                                '_eq': '$id'
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            print(query)
            self.v1q(query)

    def create_remote_obj_fk_ish_relationships(self, tables_schema, remote, remote_tables_schema):
        print("Creating object foreign key ish relationships for tables in schema {} using remote {}".format(tables_schema, remote))
        fk_constrnts = self.pg.get_all_fk_constraints(tables_schema)
        for (s,_,t,c,fs,ft,fc) in fk_constrnts:
            rel_name = inflection.singularize(ft)
            if c.endswith('_id'):
                rel_name = c[:-3]
            rel_name = 'remote_' + rel_name
            query ={
                'type': 'create_remote_relationship',
                'args' : {
                    'name' : rel_name,
                    'table' : {
                        'schema': s,
                        'name': t
                    },
                    'remote_schema': remote,
                    'hasura_fields': [c],
                    'remote_field': {
                        remote_tables_schema + '_' + ft + '_by_pk' : {
                            'arguments' : {
                                'id':  '$' + c
                            }
                        }
                    }

                }
            }
            print(query)
            self.v1q(query)

    def create_obj_fk_relationships(self, schema='public'):
        print("Creating object foreign key relationships for tables in schema ", schema)
        fk_constrnts = self.pg.get_all_fk_constraints(schema)
        queries = []
        for (s,_,t,c,fs,ft,fc) in fk_constrnts:
            rel_name = inflection.singularize(ft)
            if c.endswith('_id'):
                rel_name = c[:-3]
            table_cols = self.pg.get_all_columns_of_a_table(t, s)
            if rel_name in table_cols:
                rel_name += '_' + inflection.singularize(ft)
            queries.append({
                'type' : 'create_object_relationship',
                'args': {
                    'table': {
                        'schema': s,
                        'name': t
                    },
                    'name': rel_name,
                    'using': {
                        'foreign_key_constraint_on': c
                    }
                }
            })
            self.obj_fk_rels.add(((s,t),rel_name))
        return self.run_bulk(queries)

    def create_remote_arr_fk_ish_relationships(self, tables_schema, remote, remote_tables_schema):
        fk_constrnts = self.pg.get_all_fk_constraints(tables_schema)
        for (s,_,t,c,fs,ft,fc) in fk_constrnts:
            rel_name = 'remote_' + inflection.pluralize(t) + '_by_' + c
            query ={
                'type': 'create_remote_relationship',
                'args' : {
                    'name' : rel_name,
                    'table' : {
                        'schema': fs,
                        'name': ft
                    },
                    'remote_schema': remote,
                    'hasura_fields': ['id'],
                    'remote_field': {
                        remote_tables_schema + '_' + t : {
                            'arguments' : {
                                'where': {
                                    c : {
                                        '_eq': '$id'
                                    }
                                }
                            }
                        }
                    }
                }
            }
            print(query)
            self.v1q(query)

    def create_arr_fk_relationships(self, schema='public'):
        print("Creating array foreign key relationships for tables in schema ", schema)
        fk_constrnts = self.pg.get_all_fk_constraints(schema)
        queries = []
        for (s,cn,t,c,fs,ft,fc) in fk_constrnts:
            rel_name = inflection.pluralize(t) + '_by_' + c
            queries.append({
                'type' : 'create_array_relationship',
                'args': {
                    'table': {
                        'schema': fs,
                        'name': ft
                    },
                    'name': rel_name,
                    'using': {
                        'foreign_key_constraint_on': {
                            'table': {
                                'schema': s,
                                'name': t
                            },
                            'column': c
                        }
                    }
                }
            })
            self.arr_fk_rels.add(((fs,ft),rel_name))
        return self.run_bulk(queries)

    def run_sql(self, sql, exp_status=200):
        return self.v1q(self.mk_run_sql_q(sql))

    def mk_run_sql_q(self, sql):
        return {
            'type' : 'run_sql',
            'args': {
                'sql' : sql
            }
        }
