import requests
from zipfile import ZipFile
import requests_cache
import os
import threading
from port_allocator import PortAllocator
from run_postgres import Postgres
from run_hge import HGE
from colorama import Fore, Style
import argparse
import sys


def _first_true(iterable, default=False, pred=None):
    return next(filter(pred, iterable), default)


class HGETestSetup:

    sportsdb_url='http://www.sportsdb.org/modules/sd/assets/downloads/sportsdb_sample_postgresql.zip'

    default_work_dir = 'test_output'

    previous_work_dir_file = '.previous_work_dir'

    def __init__(self, pg_urls, pg_docker_image, hge_docker_image=None, hge_admin_secret=None, skip_stack_build=False):
        self.pg_url, self.remote_pg_url = pg_urls or (None, None)
        self.pg_docker_image = pg_docker_image
        self.hge_docker_image = hge_docker_image
        self.hge_admin_secret = hge_admin_secret
        self.skip_stack_build = skip_stack_build
        self.graphql_queries_file = os.path.abspath('queries.graphql')
        self.port_allocator = PortAllocator()
        self.set_work_dir()
        self.init_pgs()
        self.init_hges()
        self.set_previous_work_dir()

    def get_previous_work_dir(self):
        try:
            with open(self.previous_work_dir_file) as f:
                return f.read()
        except FileNotFoundError:
            return None

    def set_previous_work_dir(self):
        with open(self.previous_work_dir_file, 'w') as f:
            return f.write(self.work_dir)

    def get_work_dir(self):
        default_work_dir = self.get_previous_work_dir() or self.default_work_dir
        return os.environ.get('WORK_DIR') \
            or input(Fore.YELLOW + '(Set WORK_DIR environmental variable to avoid this)\n'
                     + 'Please specify the work directory. (default:{}):'.format(default_work_dir)
                     + Style.RESET_ALL).strip() \
            or default_work_dir


    def set_work_dir(self):
        self.work_dir = self.get_work_dir()
        print ("WORK_DIR: ", self.work_dir)
        os.makedirs(self.work_dir, exist_ok=True)
        requests_cache.install_cache(self.work_dir + '/sportsdb_cache')

    def init_pgs(self):
        pg_confs = [
            ('sportsdb_data', self.pg_url),
            ('remote_sportsdb_data', self.remote_pg_url)
        ]
        self.pg, self.remote_pg = [
            Postgres(
                port_allocator=self.port_allocator, docker_image=self.pg_docker_image,
                db_data_dir= self.work_dir + '/' + data_dir, url=url
            )
            for (data_dir, url) in pg_confs
        ]

    def init_hges(self):
        hge_confs = [
            (self.pg, 'hge.log'),
            (self.remote_pg, 'remote_hge.log')
        ]
        self.hge, self.remote_hge = [
            HGE(
                pg=pg, port_allocator=self.port_allocator, admin_secret=self.hge_admin_secret,
                log_file= self.work_dir + '/' + log_file, docker_image=self.hge_docker_image)
            for (pg, log_file) in hge_confs
        ]

    def setup_graphql_engines(self):

        if not self.hge_docker_image and not self.skip_stack_build:
            HGE.do_stack_build()

        def run_concurrently(threads):
            for thread in threads:
                thread.start()

            for thread in threads:
                thread.join()

        def run_concurrently_fns(*fns):
            threads = [threading.Thread(target=fn) for fn in fns]
            return run_concurrently(threads)

        def set_hge(hge, schema, hge_type):
            pg = hge.pg
            # Schema and data
            pg.run_sql_from_file(sql_file)
            pg.set_id_as_primary_key_for_tables(schema='public')
            pg.move_tables_to_schema('public', schema)

            # Metadata stuff
            hge.track_all_tables_in_schema(schema)
            hge.create_obj_fk_relationships(schema)
            hge.create_arr_fk_relationships(schema)

        run_concurrently_fns(
            self.pg.start_postgres_docker,
            self.remote_pg.start_postgres_docker)
        print("Postgres url:", self.pg.url)
        print("Remote Postgres url:", self.remote_pg.url)

        self.remote_hge.run()
        self.hge.run()

        # Skip if the tables are already present
        tables = self.pg.get_all_tables_in_a_schema('hge')
        if len(tables) > 0:
            return

        # Download sportsdb
        zip_file = self.download_sportsdb_zip(self.work_dir+ '/sportsdb.zip')
        sql_file = self.unzip_sql_file(zip_file)

        # Create the required tables and move them to required schemas
        hge_thread = threading.Thread(
            target=set_hge, args=(self.hge, 'hge', 'Main'))
        remote_hge_thread = threading.Thread(
            target=set_hge, args=(self.remote_hge, 'remote_hge', 'Remote'))
        run_concurrently([hge_thread, remote_hge_thread])

        # Add remote_hge as remote schema
        self.hge.add_remote_schema(
            'remote_hge', self.remote_hge.url + '/v1/graphql',
            self.remote_hge.admin_auth_headers())

        tables = self.pg.get_all_tables_in_a_schema('hdb_catalog')
        if 'hdb_remote_relationship' not in tables:
            return

        # Create remote relationships
        self.hge.create_remote_obj_rel_to_itself('hge', 'remote_hge', 'remote_hge')
        self.hge.create_remote_arr_fk_ish_relationships('hge', 'remote_hge', 'remote_hge')
        self.hge.create_remote_obj_fk_ish_relationships('hge', 'remote_hge', 'remote_hge')


    def teardown(self):
        for res in [self.hge, self.remote_hge, self.pg, self.remote_pg]:
           res.teardown()

    def download_sportsdb_zip(self, filename, url=sportsdb_url):
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            total=0
            print()
            with open(filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        total += len(chunk)
                        print("\rDownloaded: ", int(total/1024)  , 'KB', end='')
                        f.write(chunk)
            print('\nDB Zip File:', filename)
        return filename

    def unzip_sql_file(self, zip_file):
        with ZipFile(zip_file, 'r') as zip:
            sql_file = zip.infolist()[0]
            print('DB SQL file:', sql_file.filename)
            zip.extract(sql_file, self.work_dir)
        return self.work_dir + '/' + sql_file.filename


class HGETestSetupWithArgs(HGETestSetup):

    default_pg_docker_image = 'circleci/postgres:11.5-alpine-postgis'

    def __init__(self):
        self.set_arg_parse_options()
        self.parse_args()
        super().__init__(
            pg_urls = self.pg_urls,
            pg_docker_image = self.pg_docker_image,
            hge_docker_image = self.hge_docker_image,
            hge_admin_secret = self.hge_admin_secret,
            skip_stack_build = self.skip_stack_build
        )

    def set_arg_parse_options(self):
        self.arg_parser = argparse.ArgumentParser()
        self.set_pg_options()
        self.set_hge_options()

    def parse_args(self):
        self.parsed_args = self.arg_parser.parse_args()
        self.set_pg_confs()
        self.set_hge_confs()

    def set_pg_confs(self):
        self.pg_urls, self.pg_docker_image = self.get_exclusive_params([
            ('pg_urls', 'HASURA_BENCH_PG_URLS'),
            ('pg_docker_image', 'HASURA_BENCH_PG_DOCKER_IMAGE')
        ])
        if self.pg_urls:
            self.pg_urls = self.pg_urls.split(',')
        else:
            self.pg_docker_image = self.pg_docker_image or self.default_pg_docker_image

    def set_hge_confs(self):
        self.hge_docker_image = self.get_param('hge_docker_image', 'HASURA_BENCH_DOCKER_IMAGE')
        self.hge_admin_secret = self.get_param('hge_admin_secret', 'HASURA_BENCH_HGE_ADMIN_SECRET')
        self.skip_stack_build = self.parsed_args.skip_stack_build

    def set_pg_options(self):
        self.arg_parser.add_argument('--pg-urls', metavar='HASURA_BENCH_PG_URLS', help='Postgres database urls to be used for tests, given as comma separated values', required=False)
        self.arg_parser.add_argument('--pg-docker-image', metavar='HASURA_BENCH_PG_DOCKER_IMAGE', help='Postgres docker image to be used for tests', required=False)

    def set_hge_options(self):
        self.arg_parser.add_argument('--hge-docker-image', metavar='HASURA_BENCH_HGE_DOCKER_IMAGE', help='GraphQl engine docker image to be used for tests', required=False)
        self.arg_parser.add_argument('--hge-admin-secret', metavar='HASURA_BENCH_HGE_ADMIN_SECRET', help='Admin secret set for GraphQL engines. By default, no admin secret is set', required=False)
        self.arg_parser.add_argument('--skip-stack-build', help='Skip stack build if this option is set', action='store_true', required=False)


    def get_param(self, attr, env):
        return _first_true([getattr(self.parsed_args, attr), os.getenv(env)])

    def get_exclusive_params(self, params_loc):
        excl_param = None
        params_out = []
        for (attr, env) in params_loc:
            param = self.get_param(attr, env)
            params_out.append(param)
            if param:
                if not excl_param:
                    excl_param = (param, attr, env)
                else:
                    (param1, attr1, env1) = excl_param
                    def loc(a, e):
                        arg = '--' + a.replace('_','-')
                        return arg + '(env: ' + e + ')'
                    print(loc(attr, env), 'and', loc(attr1, env1), 'should not be defined together')
                    sys.exit(1)
        return params_out


if __name__ == "__main__":
    test_setup = HGETestSetupWithArgs()
    try:
        test_setup.setup_graphql_engines()
        print("Hasura GraphQL engine is running on URL:",test_setup.hge.url+ '/v1/graphql')
        input(Fore.BLUE+'Press Enter to stop GraphQL engine' + Style.RESET_ALL)
    finally:
        test_setup.teardown()

