import requests
from zipfile import ZipFile
import requests_cache
import os
import threading
from port_allocator import PortAllocator
from run_postgres import Postgres
from run_hge import HGE
from colorama import Fore, Style

def get_postgres_url():
    return os.environ['HASURA_GRAPHQL_DATABASE_URL']

def get_hge_url():
    return os.environ['HGE_URL']

def get_hge_key():
    return os.environ.get['HASURA_GRAPHQL_ADMIN_SECRET']


class Test:

    default_pg_docker_image = 'circleci/postgres:11.5-alpine-postgis'

    sportsdb_url='http://www.sportsdb.org/modules/sd/assets/downloads/sportsdb_sample_postgresql.zip'

    default_work_dir = 'test_output'

    previous_work_dir_file = '.previous_work_dir'

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

    def __init__(self, pg_docker_image=default_pg_docker_image, admin_secret=None):
        port_allocator = PortAllocator()
        self.graphql_queries_file = os.path.abspath('queries.graphql')
        self.work_dir = self.get_work_dir()
        print ("WORK_DIR: ", self.work_dir)
        os.makedirs(self.work_dir, exist_ok=True)
        requests_cache.install_cache(self.work_dir + '/sportsdb_cache')
        self.pg = Postgres(
            port_allocator=port_allocator, docker_image=pg_docker_image,
            db_data_dir= self.work_dir + '/sportsdb_data')
        self.remote_pg = Postgres(
            port_allocator=port_allocator, docker_image=pg_docker_image,
            db_data_dir= self.work_dir + '/remote_sportsdb_data')
        self.hge = HGE(
            pg=self.pg, port_allocator=port_allocator, admin_secret=admin_secret,
            log_file= self.work_dir + '/hge.log')
        self.remote_hge = HGE(
            pg=self.remote_pg, port_allocator=port_allocator, admin_secret=admin_secret,
            log_file= self.work_dir + '/remote_hge.log')
        self.set_previous_work_dir()

    def setup_graphql_engines(self):

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

        self.remote_hge.run_with_stack()
        self.hge.run_with_stack()

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

if __name__ == "__main__":
    test = Test()
    try:
        test.setup_graphql_engines()
        print("Hasura GraphQL engine is running on URL:",test.hge.url+ '/v1/graphql')
        input(Fore.BLUE+'Press Enter to stop GraphQL engine' + Style.RESET_ALL)
    finally:
        test.teardown()
