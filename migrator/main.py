from flask import Flask
from flask import jsonify
import threading
from datetime import datetime
import yaml
import json
import psycopg2
import mysql.connector as mariadb
import time


def read_yaml(file_name):
    with open(file_name, 'r') as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    return config


def get_connection(db_credentials, db_type):
    source_lib = mariadb if db_type == 'mariadb' else psycopg2
    conn = None
    while conn is None:
        try:
            conn = source_lib.connect(user=db_credentials['user'],
                                      password=db_credentials['password'],
                                      host=db_credentials['host'],
                                      database=db_credentials["database"],
                                      port=db_credentials["port"])
            conn.autocommit = False
        except (Exception, psycopg2.Error, mariadb.Error) as e:
            print(f'Database does not seem to be ready yet: {e}')
            time.sleep(3)
    print('Connected to ' + db_type)
    return conn


class Migrator:

    def __init__(self):
        migrator_config = read_yaml("migrator_config.yaml")
        self.schema_mapping_config = read_yaml("schema_mapping_config.yaml")
        self.migration_period = migrator_config['migration_period']
        self.migration_log_file = migrator_config['log_file_name']
        self.source_table_config = migrator_config['source_table_credentials']
        self.source_conn = get_connection(self.source_table_config, db_type='postgresql')
        self.target_conn = get_connection(migrator_config['target_table_credentials'], db_type='mariadb')
        self.insert_query = "INSERT INTO events(" + ",".join(self.schema_mapping_config.values()) + ") values (" + \
                            ",".join(("%s",) * len(self.schema_mapping_config)) + ")"

    def log_migration(self, count, execution_time):
        message = {"date": datetime.now().strftime("%d/%m/%y %H:%M:%S"),
                   "migrated_records_count": count,
                   "execution_time": execution_time}
        with open(self.migration_log_file, 'a+') as f:
            json.dump(message, f)
            f.write(", \n")

    def start_migration(self):
        start_time = time.time()
        select_query = "DELETE FROM events RETURNING %s" % (",".join(self.schema_mapping_config.keys()))
        source_cursor = self.source_conn.cursor()
        source_cursor.execute(select_query)
        data_to_migrate = list(source_cursor.fetchall())
        written_records_count = len(data_to_migrate)
        target_cursor = self.target_conn.cursor()
        try:
            target_cursor.executemany(self.insert_query, data_to_migrate)
            self.target_conn.commit()
            self.source_conn.commit()
        except Exception:
            self.source_conn.rollback()
            print("EXCEPTION ROLLED BACK")
            exit(1)

        execution_time = time.time() - start_time
        self.log_migration(written_records_count, execution_time)
        return written_records_count


def main():
    app = Flask(__name__)
    migrator = Migrator()

    @app.route('/get_migrations', methods=['GET'])
    def read_migrations():
        try:
            with open(migrator.migration_log_file) as log:
                return jsonify(log.readlines())
        except Exception:
            return "Migrations list is empty"

    # made it as GET to debug and test it easier
    @app.route('/start_migration', methods=['GET'])
    def start_migration_call():
        migrator.start_migration()
        return "Migration started"

    app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True
    threading.Thread(target=lambda: app.run(host='0.0.0.0', port=5000, use_reloader=False)).start()
    while True:
        threading.Thread(target=migrator.start_migration).start()
        time.sleep(migrator.migration_period)


if __name__ == '__main__':
    main()
    