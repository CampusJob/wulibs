
import json
import logging
import munch
import psycopg2
import requests
import socket
import time

from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import closing
from psycopg2 import sql


class DatabaseLab:

    def __init__(self, url, token, snapshot_name, timeout=120):
        self.dblab_url = url
        self.headers = {"Verification-Token": token}
        self.snapshot_name = snapshot_name

        self.timeout = timeout

    def is_reachable(self):
        response = requests.get(f"{self.dblab_url}/status", headers=self.headers)
        if response.status_code != 200 or response.json()["status"]["code"] != "OK":
            raise Exception(f"database lab [{self.dblab_url}] is not is_reachable")

    def get_database(self, database_id):
        response = requests.get(f"{self.dblab_url}/clone/{database_id}", headers=self.headers)
        if response.status_code != 200:
            raise Exception("database [{database_id}] not found")

        return munch.Munch.fromDict(response.json())

    def list_databases(self):
        response = requests.get(f"{self.dblab_url}/status", headers=self.headers)
        return munch.Munch.fromDict(response.json()["clones"])

    def create_database(self, database_id, username, password):
        payload = {
            "id": database_id,
            "snapshot": {
                "id": self.snapshot_name
            },
            "protected": True,
            "db": {
                "username": username,
                "password": password
            }
        }

        try:
            self.get_database(database_id)
            logging.info(f"database [{database_id}] already exists")
            return

        except Exception:
            logging.info(f"database [{database_id}] not found, creating...")

        response = requests.post(f"{self.dblab_url}/clone", data=json.dumps(payload), headers=self.headers)
        if response.status_code != 201:
            raise Exception(f"failed to create database [{database_id}]")

    def delete_database(self, database_id):
        try:
            self.get_database(database_id)

        except Exception:
            logging.info(f"database [{database_id}] not found, skipping deletion.")
            return

        payload = {
            "protected": False
        }

        logging.info(f"unprotecting database [{database_id}]")
        response = requests.patch(f"{self.dblab_url}/clone/{database_id}", data=json.dumps(payload), headers=self.headers)

        if response.status_code != 200:
            raise Exception(f"failed to unprotect database [{database_id}]")

        logging.info(f"deleting database [{database_id}]")
        response = requests.delete(f"{self.dblab_url}/clone/{database_id}", headers=self.headers)

        if response.status_code != 200:
            raise Exception(f"failed to delete database [{database_id}]")

    def wait_for_availability(self, database_id):
        timeout = 0
        while timeout < self.timeout:
            database = self.get_database(database_id)
            if database.status.code == "OK":
                return

            timeout += 5
            time.sleep(5)

        raise Exception(f"database [{database_id}] not ready: {database.status.code}")

    def database_is_reachable(self, host, port=5432):
        timeout = 0
        while timeout < self.timeout:

            with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
                sock.settimeout(1)

                if sock.connect_ex((host, int(port))) == 0:
                    return True
                else:
                    logging.warning(f"{host}:{port} not yet reachable")

            timeout += 5
            time.sleep(5)

        raise Exception(f"database not reachable at [{host}:{port}]")

    def update_ownership(self, db_url, db_port, db_name, username, password):
        with psycopg2.connect(host=db_url,
                              port=db_port,
                              database=db_name,
                              user=username,
                              password=password) as connection:

            connection.autocommit = True
            with connection.cursor() as cursor:
                for schema in ["public", "inbox"]:
                    for _entity_type in ["table", "sequence", "view", ["matview", "materialized view"]]:
                        entity_type = _entity_type if type(_entity_type) == str else _entity_type[0]
                        query = f"SELECT {entity_type}NAME FROM pg_{entity_type}s WHERE SCHEMANAME='{schema}'"

                        cursor.execute(query)
                        results = cursor.fetchall()

                        for row in results:
                            entity_name = row[0]

                            entity_type = _entity_type if type(_entity_type) == str else _entity_type[1]
                            logging.debug(f"updating ownership of {entity_type} {entity_name} to {username}")
                            query = f"SET search_path = {schema}; ALTER {entity_type} {entity_name} OWNER TO {username}"

                            cursor.execute(query)

    def analyze(self, db_url, db_name, db_port, username, password):

        def run_analyze(table_name, connection):
            with connection.cursor() as cursor:
                cursor.execute(sql.SQL('ANALYZE {};').format(sql.Identifier(table_name)))

        with ThreadPoolExecutor(max_workers=10) as executor:
            with psycopg2.connect(host=db_url,
                                  port=db_port,
                                  database=db_name,
                                  password=password,
                                  user=username) as connection:

                connection.autocommit = True
                with connection.cursor() as cursor:
                    query = """\
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema in ('public', 'inbox')
                      AND table_type = 'BASE TABLE';
                    """
                    cursor.execute(query)
                    results = cursor.fetchall()

                future_to_table_name = {}
                for row in results:
                    table_name = row[0]
                    future_to_table_name[executor.submit(run_analyze, table_name, connection)] = table_name

                for future in as_completed(future_to_table_name):
                    table_name = future_to_table_name[future]
                    try:
                        _ = future.result()  # noqa

                    except Exception as e:
                        logging.error(f"{table_name} generated an exception: {e}")
