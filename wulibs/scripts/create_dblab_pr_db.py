
import argparse
import logging
import munch
import os
import time
import sys
import wulibs


NAMESERVER = os.getenv("NAMESERVER", "100.64.0.10")

DATABASE_USERNAME = os.getenv("DATABASE_USERNAME", "dblab")
DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD", "dblab")
DATABASE_NAME = os.getenv("DATABASE_NAME", "tcjpr")

KUBERNETES_NAMESPACE = os.getenv("KUBERNETES_NAMESPACE", "default")
DBLAB_ENDPOINT = os.getenv("DBLAB_ENDPOINT", "10.0.59.193")
DBLAB_URL = os.getenv("DBLAB_URL", "http://dblab.default.svc.cluster.local")
DBLAB_TOKEN = os.getenv("DBLAB_TOKEN", "secret_token")
DBLAB_SNAPSHOT_NAME = os.getenv("DBLAB_SNAPSHOT_NAME", "dblab_pool@initdb")


def create_dblab_pr_db(pr_number, analyze=False):
    try:
        dblab = wulibs.DatabaseLab(DBLAB_URL, DBLAB_TOKEN, DBLAB_SNAPSHOT_NAME)
        dblab.is_reachable()

        database_id = f"pr{pr_number}-anon-db"

        logging.info(f"creating database [{database_id}]")
        dblab.create_database(database_id, DATABASE_USERNAME, DATABASE_PASSWORD)

        logging.info(f"waiting for database [{database_id}] to be available")
        dblab.wait_for_availability(database_id)

        logging.info(f"getting database [{database_id}]")
        database = dblab.get_database(database_id)

        service = munch.Munch.fromDict({
            "port": 5432,
            "target_port": int(database.db.port),
            "service_name": database_id,
            "namespace": KUBERNETES_NAMESPACE
        })

        kube = wulibs.Kubernetes()

        logging.info(f"creating kubernetes service [{service.service_name}] "
                     f"with endpoint [{DBLAB_ENDPOINT}][{service.target_port}]")
        kube.update_service(service)
        kube.update_endpoints(service, [DBLAB_ENDPOINT])

        database_host = f"{service.service_name}.{service.namespace}.svc.cluster.local."

        logging.info(f"trying to resolve {service.service_name}")
        dns_resolver = wulibs.DnsResolver(nameserver=NAMESERVER)
        dns_resolver.resolve(database_host)

        logging.info(f"trying to reach database {database_id} at {service.service_name}:{service.port}")
        dblab.wait_for_database_to_be_reachable(database_host, service.port)

        database_url = f"{database_id}.{KUBERNETES_NAMESPACE}.svc.cluster.local"
        logging.info(f"updating ownership of entities to user {DATABASE_USERNAME}")
        dblab.update_ownership(database_url, service.port, DATABASE_NAME, DATABASE_USERNAME, DATABASE_PASSWORD)

        logging.info(f"PGPASSWORD={DATABASE_PASSWORD} psql -U {DATABASE_USERNAME} -h "
                     f"{database_id}.{KUBERNETES_NAMESPACE}.svc.cluster.local -p 5432 {DATABASE_NAME}")

        if analyze:
            start_time = time.time()

            logging.info(f"analyzing database [{database_url}]")
            dblab.analyze(database_url, service.port, DATABASE_NAME, DATABASE_USERNAME, DATABASE_PASSWORD)

            end_time = time.time()

            logging.info(f"database analysis complete in {str(end_time - start_time )}s.")

    except Exception as e:
        logging.exception(e)
        sys.exit(1)


if __name__ == "__main__":
    logging.basicConfig(
        stream=sys.stdout, level=logging.INFO,
        format="%(levelname)s - %(message)s")

    parser = argparse.ArgumentParser(description="Create DBLab PR database")
    parser.add_argument("pr_number", type=int, help="The PR number")
    parser.add_argument("--analyze", help="Analyze PR database", action="store_true")
    args = parser.parse_args()

    create_dblab_pr_db(args.pr_number, args.analyze)
