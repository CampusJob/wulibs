
import argparse
import logging
import os
import sys

from wulibs.scripts import create_dblab_pr_db
from wulibs.scripts import delete_dblab_pr_db


NAMESERVER = os.getenv("NAMESERVER", "100.64.0.10")

DATABASE_USERNAME = os.getenv("DATABASE_USERNAME", "dblab")
DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD", "dblab")
DATABASE_NAME = os.getenv("DATABASE_NAME", "tcjpr")

KUBERNETES_NAMESPACE = os.getenv("KUBERNETES_NAMESPACE", "default")

DBLAB_ENDPOINT = os.getenv("DBLAB_ENDPOINT", "10.0.61.195")
DBLAB_URL = os.getenv("DBLAB_URL", "http://dblab.default.svc.cluster.local")
DBLAB_TOKEN = os.getenv("DBLAB_TOKEN", "secret_token")
DBLAB_SNAPSHOT_NAME = os.getenv("DBLAB_SNAPSHOT_NAME", "dblab_pool@initdb")


def recreate_dblab_pr_db(pr_number):
    database_id = f"pr{pr_number}-anon-db"

    logging.info(f"recreating PR database [{database_id}]")

    delete_dblab_pr_db.delete_dblab_pr_db(pr_number)
    create_dblab_pr_db.create_dblab_pr_db(pr_number)


if __name__ == "__main__":
    logging.basicConfig(
        stream=sys.stdout, level=logging.INFO,
        format="%(levelname)s - %(message)s")

    parser = argparse.ArgumentParser(description="Recreate DBLab PR database")
    parser.add_argument("pr_number", type=int, help="The PR number")
    args = parser.parse_args()

    recreate_dblab_pr_db(args.pr_number)
