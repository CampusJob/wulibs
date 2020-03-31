
import argparse
import logging
import os
import sys
import wulibs


DATABASE_USERNAME = os.getenv("DATABASE_USERNAME", "dblab")
DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD", "dblab")
DATABASE_NAME = os.getenv("DATABASE_NAME", "tcjpr")

KUBERNETES_NAMESPACE = os.getenv("KUBERNETES_NAMESPACE", "default")

DBLAB_ENDPOINT = os.getenv("DBLAB_ENDPOINT", "10.0.61.51")
DBLAB_URL = os.getenv("DBLAB_URL", "http://dblab.default.svc.cluster.local")
DBLAB_TOKEN = os.getenv("DBLAB_TOKEN", "secret_token")


def delete_dblab_pr_db(pr_number):

    try:
        dblab = wulibs.DatabaseLab(DBLAB_URL, DBLAB_TOKEN)
        dblab.is_reachable()

        database_id = f"pr{pr_number}-anon-db"

        logging.info(f"deleting database [{database_id}]")
        dblab.delete_database(database_id)

        kube = wulibs.Kubernetes()

        logging.info(f"deleting kubernetes service [{database_id}]")
        kube.delete_service(KUBERNETES_NAMESPACE, database_id)

    except Exception as e:
        logging.exception(e)
        sys.exit(1)


if __name__ == "__main__":
    logging.basicConfig(
        stream=sys.stdout, level=logging.INFO,
        format="%(levelname)s - %(message)s")

    parser = argparse.ArgumentParser(description="Delete DBLab PR database")
    parser.add_argument("pr_number", type=int, help="The PR number")
    args = parser.parse_args()

    delete_dblab_pr_db(args.pr_number)
