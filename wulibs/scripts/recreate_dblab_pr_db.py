
import argparse
import logging
import sys

import create_dblab_pr_db
import delete_dblab_pr_db


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
