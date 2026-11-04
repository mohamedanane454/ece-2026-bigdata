# Lab 03 / lab-1 - Projet Python avec uv et generation des datasets
pwd
mkdir /home/onyxia/work/$GIT_REPO_NAME
cd /home/onyxia/work/$GIT_REPO_NAME
git config --global init.defaultBranch main
git init
cat <<'INI' >.gitignore
.*
!.gitignore
!.python-version
__pycache__/
INI
git config --global user.name
git config --global user.email
command -v uv
uv init --package
uv sync
ls -a
cat pyproject.toml
git add .gitignore .python-version pyproject.toml README.md src uv.lock
git commit -m "feat: initial project layout"
# Remote GitHub : ajoute plus tard (git remote add origin https://github.com/<user>/$GIT_REPO_NAME.git)
gpush -u origin main
cat <<'MD' >README.md
# Dataset generator

This project generates a random dataset consisting of users and orders. Scripts are written in Python and the project
uses [uv](https://docs.astral.sh/uv/).

## Usage

```bash
uv run dataset-users -h
#> usage: dataset-users [-h] [-c COUNT] [-o {csv,json,jsonline}]
uv run dataset-orders -h
#> usage: dataset-orders [-h] [-C COUNT_MIN] [-c COUNT_MAX] [-d DATE_FROM] [-o {csv,json,jsonline}] [-u COUNT_USERS]
```
MD
git add README.md
git commit -m "docs: project introduction and usages"
gpush
cat <<'PY' >src/$UV_PROJECT_NAME/serialize.py
import csv
import io
import json


def serialize(dataset, output):
    """Print a list of dictionaries in the requested format."""
    if output == "":
        return
    elif output == "json":
        print(json.dumps(dataset, default=str))
    elif output == "jsonline":
        for record in dataset:
            print(json.dumps(record, default=str))
    elif output == "csv":
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=dataset[0].keys())
        writer.writeheader()
        writer.writerows(dataset)
        print(buffer.getvalue(), end="")
    else:
        raise ValueError("Unsupported output format.")
PY
uv add "faker>=40.37.0" || uv add faker
cat pyproject.toml
cat <<'PY' >src/$UV_PROJECT_NAME/dataset_users.py
import argparse

from faker import Faker

from .serialize import serialize

fake = Faker()
# Generate the same dataset on every execution
Faker.seed(42)


def users_generate(count=50, output=""):
    users = []
    for _ in range(count):
        user = {"uuid": fake.uuid4(), **fake.simple_profile()}
        users.append(user)
    serialize(users, output)
    return users


def main():
    parser = argparse.ArgumentParser(prog="dataset-users", description="Users generator")
    parser.add_argument(
        "-c", "--count", help="Number of users to generate.", type=int, default=50
    )
    parser.add_argument(
        "-o",
        "--output",
        help="Output format.",
        default="json",
        choices=["csv", "json", "jsonline"],
    )
    args = parser.parse_args()
    users_generate(args.count, args.output)


if __name__ == "__main__":
    main()
PY
cat <<'PY' >src/$UV_PROJECT_NAME/dataset_orders.py
import argparse
import datetime

from faker import Faker
from faker.providers import DynamicProvider

from .dataset_users import users_generate
from .serialize import serialize

fake = Faker()
# Generate the same dataset on every execution
Faker.seed(42)
fake.add_provider(
    DynamicProvider(
        provider_name="product",
        elements=["bread", "brioche", "cookie", "croissant", "donut", "drink"],
    )
)


def orders_generate(count_min=0, count_max=100, count_users=50, date_from=None, output=""):
    orders = []
    date_start = date_from or datetime.datetime(2020, 1, 1, tzinfo=datetime.UTC)
    for user in users_generate(count_users):
        for _ in range(fake.pyint(min_value=count_min, max_value=count_max)):
            orders.append(
                {
                    "uuid": fake.uuid4(),
                    "user_uuid": user["uuid"],
                    "date": fake.date_time_between(
                        date_start, date_start + datetime.timedelta(hours=1), tzinfo=datetime.UTC
                    ),
                    "quantity": fake.pyint(min_value=1, max_value=5),
                    "product": fake.product(),
                }
            )
            # Each order is placed in the hour following the previous one
            date_start += datetime.timedelta(hours=1)
    serialize(orders, output)
    return orders


def main():
    parser = argparse.ArgumentParser(prog="dataset-orders", description="Orders generator")
    parser.add_argument(
        "-C",
        "--count-min",
        help="Minimum number of orders to generate per user.",
        type=int,
        default=0,
    )
    parser.add_argument(
        "-c",
        "--count-max",
        help="Maximum number of orders to generate per user.",
        type=int,
        default=100,
    )
    parser.add_argument(
        "-d",
        "--date-from",
        help="Date of the first order, in ISO format (default: 2020-01-01).",
        type=lambda value: datetime.datetime.fromisoformat(value).replace(tzinfo=datetime.UTC),
    )
    parser.add_argument(
        "-o",
        "--output",
        help="Output format.",
        default="json",
        choices=["csv", "json", "jsonline"],
    )
    parser.add_argument(
        "-u", "--count-users", help="Number of users to generate.", type=int, default=50
    )
    args = parser.parse_args()
    orders_generate(args.count_min, args.count_max, args.count_users, args.date_from, args.output)


if __name__ == "__main__":
    main()
PY
# Remplace l'entree par defaut de [project.scripts] par les deux generateurs
python3 - <<'PY'
import re
p = "pyproject.toml"
s = open(p).read()
s = re.sub(r'(\[project\.scripts\]\n)(.*\n)', r'\1dataset-users = "ece_2026_bigdata.dataset_users:main"\ndataset-orders = "ece_2026_bigdata.dataset_orders:main"\n', s)
open(p, "w").write(s)
PY
cat pyproject.toml
uv run dataset-users -h
uv run dataset-users -c 2 -o jsonline
uv run dataset-orders -h
uv run dataset-orders -u 2 -C 1 -c 2 -o json | jq '.[0]'
git add pyproject.toml uv.lock src
git commit -m "feat: dataset generation scripts"
gpush
git log --oneline
true
