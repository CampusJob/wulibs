
import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="wulibs",
    version="0.0.2",

    description="WayUp Python DevOps Libraries",

    url="https://github.com/campusjob/wulibs",
    author="Olivier Pilotte",
    author_email="opilotte@wayup.com",
    license="Apache Licence v2.0",

    packages=setuptools.find_packages(exclude=["docs", "tests*"]),

    install_requires=["munch", "kubernetes", "psycopg2", "requests"],
)
