from setuptools import setup

try:
    import pypandoc

    long_description = pypandoc.convert_file("README.md", "rst")
except (IOError, ImportError):
    long_description = open("README.md").read()

setup(
    name="bigquery-fdw",
    version="2.4.0",
    description="BigQuery Foreign Data Wrapper for PostgreSQL",
    long_description=long_description,
    author="Gabriel Bordeaux",
    author_email="pypi@gab.lc",
    url="https://github.com/gabfl/bigquery_fdw",
    license="MIT",
    packages=["bigquery_fdw"],
    package_dir={"bigquery_fdw": "src"},
    # external dependencies
    install_requires=[
        "google-cloud-bigquery==3.11.1",
        "google-cloud-bigquery-storage==2.20.0",
        "db-dtypes==1.1.1",
        "pandas==2.0.2",
        "pyarrow",
    ],
    entry_points={
        "console_scripts": [
            # 'bigquery_fdw = bigquery_fdw.fdw:main',
            "bq_client_test = bigquery_fdw.bqclient_test:main",
        ],
    },
    classifiers=[  # see https://pypi.python.org/pypi?%3Aaction=list_classifiers
        "Topic :: Database",
        "Topic :: Database :: Database Engines/Servers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: MacOS",
        "Operating System :: POSIX :: Linux",
        "Natural Language :: English",
        #  'Programming Language :: Python :: 2.7',
        "Programming Language :: Python :: 3",
        "Programming Language :: Python",
        "Development Status :: 4 - Beta",
        #  'Development Status :: 5 - Production/Stable',
    ],
)
