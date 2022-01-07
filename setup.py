from setuptools import find_packages, setup


install_requires = open("requirements.txt").read().strip().split("\n")
dev_requires = open("requirements-dev.txt").read().strip().split("\n")


setup(
    # Package metadata
    name="prefect_aws",
    description="Prefect tasks and subflows for interacting with AWS",
    license="Apache License 2.0",
    author="Prefect Technologies, Inc.",
    author_email="help@prefect.io",
    url="https://github.com/PrefectHQ/prefect-aws",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    # Versioning
    version="0.1.0",
    # Package setup
    packages=find_packages(exclude=("tests", "docs")),
    # Requirements
    python_requires=">=3.7",
    install_requires=install_requires,
    extras_require={"dev": dev_requires},
)