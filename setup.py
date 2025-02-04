from setuptools import setup, find_packages

setup(
    name="success_rates",
    version="0.1.0",
    description="A package to compute vehicle detection success rates from DuckDB-supported files.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Daniel Libicki",
    author_email="daniel.libicki@gmail.com",
    url="https://github.com/mccomplete/success",
    packages=find_packages(),
    install_requires=[
        "duckdb",
    ],
    python_requires=">=3.8",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
