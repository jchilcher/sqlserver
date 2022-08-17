import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="sqlserver",
    version="0.0.5",
    author="Johnathen Chilcher",
    author_email="jchilcher@cursedwave.com",
    description="Installable Python Class utilizing pyodbc to run common queries as functions.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/jchilcher/sqlserver",
    packages=setuptools.find_packages(),
    install_requires=[
        'pyodbc'
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: GNU GPLv3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.8',
)