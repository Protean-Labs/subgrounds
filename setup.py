import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="subgrounds",
    version="0.0.1",
    author="Protean Labs",
    author_email="info@protean.so",
    description="A framework for integrating The Graph data with dash components.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Protean-Labs/subgrounds",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License"
    ],
    package_dir={"": "subgrounds"},
    packages=setuptools.find_packages(where="subgrounds"),
    python_requires=">=3.10",
)