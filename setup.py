from setuptools import setup

setup(
    name="airtable-async",
    packages=["airbase", "airbase.utils"],
    description="An asynchronous Python API Wrapper for the Airtable API",
    author="Luis Felipe Paris",
    author_email="lfparis@gmail.com",
    url="https://github.com/lfparis/airbase",
    version="0.0.1b1",
    install_requires=["aiohttp", "pandas"],
    python_requires="!=2.7.*, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*, !=3.4.*, !=3.5.*, !=3.6.*",  # noqa: E501
    keywords=["airtable", "api", "async", "async.io"],
    license="The MIT License (MIT)",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Environment :: Console",
        "Framework :: AsyncIO",
        "Intended Audience :: Developers",
        "Programming Language :: Python",
        "Topic :: Software Development",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: Implementation :: CPython",
    ],
)
