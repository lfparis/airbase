from setuptools import setup

setup(
    name="airtable-async",
    packages=["airbase", "airbase.utils"],
    description="An asynchronous Python API Wrapper for the Airtable API",
    author="Luis Felipe Paris",
    author_email="lfparis@gmail.com",
    url="https://github.com/lfparis/airbase",
    version="0.0.1b9",
    install_requires=["aiohttp"],
    extras_require={"tools": ["pandas"]},
    package_data={"airbase": ["py.typed"]},
    zip_safe=False,
    python_requires=">=3.7",
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
