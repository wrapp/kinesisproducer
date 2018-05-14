from setuptools import setup

setup(
    name="kinesis-producer",
    version="0.0.1",
    packages=['wrapp', 'wrapp.kinesisproducer'],
    author="Fredrik Håård",
    author_email="fredrik@metallapan.se",
    description="Single client for producing Kinesis Firehose or Stream (planned) records. Python 3.3+.",
    license="MIT",
    keywords="aws kinesis firehose streams",
    url="https://github.com/wrapp/kinesisproducer",
    classifiers="""Development Status :: 3 - Alpha
Intended Audience :: Developers
Topic :: Software Development :: Libraries
License :: Freely Distributable
License :: OSI Approved :: MIT License
Operating System :: OS Independent
Programming Language :: Python :: 3
Programming Language :: Python :: 3.3
Programming Language :: Python :: 3.4
Programming Language :: Python :: 3.5""".split('\n'),
    long_description=open('README.md', 'rt').read(),
)
