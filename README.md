# concord-py

**Python client libraries for Concord**

## Building

This repository is organized to work with the pip python package manager.
In order to create the package you will need the necessary generated code.
Run the build_thrift script from the root of the concord project:
```
~/../concord/ $ ./configure.py --thrift
```

The generated code this package depends on is now located in ``concord/internal``.
To install the package and its dependencies use pip:
```
pip install concord
```

