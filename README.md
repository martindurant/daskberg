# Python client for iceberg

### Instructions for running the local REST server

- clone https://github.com/tabular-io/iceberg-rest-image
- build with ``gradle``
- run ``docker build -t ice .`` in that directory
```- run ``docker run -p 8181:8181 -v /tmp:${PWD}/rest ice`` in *this* directory
```