import requests
example_schema = [{'id': 1, 'name': 'date', 'required': False, 'type': 'date'},
 {'id': 2, 'name': 'symbol', 'required': False, 'type': 'string'}]


class IceRESTClient:

    def __init__(self, endpoint):
        self.endpoint = endpoint
        self.namespace = None
        self.session = requests.Session()

    def url(self, path):
        return f"{self.endpoint}{path}"

    def list_namespaces(self):
        return self.session.get(self.url("/v1/namespaces")).json()["namespaces"]

    def get_namespace(self, namespace):
        return self.session.get(self.url(f"/v1/namespaces/{namespace}")).json()["properties"]

    def create_namespace(self, namespace):
        return self.session.post(
            self.url(f"/v1/namespaces"), json={"namespace": [namespace]}
        ).json()

    def list_tables(self, namespace=None):
        namespace = namespace or self.namespace
        return self.session.get(self.url(f"/v1/namespaces/{namespace}/tables")).json()["identifiers"]

    def create_table(self, name, schema, namespace=None, stage=False):
        """

        :param name: str
        :param schema: dict | list
            Like ``example_schema`` (a list), or {field_name: type} where
            the types have already been converted to ice.
        :param namespace: str | None
        :param stage: bool
            If True, commit is deferred until explicitly called using
            update_table
        :return:
        """
        if isinstance(schema, dict):
            schema = [{"id": i, "name": k, "type": v, "required": False}
                      for i, (k, v) in enumerate(schema.items())]
        namespace = namespace or self.namespace
        data = {
            "name": name,
            "schema": {
                "type": "struct",
                "schema-id": 0,
                "identifier-field-ids": [],
                "fields": schema
            },
            "stage-create": stage
        }
        return self.session.post(self.url(f"/v1/namespaces/{namespace}/tables"), json=data).json()

    def get_table(self, name, namespace=None):
        namespace = namespace or self.namespace
        return self.session.get(self.url(f"/v1/namespaces/{namespace}/tables/{name}")).json()

    def delete_table(self, name, namespace=None, purge=True):
        namespace = namespace or self.namespace
        data = {"purgeRequested": purge}
        return self.session.delete(self.url(f"/v1/namespaces/{namespace}/tables/{name}"),
                                   json=data).json()


def _get_api():
    import yaml
    import fsspec
    with fsspec.open("https://github.com/apache/iceberg/raw/master/"
                     "open-api/rest-catalog-open-api.yaml") as f:
        return yaml.safe_load(f.read())


api = [None]


def _get_def(path):
    if api[0] is None:
        api[0] = _get_api()
    parts = path.strip("#").strip("/").split("/")
    out = api[0]
    for part in parts:
        out = out[part]
    return out