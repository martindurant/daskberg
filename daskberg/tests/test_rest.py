import os
import shlex
import shutil
import subprocess
import time

import pytest
import requests

from daskberg.ice import IcebergDataset
from daskberg.rest_client import IceRESTClient

loc = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))


def stop_docker(name):
    cmd = shlex.split('docker ps -a -q --filter "name=%s"' % name)
    cid = subprocess.check_output(cmd).strip().decode()
    if cid:
        subprocess.call(["docker", "rm", "-f", cid])


@pytest.fixture(scope="module")
def client():
    try:
        subprocess.check_call(
            ["docker", "run", "hello-world"], stdout=subprocess.DEVNULL
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        pytest.skip("docker run not available")
        return

    name = "ice"
    stop_docker(name)
    # dumps stuff in ./rest/ for now for ease of finding
    os.makedirs(f"{loc}/rest", exist_ok=True)
    cmd = f"docker run -d --name {name} -p 8181:8181 -v {loc}/rest:/tmp mdurant/ice:1"
    cid = subprocess.check_output(shlex.split(cmd)).strip().decode()
    timeout = 15
    while True:
        try:
            requests.get("http://localhost:8181")
            yield IceRESTClient("http://localhost:8181")
            break
        except Exception:
            time.sleep(0.5)
            timeout -= 0.5
            if timeout < 0:
                raise
    stop_docker(name)
    shutil.rmtree(f"{loc}/rest")


def test_namespaces(client):
    assert client.list_namespaces() == []

    client.create_namespace("freda")
    assert client.list_namespaces() == ["freda"]

    assert client.delete_namespace("freda")
    assert client.list_namespaces() == []


def test_table(client: IceRESTClient):
    example_schema = [
        {"id": 1, "name": "date", "required": False, "type": "date"},
        {"id": 2, "name": "symbol", "required": False, "type": "string"},
    ]
    client.create_namespace("fred")
    client.namespace = "fred"
    assert client.list_tables() == []
    client.create_table("test", example_schema)
    assert client.list_tables()
    out = client.get_table("test")
    assert out["metadata"]["schema"]["fields"] == example_schema
    berg = IcebergDataset(
        out["metadata-location"].replace("/tmp", loc + "/rest"),
        original_url=out["metadata-location"],
    )
    assert os.path.isdir(berg.url)
    with pytest.raises(ValueError):
        # because no snapshots yet
        berg.schema
