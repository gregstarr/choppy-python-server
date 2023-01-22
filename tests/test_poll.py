import pathlib
import subprocess
import logging
import pytest
import requests
import time
from choppy_back.server import poll


resources = pathlib.Path(__file__).parent / "test_data"


@pytest.fixture(scope="module")
def app_process():
    with subprocess.Popen(
        [
            "/home/greg/.conda/envs/pcs/bin/python",
            "-m",
            "flask",
            "--app",
            "/home/greg/code/choppy_front/choppy_front/app:create_app",
            "--debug",
            "run",
        ],
        stdout=subprocess.PIPE,
    ) as proc:
        time.sleep(2)
        yield proc
        time.sleep(2)
        proc.kill()


def test_poll_empty(app_process, caplog):
    caplog.set_level(logging.INFO)
    poll("http://127.0.0.1:5000/poll")
    assert caplog.messages.pop() == "empty job queue"


def test_poll_nonempty(app_process, caplog):
    caplog.set_level(logging.INFO)
    client = requests.session()
    stlfile = resources / "bunny.stl"
    url = "http://127.0.0.1:5000"
    resp = client.get(url)
    form_data = {
        "name": "greg",
        "email": "greg@greg.com",
        "printer_x": "100",
        "printer_y": "100",
        "printer_z": "100",
        "tolerance": "0.5",
    }
    resp = client.post(url, data=form_data, files={"stlfile": (str(stlfile.absolute()), open(stlfile, "rb"))})
    assert b"Success" in resp.content
    poll("http://127.0.0.1:5000/poll")
    assert caplog.messages.pop() == "non-empty job queue"
