import pathlib
import requests


resources = pathlib.Path(__file__).parent / "test_data"


def debug_upload():
    client = requests.session()
    stlfile = resources / "bunny.stl"
    url = "http://127.0.0.1:5000"
    resp = client.get(url)
    form_data = {
        "name": "greg",
        "email": "greg@greg.com",
        "printer_x": "200",
        "printer_y": "200",
        "printer_z": "200",
        "tolerance": "0.5",
    }
    resp = client.post(url, data=form_data, files={"stlfile": (str(stlfile.absolute()), open(stlfile, "rb"))})
    return resp


if __name__ == "__main__":
    r = debug_upload()
    print(r.content)
    