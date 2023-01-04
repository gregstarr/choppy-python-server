import requests
import logging
import sys
import asyncio
import pathlib
import yaml
import tarfile
from pychop_server import settings


def configure_logging():
    logging.basicConfig(
        datefmt="%Y-%m-%dT%H:%M:%S",
        format="%(asctime)s | %(levelname)s | %(message)s",
        level=logging.INFO,
        stream=sys.stdout,
    )


async def run_chopper(job_info):
    config = {
        "beam_width": 2,
        "connector_diameter": 5,
        "connector_spacing": 10,
        "connector_tolerance": job_info["tolerance"],
        "mesh": str(job_info["local_mesh_path"].absolute()),
        "printer_extents": job_info["printer_size"],
        "directory": str(job_info["local_job_dir"].absolute()),
        "name": pathlib.Path(job_info["file_name"]).stem,
        "scale_factor": 1,
    }
    with open(job_info["cfg_file_path"], "w") as f:
        yaml.dump(config, f)
    command = (
        f"conda run -n pychop3d pychop -c {str(job_info['cfg_file_path'].absolute())}"
    )
    logging.info("running command:")
    logging.info(command)
    process = await asyncio.create_subprocess_shell(
        command, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE
    )
    out, err = await process.communicate()
    logging.info("OUT:")
    logging.info(out)
    logging.info("ERR:")
    logging.info(err)


output_dir = pathlib.Path(__file__).parent.parent / "output"


def collect_job_info(response):
    job_id = response.headers["job_id"]
    filename = response.headers["file_name"]
    info = {
        "name": response.headers["name"],
        "email": response.headers["email"],
        "printer_size": [float(d) for d in response.headers["printer_size"].split(",")],
        "tolerance": float(response.headers["tolerance"]),
        "file_name": filename,
        "remote_mesh_path": response.headers["local_path"],
        "job_id": job_id,
        "client_ip_addr": response.headers["remote_addr"],
        "local_job_dir": output_dir / job_id,
        "local_mesh_path": output_dir / job_id / filename,
        "cfg_file_path": output_dir / job_id / "config.yaml",
        "mesh_name": pathlib.Path(filename).stem,
    }
    info["local_job_dir"].mkdir()
    with open(info["local_mesh_path"], "wb") as f:
        f.write(response.content)
    logging.info("job info:")
    logging.info(info)
    return info


def prepare_archive(job_info):
    mesh_name = job_info["mesh_name"]
    tar_path = job_info["local_job_dir"] / f"{job_info['job_id']}.tar.gz"
    with tarfile.open(tar_path, "w:gz") as tar:
        for stlfile in job_info["local_job_dir"].glob(
            f"{mesh_name}*/{mesh_name}_part*.stl"
        ):
            tar.add(stlfile)
    return tar_path


async def poll_and_run(url):
    logging.info("sending request")
    response = requests.get(f"{url}/poll", headers={"Key": settings.SECRET_KEY})
    if response.status_code != 200:
        logging.info("status code != 200")
        return
    if response.content == b"0\n":
        logging.info("empty job queue")
        return
    logging.info("non-empty job queue")
    job_info = collect_job_info(response)
    await run_chopper(job_info)
    logging.info("finished")
    tar_path = prepare_archive(job_info)
    response = requests.post(
        f"{url}/job_page/{job_info['job_id']}/upload",
        headers={"Key": settings.SECRET_KEY},
        files={"file": open(tar_path, "rb")},
    )
    result = response.json()
    logging.info(result)


async def main():
    configure_logging()
    if settings.DEBUG:
        url = "http://127.0.0.1:5000"
    else:
        url = "https://www.pychop.xyz"
    tasks = set()
    while True:
        logging.info(f"{len(tasks)=}")
        if len(tasks) < settings.WORKERS:
            logging.info("creating task")
            task = asyncio.create_task(poll_and_run(url))
            tasks.add(task)
            task.add_done_callback(tasks.discard)
        await asyncio.sleep(settings.POLL_PERIOD)


if __name__ == "__main__":
    asyncio.run(main())
