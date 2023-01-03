import requests
import logging
import sys
import asyncio
import pathlib
import yaml
from pychop_server import settings


def configure_logging():
    logging.basicConfig(
        datefmt="%Y-%m-%dT%H:%M:%S",
        format="%(asctime)s | %(levelname)s | %(message)s",
        level=logging.INFO,
        stream=sys.stdout
    )


async def run_chopper(job_info):
    config = {
        "beam_width": 2,
        "connector_diameter": 5,
        "connector_spacing": 10,
        "connector_tolerance": job_info["tolerance"],
        "mesh": str(job_info["local_path"].absolute()),
        "printer_extents":  job_info["printer_size"],
        "directory": str(job_info["local_path"].parent.absolute()),
        "name": pathlib.Path(job_info["file_name"]).stem,
        "scale_factor": 1,
    }
    cfg_file = pathlib.Path(config["directory"]) / "config.yaml"
    with open(cfg_file, "w") as f:
        yaml.dump(config, f)
    command = f"conda run -n pychop3d pychop -c {str(cfg_file.absolute())}"
    logging.info("running command:")
    logging.info(command)
    process = await asyncio.create_subprocess_shell(
        command,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE
    )
    out, err = await process.communicate()
    logging.info("OUT:")
    logging.info(out)
    logging.info("ERR:")
    logging.info(err)


output_dir = pathlib.Path(__file__).parent.parent / "output"


def collect_job_info(response):
    info = {
        "name": response.headers["name"],
        "email": response.headers["email"],
        "printer_size": [float(d) for d in response.headers["printer_size"].split(",")],
        "tolerance": float(response.headers["tolerance"]),
        "file_name": response.headers["file_name"],
        "remote_path": response.headers["local_path"],
        "job_id": response.headers["job_id"],
        "remote_addr": response.headers["remote_addr"],
    }
    local_dir = output_dir / info["job_id"]
    local_dir.mkdir()
    info["local_path"] = local_dir / info["file_name"]
    with open(info["local_path"], "wb") as f:
        f.write(response.content)
    logging.info("job info:")
    logging.info(info)
    return info


async def poll_and_run(url):
    logging.info("sending request")
    response = requests.get(url, headers={"Key": settings.SECRET_KEY})
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


async def main():
    configure_logging()
    if settings.DEBUG:
        url = "http://127.0.0.1:5000/poll"
    else:
        url = "https://www.pychop.xyz/poll"
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
