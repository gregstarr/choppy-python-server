import requests
import re
import logging
import asyncio
import pathlib
import yaml
import tarfile
import typing
from pydantic import BaseModel
import uuid
from pychop_server import settings


def configure_logging():
    logging.basicConfig(
        datefmt="%Y-%m-%dT%H:%M:%S",
        format="%(asctime)s | %(levelname)s | %(message)s",
        level=logging.INFO,
        handlers=[logging.StreamHandler()],
    )


async def run_chopper(job_info):
    config = {
        "beam_width": 2,
        "connector_diameter": 5,
        "connector_spacing": 10,
        "connector_tolerance": job_info.tolerance,
        "mesh": str(job_info.local_mesh_path.absolute()),
        "printer_extents": list(job_info.printer_size),
        "directory": str(job_info.local_job_dir.absolute()),
        "name": pathlib.Path(job_info.file_name).stem,
        "scale_factor": 1,
    }
    with open(job_info.cfg_file_path, "w") as f:
        yaml.dump(config, f)
    command = (
        f"conda run -n pychop3d pychop -c {str(job_info.cfg_file_path.absolute())}"
    )
    logging.info("running command:")
    logging.info(command)
    process = await asyncio.create_subprocess_shell(command, stdout=asyncio.subprocess.PIPE)
    await process.communicate()


output_dir = pathlib.Path(__file__).parent.parent / "output"


class JobInfo(BaseModel):
    name: str
    email: str
    printer_size: typing.Tuple[float, float, float]
    tolerance: float
    file_name: str
    remote_mesh_path: str
    job_id: uuid.UUID
    client_ip_addr: str
    local_job_dir: pathlib.Path
    local_mesh_path: pathlib.Path
    cfg_file_path: pathlib.Path
    mesh_name: str


def collect_job_info(response):
    job_id = response.headers["job_id"]
    filename = response.headers["file_name"]
    info = JobInfo(
        name=response.headers["name"],
        email=response.headers["email"],
        printer_size=[d for d in response.headers["printer_size"].split(",")],
        tolerance=response.headers["tolerance"],
        file_name=filename,
        remote_mesh_path=response.headers["local_path"],
        job_id=job_id,
        client_ip_addr=response.headers["remote_addr"],
        local_job_dir=output_dir / job_id,
        local_mesh_path=output_dir / job_id / filename,
        cfg_file_path=output_dir / job_id / "config.yaml",
        mesh_name=pathlib.Path(filename).stem,
    )
    info.local_job_dir.mkdir()
    with open(info.local_mesh_path, "wb") as f:
        f.write(response.content)
    logging.info("job info:")
    logging.info(info)
    return info


class JobHandler:
    job_info: JobInfo
    bytes_read: int = 0
    log_file: pathlib.Path
    url: str
    status: typing.Dict[str, typing.List[float]]
    
    def __init__(self, url, job_info) -> None:
        self.job_info = job_info
        self.url = url
        self.status = {}
        self.log_file = None

    def parse_logs(self):
        logging.info("checking logs")
        if self.log_file.stat().st_size < (self.bytes_read + 100):
            logging.info("log file didn't change")
            return

        with self.log_file.open() as f:
            f.seek(self.bytes_read)
            log_text = f.read()
        matches = re.findall(r"\$(\S+) (.+)\n", log_text)
        if not matches:
            logging.info("no status updates")
            return
        for match in matches:
            key = match[0]
            val = match[1].split("/")
            val = val[0] if len(val) == 1 else val
            self.status[key] = val
        logging.info(f"{self.status=}")
        response = requests.post(
            f"{self.url}/job_page/{self.job_info.job_id}/update",
            headers={"Key": settings.SECRET_KEY},
            json=self.status,
        )
        if response.status_code != 200:
            logging.info("status code != 200")
            return
        result = response.json()
        logging.info(result)

    def prepare_archive(self):
        mesh_name = self.job_info.mesh_name
        tar_path = self.job_info.local_job_dir / f"{self.job_info.job_id}.tar.gz"
        with tarfile.open(tar_path, "w:gz") as tar:
            for stlfile in self.job_info.local_job_dir.glob(f"{mesh_name}*/{mesh_name}_part*.stl"):
                logging.info(stlfile)
                tar.add(stlfile, arcname=stlfile.name)
        return tar_path

    async def wait_for_log(self):
        logging.info("waiting for log")
        mesh_name = self.job_info.mesh_name
        while self.log_file is None:
            try:
                self.log_file = next(self.job_info.local_job_dir.glob(f"{mesh_name}*/info.log"))
            except StopIteration:
                pass
                await asyncio.sleep(3)

    async def run(self):
        task = asyncio.create_task(run_chopper(self.job_info))
        await self.wait_for_log()
        while not task.done():
            self.parse_logs()
            await asyncio.sleep(5)
        logging.info("finished")
        tar_path = self.prepare_archive()
        response = requests.post(
            f"{self.url}/job_page/{self.job_info.job_id}/upload",
            headers={"Key": settings.SECRET_KEY},
            files={"file": open(tar_path, "rb")},
        )
        if response.status_code != 200:
            logging.info("status code != 200")
            return
        result = response.json()
        logging.info(result)


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
    handler = JobHandler(url, job_info)
    await handler.run()


async def main():
    configure_logging()
    if settings.DEBUG:
        url = "http://127.0.0.1:5000"
    else:
        url = "https://pychop.xyz"
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
