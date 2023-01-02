import requests
import logging
import time
from pychop_server.settings import POLL_PERIOD, SECRET_KEY, DEBUG


def configure_logging():
    logging.basicConfig(
        datefmt="%Y-%m-%dT%H:%M:%S",
        format='%(asctime)s | %(levelname)s | %(message)s',
        level=logging.INFO,
        stream=logging.StreamHandler()
    )


def queue_job():
    ...


def poll(url):
    logging.info("sending request")
    response = requests.get(url, headers={"Key": SECRET_KEY})
    if response.status_code != 200:
        logging.info("status code != 200")
        return
    if response.content == b"0\n":
        logging.info("empty job queue")
        return
    
    logging.info("non-empty job queue")
    queue_job()
    

def main():
    if DEBUG:
        url = "127.0.0.1:5000/poll"
    else:
        url = "https://www.pychop.xyz/poll"
    while True:
        poll(url)
        time.sleep(POLL_PERIOD)


if __name__ == "__main__":
    main()
