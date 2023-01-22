from environs import Env

env = Env()
env.read_env()

SECRET_KEY = env.str("SECRET_KEY")
POLL_PERIOD = 10
DEBUG = env.str("DEBUG") == '1'
if DEBUG:
    SECRET_KEY = "not-so-secret-in-tests"
WORKERS = 8
