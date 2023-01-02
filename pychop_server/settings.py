from environs import Env

env = Env()
env.read_env()

SECRET_KEY = env.str("SECRET_KEY")
POLL_PERIOD = env.str("POLL_PERIOD")
DEBUG = env.str("DEBUG")
if DEBUG:
    SECRET_KEY = "not-so-secret-in-tests"