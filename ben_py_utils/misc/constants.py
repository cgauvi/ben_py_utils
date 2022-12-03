
from os.path import join, dirname, isdir
from os import listdir, makedirs

# Root 
ROOT_PROJECT = join(dirname(__file__), "..")
assert isdir(ROOT_PROJECT)

# data dir 
DATA_DIR = join(ROOT_PROJECT, "data")
if not isdir(DATA_DIR):
    makedirs(DATA_DIR)

# cache
DEFAULT_CACHE = join(DATA_DIR, "cache")
if not isdir(DEFAULT_CACHE):
    makedirs(DEFAULT_CACHE)


if __name__ == "__main__":

    print(ROOT_PROJECT)