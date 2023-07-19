from fastapi import FastAPI
import redis
import pandas as pd
import tempfile
import logging
import os
import urllib.request
import uuid
from poc_redis_fastapi_chemblntd.chemblntd import Chembtlntd
from redis_om import NotFoundError, Migrator

# logger setup
logging.basicConfig(level=logging.INFO, format="%(name)s - %(levelname)s - %(message)s")

# get root logger
logger = logging.getLogger(__name__)

# init Fast Api & Redis
app = FastAPI()
# for running in docker compose
REDIS_HOST = os.environ.get("REDIS_HOST", "127.0.0.1")
r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)

# Constants
CSV_URL = "ftp://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLNTD/set7_harvard_liver/Harvard_ALL.csv"
TEMP_DIR = tempfile.gettempdir()
TEMP_FILE_LOC = f"{TEMP_DIR}/{str(uuid.uuid4())}.csv"
REDIS_OBJ = "chemblntd"


def remove_file(path: str):
    try:
        os.remove(path)
    except OSError:
        logger.warning(f"File {path} can not be removed.")
        pass


@app.get("/chemblntd/hash")
async def get_indices():
    identifier = "valid hashes"
    keys = r.keys()
    hashes = []
    if len(keys) != 0:
        hashes = [k.split(":")[2] for k in keys]
        hashes.sort()

    return {"chemblntd": identifier, "data": hashes}


@app.get("/chemblntd/hash/{hash}")
async def get_chemblntd(hash: str):
    try:
        result = Chembtlntd.get(hash)
        return result.dict()
    except NotFoundError:
        return {}


@app.get("/chemblntd/search/cid/{cid}")
async def search_cid_all(cid: str):
    try:
        results = Chembtlntd.find(Chembtlntd.cid == cid).all()
    except NotFoundError:
        return []

    return [r.dict() for r in results]


@app.get("/chemblntd/search/sid/{sid}")
async def search_sid_all(sid: str):
    try:
        results = Chembtlntd.find(Chembtlntd.sid == sid).all()
    except NotFoundError:
        return []

    return [r.dict() for r in results]


@app.get("/chemblntd/search/smiles/{smiles}")
async def search_smiles(smiles: str):
    try:
        results = Chembtlntd.find(Chembtlntd.sid_smiles % smiles).all()
    except NotFoundError:
        return []

    return [r.dict() for r in results]


@app.post("/refresh")
async def refresh():
    # ping redis db
    try:
        logger.info("Pinging redis db.")
        r.ping()
        logger.info("Redis db is up.")
    except redis.exceptions.ConnectionError as e:
        logger.error("Redis db is down. Exiting.")
        logger.error(e)
        return

    # download csv file to init db
    try:
        logger.info("Downloading data.")
        info = urllib.request.urlretrieve(CSV_URL, TEMP_FILE_LOC)
        assert info[0] == TEMP_FILE_LOC
        assert os.path.exists(info[0])
    except AssertionError as e:
        logger.error(f"Temp file {TEMP_FILE_LOC} does not exist or is not correct.")
        logger.error(e)
        remove_file(info[0])
        remove_file(TEMP_FILE_LOC)
        return
    except Exception as e:
        logger.error(f"Error downloading file {CSV_URL}.")
        logger.error(e)
        remove_file(TEMP_FILE_LOC)
        return

    # load data into pandas df
    logger.info("Loading data into pandas df.")
    df = pd.read_csv(info[0], header=0, sep=",", index_col=False)

    # clean df column names
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("(", "")
        .str.replace(")", "")
        .str.replace("%", "pct")
    )
    logger.info(f"Dataframe shape: {df.shape}.")

    # clear redis db
    try:
        logger.info("Flushing redis db.")
        r.flushdb()
    except Exception as e:
        logger.error("Error flushing redis db. Exiting.")
        logger.error(e)
        remove_file(TEMP_FILE_LOC)
        return

    # load csv file into redis db
    n, cycle = 0, 0
    for i, row in df.iterrows():
        x = Chembtlntd(**row.to_dict())
        x.save()
        n += 1
        if (n % 64) == 0:
            cycle += 1
            logger.info(f"{cycle} cycles executed. Total rows: {n}.")

    logger.info(
        f"Final data load into Redis: {cycle} cycles executed. Total rows: {n}."
    )

    # create indices
    # Before running queries, we need to run migrations to set up the
    # indexes that Redis OM will use. You can also use the `migrate`
    # CLI tool for this!
    logger.info("Creating indices.")
    Migrator().run()

    logger.info("Startup event finished.")
    remove_file(TEMP_FILE_LOC)
    return {"status": "ok", "message": f"Data loaded into redis db. Total rows: {n}."}


@app.on_event("startup")
async def startup_event():
    # ping redis db
    try:
        logger.info("Pinging redis db.")
        r.ping()
        logger.info("Redis db is up.")
    except redis.exceptions.ConnectionError as e:
        logger.error("Redis db is down. Exiting.")
        logger.error(e)
        return
