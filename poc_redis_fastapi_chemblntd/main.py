import json
from fastapi import FastAPI
from fastapi.params import Path
from fastapi.responses import JSONResponse
from pydantic import Field
import redis
import pandas as pd
import tempfile
import logging
import os
import urllib.request
import uuid
from redis_om import NotFoundError, Migrator

from poc_redis_fastapi_chemblntd.chemblntd import ChembtlntdRedis
from poc_redis_fastapi_chemblntd.openapi import (
    Hashes,
    Items,
    Message,
    SmilesBody,
    responses,
    Item,
)

# logger setup
logging.basicConfig(
    level=logging.INFO, format="%(levelname)s:\t  %(name)s - %(message)s"
)

# get root logger
logger = logging.getLogger(__name__)

# init fastapi
app = FastAPI()

# init redis
REDIS_HOST = os.environ.get("REDIS_HOST", "127.0.0.1")  # for docker-compose
r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)

# Constants
CSV_URL = "ftp://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLNTD/set7_harvard_liver/Harvard_ALL.csv"
TEMP_DIR = tempfile.gettempdir()
TEMP_FILE_LOC = f"{TEMP_DIR}/{str(uuid.uuid4())}.csv"


def remove_file(path: str):
    try:
        os.remove(path)
    except OSError:
        logger.warning(f"File {path} can not be removed.")
        pass


def ping_redis() -> bool:
    try:
        logger.info("Pinging redis db.")
        r.ping()
        logger.info("Redis db is up.")
        return True
    except redis.exceptions.ConnectionError as e:
        logger.error("Redis db is down. Exiting.")
        logger.error(e)
        return False


def flush_redis() -> bool:
    try:
        logger.info("Flushing redis db.")
        r.flushdb()
        return True
    except Exception as e:
        logger.error("Error flushing redis db. Exiting.")
        logger.error(e)
        return False


@app.get(
    "/chemblntd/hash",
    response_model=Hashes,
    responses={500: {"message": "Internal server error"}},
)
async def get_hashes():
    try:
        keys = r.keys()
        hashes = []
        if len(keys) != 0:
            hashes = [k.split(":")[2] for k in keys]
            hashes.sort()

        logger.info(f"Number of hashes: {len(hashes)}")
        msg = {"hashes": hashes}

        return JSONResponse(status_code=200, content=msg)
    except Exception as e:
        logger.error(e)
        return JSONResponse(
            status_code=500, content={"message": "Internal server error"}
        )


@app.get("/chemblntd/hash/{hash}", response_model=Item, responses={**responses})
async def get_chemblntd(hash: str = Path(example="01H5T42K2AEA7682MR9XD7Y2S4")):
    try:
        result = ChembtlntdRedis.get(hash)
        msg = {"id": hash, "value": result.dict()}

        return JSONResponse(status_code=200, content=msg)
    except NotFoundError:
        return JSONResponse(status_code=404, content={"message": "Item not found"})
    except Exception as e:
        logger.error(e)
        return JSONResponse(
            status_code=500, content={"message": "Internal server error"}
        )


@app.get("/chemblntd/search/cid/{cid}", response_model=Items, responses={**responses})
async def search_by_cid(cid: str = Path(example="54735847.0")):
    try:
        results = ChembtlntdRedis.find(ChembtlntdRedis.cid == cid).all()
        msg = {"id": cid, "value": results.dict()}

        return JSONResponse(status_code=200, content=msg)
    except NotFoundError:
        return JSONResponse(status_code=404, content={"message": "Item not found"})
    except Exception as e:
        logger.error(e)
        return JSONResponse(
            status_code=500, content={"message": "Internal server error"}
        )


@app.get("/chemblntd/search/sid/{sid}", response_model=Items, responses={**responses})
async def search_by_sid(sid: str = Path(example="121363756")):
    try:
        results = ChembtlntdRedis.find(ChembtlntdRedis.sid == sid).all()
        msg = {"id": sid, "value": results.dict()}

        return JSONResponse(status_code=200, content=msg)
    except NotFoundError:
        return JSONResponse(status_code=404, content={"message": "Item not found"})
    except Exception as e:
        logger.error(e)
        return JSONResponse(
            status_code=500, content={"message": "Internal server error"}
        )


# TODO: fix json decode errors for examples like "("
@app.post("/chemblntd/search/smiles/", response_model=Items, responses={**responses})
async def search_by_smiles(smiles: SmilesBody):
    try:
        results = ChembtlntdRedis.find(ChembtlntdRedis.sid_smiles % smiles.smiles).all()
        msg = {"id": smiles.smiles, "value": [r.dict() for r in results]}

        return JSONResponse(status_code=200, content=msg)
    except NotFoundError:
        return JSONResponse(status_code=404, content={"message": "Item not found"})
    except Exception as e:
        logger.error(e)
        return JSONResponse(
            status_code=500, content={"message": "Internal server error"}
        )


@app.post(
    "/refresh",
    response_model=Message,
    responses={
        200: {"message": "Refresh complete. Data loaded into redis db."},
        500: {"message": "Internal server error"},
    },
)
async def refresh():
    # ping redis db
    if not ping_redis():
        return JSONResponse(
            status_code=500, content={"message": "Internal server error"}
        )

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
        return JSONResponse(
            status_code=500, content={"message": "Internal server error"}
        )
    except Exception as e:
        logger.error(f"Error downloading file {CSV_URL}.")
        logger.error(e)
        remove_file(TEMP_FILE_LOC)
        return JSONResponse(
            status_code=500, content={"message": "Internal server error"}
        )

    # load data into pandas df & clean
    # TODO: exception handling here
    logger.info("Loading data into pandas df.")
    df = pd.read_csv(info[0], header=0, sep=",", index_col=False)
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("(", "")
        .str.replace(")", "")
        .str.replace("%", "pct")
    )
    logger.info(f"Dataframe shape: {df.shape}.")

    # flush redis db
    if not flush_redis():
        remove_file(TEMP_FILE_LOC)
        del df
        return JSONResponse(
            status_code=500, content={"message": "Internal server error"}
        )

    # load data into redis db
    # TODO: exception handling and handle objects not added
    for line_num, (i, row) in enumerate(df.iterrows()):
        x = ChembtlntdRedis(**row.to_dict())
        x.save()
        if ((line_num + 1) % 100) == 0:
            logger.info(
                f"Total rows: {line_num+1}. ({round(100*(line_num + 1)/len(df), 1)})%"
            )
    logger.info(f"Final data load into Redis: {len(df)} total rows.")

    # create indices by running Migrate for Redis OM
    try:
        logger.info("Creating indices.")
        Migrator().run()
    except Exception as e:
        logger.error("Error creating indices.")
        logger.error(e)
        remove_file(TEMP_FILE_LOC)
        del df
        return JSONResponse(
            status_code=500, content={"message": "Internal server error"}
        )

    logger.info("Refresh complete.")
    remove_file(TEMP_FILE_LOC)
    del df

    return JSONResponse(
        status_code=200,
        content={"message": f"Refresh complete. Data loaded into redis db."},
    )


@app.on_event("startup")
async def startup_event():
    # ping redis db
    if not ping_redis():
        return
