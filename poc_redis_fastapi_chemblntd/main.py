import sys
import traceback
import types
from typing import Literal, Optional, Type
from fastapi import FastAPI
from fastapi.params import Path
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import redis
import pandas as pd
import tempfile
import logging
import os
import urllib.request
import uuid
from redis_om import NotFoundError, Migrator, HashModel, Field

from poc_redis_fastapi_chemblntd.chemblntd import ChembtlntdRedis
from poc_redis_fastapi_chemblntd.helpers import class_with_types, random_value
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
    level=logging.INFO, format="%(levelname)s:\t  %(name)s [%(lineno)s] - %(message)s"
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
TYPE_ANNOTATIONS = {
    "int": int,
    "float": float,
    "str": str,
    "bool": bool,
}


def remove_file(path: str) -> None:
    """Delete a file if it exists.

    Args:
        path (str): The path to the file to delete.

    Examples:
        >>> remove_file("foo.txt")
    """

    try:
        os.remove(path)
    except OSError:
        logger.warning(f"File {path} can not be removed.")
        pass


def ping_redis() -> bool:
    """Ping the Redis database to check that it is up.

    Uses the `ping` method of the `redis` package to check that the Redis database is up.

    Returns:
        bool: True if the Redis database is up, False otherwise.

    Examples:
        >>> ping_redis()
        True
    """
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
    """Flush the Redis database.

    Returns:
        bool: True if the Redis database is flushed, False is there is an Exception thrown.

    Examples:
        >>> flush_redis()
        True
    """
    try:
        logger.info("Flushing redis db.")
        r.flushdb()
        return True
    except Exception as e:
        logger.error("Error flushing redis db. Exiting.")
        logger.error(e)
        return False


def temp_location() -> str:
    return f"{TEMP_DIR}/{str(uuid.uuid4())}.csv"


def is_float(s: str) -> bool:
    """Check if a string is a float.

    Args:
        s (str): The string to check.

    Returns:
        bool: True if the string is a float, False otherwise.

    Examples:
        >>> is_float("1.0")
        True
        >>> is_float("1")
        False
    """
    try:
        float(s)
        return True
    except ValueError:
        return False


def is_int(s: str) -> bool:
    """Check if a string is an integer.

    Args:
        s (str): The string to check.

    Returns:
        bool: True if the string is an integer, False otherwise.

    Examples:
        >>> is_int("1")
        True
        >>> is_int("1.0")
        False
    """
    try:
        return float(s).is_integer()
    except ValueError:
        return False


def is_bool(s: str) -> bool:
    """Check if a string is a boolean.

    Args:
        s (str): The string to check.

    Returns:
        bool: True if the string is a boolean, False otherwise.

    Examples:
        >>> is_bool("True")
        True
        >>> is_bool("1")
        False
    """
    if s.lower() in ["true", "false"]:
        return True
    else:
        return False


def geuss_schema(df: pd.DataFrame, sample_size: int = 25) -> dict[str, Type]:
    """Geuss the schema of a pandas DataFrame.

    Args:
        df (pd.DataFrame): The pandas DataFrame to geuss the schema of.

    Returns:
        dict[str, Type]: A dictionary of the column names and their associated types.

    Examples:
        >>> df = pd.DataFrame({"a": [1, 2, 3], "b": [1.0, 2.0, 3.0]})
        >>> geuss_schema(df)
        {"a": int, "b": float}
    """
    schema = {}
    first_rows = df.head(sample_size)

    for col in first_rows.columns:
        c = first_rows[col].to_list()

        if first_rows[col].dtype == "int64" or all(is_int(e) for e in c):
            schema[col] = int
        elif first_rows[col].dtype == "float64" or all(is_float(e) for e in c):
            schema[col] = float
        elif first_rows[col].dtype == "object":
            schema[col] = str
        elif first_rows[col].dtype == "bool" or all(is_bool(e) for e in c):
            schema[col] = bool
        else:
            schema[col] = str

    return schema


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the column names of a pandas DataFrame.

    Args:
        df (pd.DataFrame): The pandas DataFrame to clean the column names of.

    Returns:
        pd.DataFrame: The pandas DataFrame with the cleaned column names.

    Examples:
        >>> df = pd.DataFrame({"A": [1, 2, 3], "B": [1.0, 2.0, 3.0]})
        >>> clean_column_names(df)
           a    b
        0  1  1.0
        1  2  2.0
        2  3  3.0
    """
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("(", "")
        .str.replace(")", "")
        .str.replace("%", "pct")
    )

    return df


class RefreshHashModelInfo(BaseModel):
    type: Literal["int", "float", "str", "bool"]
    index: bool = False
    full_text_search: bool = False


class RefreshBody(BaseModel):
    url: str
    custom_schema: Optional[dict[str, RefreshHashModelInfo]]  # key = column name


@app.post("/")
async def dynamic_refresh(directions: RefreshBody):
    """
    Refreshes the Redis database with the latest ChEMBL-NTD data.
    """
    # ping redis db
    if not ping_redis():
        return JSONResponse(
            status_code=500, content={"message": "Internal server error"}
        )

    if not directions.url.endswith(".csv"):
        return JSONResponse(
            status_code=404, content={"message": "Only csv files are supported"}
        )

    # download csv file to init db
    try:
        logger.info("Downloading data.")
        # TODO: get temp file loc dynamically
        temp_file_loc = temp_location()
        info = urllib.request.urlretrieve(directions.url, temp_file_loc)
        assert info[0] == temp_file_loc
        assert os.path.exists(info[0])
    except AssertionError as e:
        logger.error(f"Temp file {temp_file_loc} does not exist or is not correct.")
        logger.error(e)
        remove_file(info[0])
        remove_file(temp_file_loc)
        return JSONResponse(
            status_code=500, content={"message": "Internal server error"}
        )
    except Exception as e:
        logger.error(f"Error downloading file {directions.url}.")
        logger.error(e)
        remove_file(temp_file_loc)
        return JSONResponse(
            status_code=500, content={"message": "Internal server error"}
        )

    # load data into pandas df & clean
    logger.info("Loading data into pandas df.")
    df = pd.read_csv(info[0], header=0, sep=",", index_col=False)
    df = clean_column_names(df)
    logger.info(f"Dataframe columns: {df.columns}.")
    logger.info(f"Dataframe shape: {df.shape}.")

    # get new class based on attributes
    models = {
        "hash": {"name": "CsvHashModel", "model": HashModel, "instance": None},
        "base": {"name": "CsvBaseModel", "model": BaseModel, "instance": None},
    }

    try:
        for k, d in models.items():
            # ref: https://stackoverflow.com/questions/59412427/set-module-on-class-created-with-types-new-class
            # ref: https://python-course.eu/oop/dynamically-creating-classes-with-type.php

            # add dynamic class attribute annotations
            class_body = {
                "__annotations__": {
                    col: TYPE_ANNOTATIONS[t.type]
                    for col, t in directions.custom_schema.items()
                },
            }

            # init with field values if hash
            if k == "hash":
                for col, t in directions.custom_schema.items():
                    if t.index:
                        class_body[col] = Field(
                            index=t.index, full_text_search=t.full_text_search
                        )
            elif k == "base":
                class_body["model_config"] = {
                    "json_schema_extra": {
                        "examples": [
                            {
                                col: random_value(8, t.type)
                                for col, t in directions.custom_schema.items()
                            }
                        ]
                    }
                }

            models[k]["instance"] = types.new_class(
                d["name"],
                (d["model"],),
                {},
                lambda ns: ns.update(class_body),
            )
    except Exception:
        logger.error(traceback.format_exc())
        del df
        remove_file(temp_file_loc)
        return JSONResponse(
            status_code=500, content={"message": "Internal server error"}
        )

    # flush redis db
    if not flush_redis():
        remove_file(temp_file_loc)
        del df
        return JSONResponse(
            status_code=500, content={"message": "Internal server error"}
        )

    # load data into redis db
    for line_num, (i, row) in enumerate(df.iterrows()):
        x = models["hash"]["instance"](**row.to_dict())  # Upload to Redis
        x.save()
        # logger.info(msg=x.dict())
        # break

    del df
    remove_file(temp_file_loc)


class GetColumnsBody(BaseModel):
    url: str


@app.post("/columns")
async def geuss_columns(body: GetColumnsBody):
    if not body.url.endswith(".csv"):
        return JSONResponse(
            status_code=404, content={"message": "Only csv files are supported"}
        )

    # download csv file to init db
    try:
        logger.info("Downloading data.")
        # TODO: get temp file loc dynamically
        temp_file_loc = temp_location()
        info = urllib.request.urlretrieve(body.url, temp_file_loc)
        assert info[0] == temp_file_loc
        assert os.path.exists(info[0])
    except AssertionError as e:
        logger.error(f"Temp file {temp_file_loc} does not exist or is not correct.")
        logger.error(e)
        remove_file(info[0])
        remove_file(temp_file_loc)
        return JSONResponse(
            status_code=500, content={"message": "Internal server error"}
        )
    except Exception as e:
        logger.error(f"Error downloading file {body.url}.")
        logger.error(e)
        remove_file(temp_file_loc)
        return JSONResponse(
            status_code=500, content={"message": "Internal server error"}
        )

    # load data into pandas df & clean
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

    s = geuss_schema(df)
    for k, v in s.items():
        s[k] = v.__name__

    msg = {"guessed_schema": s}
    del df
    remove_file(temp_file_loc)

    return JSONResponse(status_code=200, content=msg)


@app.get(
    "/chemblntd/hash",
    response_model=Hashes,
    responses={500: {"message": "Internal server error"}},
)
async def get_hashes():
    """
    Returns a list of all valid hash ids in the Redis database.
    """
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
async def get_chemblntd(
    hash: str = Path(
        title="The hash key in Redis",
        description="The identifier for which to identify an entry in the associated Redis Db.",
        example="01H5T42K2AEA7682MR9XD7Y2S4",
    )
):
    """
    Returns the ChEMBL-NTD entry for the given hash id in the Redis database.
    """
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
async def search_by_cid(
    cid: str = Path(
        title="PubChem CID",
        description="The identifier of the molecule in PubChem",
        example="54735847.0",
    )
):
    """
    Returns a list of ChEMBL-NTD entries for the given CID (Compound) in the Redis database.
    """
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
async def search_by_sid(
    sid: str = Path(
        title="PubChem SID",
        description="The identifier of the substance in PubChem",
        example="121363756",
    )
):
    """
    Returns a list of ChEMBL-NTD entries for the given SID (Substance) in the Redis database.
    """
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
    """
    Returns a list of ChEMBL-NTD entries that containe the given SMILES string in the Redis database.
    """
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
    """
    Refreshes the Redis database with the latest ChEMBL-NTD data.
    """
    # ping redis db
    if not ping_redis():
        return JSONResponse(
            status_code=500, content={"message": "Internal server error"}
        )

    # download csv file to init db
    try:
        logger.info("Downloading data.")
        temp_file_loc = temp_location()
        info = urllib.request.urlretrieve(CSV_URL, temp_file_loc)
        assert info[0] == temp_file_loc
        assert os.path.exists(info[0])
    except AssertionError as e:
        logger.error(f"Temp file {temp_file_loc} does not exist or is not correct.")
        logger.error(e)
        remove_file(info[0])
        remove_file(temp_file_loc)
        return JSONResponse(
            status_code=500, content={"message": "Internal server error"}
        )
    except Exception as e:
        logger.error(f"Error downloading file {CSV_URL}.")
        logger.error(e)
        remove_file(temp_file_loc)
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
        remove_file(temp_file_loc)
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
        remove_file(temp_file_loc)
        del df
        return JSONResponse(
            status_code=500, content={"message": "Internal server error"}
        )

    logger.info("Refresh complete.")
    remove_file(temp_file_loc)
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
