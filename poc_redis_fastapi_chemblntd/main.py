import logging
import os
import tempfile
import traceback
import urllib.request

import pandas as pd
import redis
from fastapi import FastAPI
from fastapi.params import Path
from fastapi.responses import JSONResponse
from fastapi_pagination import Page, add_pagination, paginate
from redis_om import Field, Migrator, NotFoundError

from poc_redis_fastapi_chemblntd.chemblntd import ChembtlntdRedis
from poc_redis_fastapi_chemblntd.helpers import *
from poc_redis_fastapi_chemblntd.openapi import (
    GetColumnsBody,
    Item,
    Items,
    RefreshBody,
    SmilesBody,
    responses,
)

# logger setup
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s:\t  %(name)s [%(lineno)s] - %(message)s",
)

# get root logger
logger = logging.getLogger(__name__)

# init fastapi
app = FastAPI()

# add pagination
add_pagination(app)

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
CUSTOM_CLASSES = {  # type: ignore
    "hash": {},
    "base": {},
}
ACCEPTED_FILE_TYPES = ["csv", "tsv", "xlsx", "xls"]


@app.post("/columns")
async def geuss_columns(body: GetColumnsBody):
    if not any(body.url.endswith(e) for e in ACCEPTED_FILE_TYPES):
        accepted_files = ".".join(ACCEPTED_FILE_TYPES)
        return JSONResponse(
            status_code=404,
            content={
                "message": f"File type is not supported. Use one of the following: {accepted_files}",
            },
        )

    ending = body.url.split(".")[-1]

    # download csv file to init db
    try:
        logger.info("Downloading data.")
        temp_file_loc = temp_location(ending)
        info = urllib.request.urlretrieve(body.url, temp_file_loc)
        assert info[0] == temp_file_loc
        assert os.path.exists(info[0])
    except AssertionError as e:
        logger.error(f"Temp file {temp_file_loc} does not exist or is not correct.")
        logger.error(e)
        remove_file(info[0])
        remove_file(temp_file_loc)
        return JSONResponse(
            status_code=500,
            content={"message": "Internal server error"},
        )
    except Exception as e:
        logger.error(f"Error downloading file {body.url}.")
        logger.error(e)
        remove_file(temp_file_loc)
        return JSONResponse(
            status_code=500,
            content={"message": "Internal server error"},
        )

    # get encoding
    encoding = get_encoding(temp_file_loc)

    # load data into pandas df & clean
    logger.info("Loading data into pandas df.")
    if ending == "csv":
        df = pd.read_csv(info[0], header=0, sep=",", index_col=False, encoding=encoding)
    elif ending == "tsv":
        df = pd.read_table(
            info[0],
            header=0,
            sep="\t",
            index_col=False,
            encoding=encoding,
        )
    elif ending == "xlsx" or ending == "xls":
        df = pd.read_excel(info[0], header=0, index_col=False)

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
        s[k] = v.__name__  # type: ignore

    s = add_refresh_attributes(s)

    msg = {"guessed_schema": s, "url": body.url}
    del df
    remove_file(temp_file_loc)

    return JSONResponse(status_code=200, content=msg)


@app.get(
    "/hash",
    response_model=Page[str],
    responses={500: {"message": "Internal server error"}},
)
async def get_indices():
    """
    Returns a list of all valid indices in the Redis database.
    """
    try:
        keys = r.keys()
        indices = list({".".join(k.split(":")[1].split(".")[-2:]) for k in keys})

        logger.info(f"Available Indices: {indices}")
        # msg = {"indices": indices}
        return paginate(indices)
        # return JSONResponse(status_code=200, content=msg)
    except Exception as e:
        logger.error(e)
        return JSONResponse(
            status_code=500,
            content={"message": "Internal server error"},
        )


@app.get(
    "/hash/{index_name}",
    response_model=Page[str],
    responses={500: {"message": "Internal server error"}},
)
async def get_hashes(
    index_name: str = Path(
        title="The index name in Redis",
        description="The identifier for which to identify an entry in the associated Redis Db.",
        example="chemblntd",
    ),
):
    """
    Returns a list of all valid hash ids in the Redis database.
    """
    try:
        keys = r.keys()
        hashes = []
        if len(keys) != 0:
            hashes = [k.split(":")[2] for k in keys if k.split(":")[1].split(".")[-1] == index_name]
            hashes.sort()

        logger.info(f"Number of hashes: {len(hashes)}")
        return paginate(hashes)

        # return JSONResponse(status_code=200, content=msg)
    except Exception as e:
        logger.error(e)
        return JSONResponse(
            status_code=500,
            content={"message": "Internal server error"},
        )


@app.get("/hash/{index_name}/{hash}", response_model=Item, responses={**responses})
async def get_hash(
    index_name: str = Path(
        title="The index name in Redis",
        description="The identifier for which to identify an entry in the associated Redis Db.",
        example="chemblntd",
    ),
    hash: str = Path(
        title="The hash key in Redis",
        description="The identifier for which to identify an entry in the associated Redis Db.",
        example="01H5T42K2AEA7682MR9XD7Y2S4",
    ),
):
    """
    Returns the ChEMBL-NTD entry for the given hash id in the Redis database.
    """
    print(CUSTOM_CLASSES)
    if index_name not in CUSTOM_CLASSES["hash"]:
        return JSONResponse(404, {"message": "Index name not found."})
    try:
        result = CUSTOM_CLASSES["hash"][index_name]["instance"].get(hash)
        msg = {"id": hash, "name": index_name, "value": result.dict()}

        return JSONResponse(status_code=200, content=msg)
    except NotFoundError:
        return JSONResponse(status_code=404, content={"message": "Item not found"})
    except Exception as e:
        logger.error(e)
        return JSONResponse(
            status_code=500,
            content={"message": "Internal server error"},
        )


@app.get("/chemblntd/search/cid/{cid}", response_model=Items, responses={**responses})
async def search_by_cid(
    cid: str = Path(
        title="PubChem CID",
        description="The identifier of the molecule in PubChem",
        example="54735847.0",
    ),
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
            status_code=500,
            content={"message": "Internal server error"},
        )


@app.get("/chemblntd/search/sid/{sid}", response_model=Items, responses={**responses})
async def search_by_sid(
    sid: str = Path(
        title="PubChem SID",
        description="The identifier of the substance in PubChem",
        example="121363756",
    ),
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
            status_code=500,
            content={"message": "Internal server error"},
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
            status_code=500,
            content={"message": "Internal server error"},
        )


@app.post("/refresh")
async def dynamic_refresh(body: RefreshBody):
    """
    Refreshes the Redis database with the latest ChEMBL-NTD data.
    """
    # ping redis db
    if not ping_redis(r):
        return JSONResponse(
            status_code=500,
            content={"message": "Internal server error"},
        )

    # check url
    if not any(body.url.endswith(e) for e in ACCEPTED_FILE_TYPES):
        return JSONResponse(
            status_code=404,
            content={
                "message": f"File type is not supported. Use one of the following: {','.join(ACCEPTED_FILE_TYPES)}",
            },
        )

    ending = body.url.split(".")[-1]

    # download csv file to init db
    try:
        logger.info("Downloading data.")
        # TODO: get temp file loc dynamically
        temp_file_loc = temp_location("csv")
        info = urllib.request.urlretrieve(body.url, temp_file_loc)
        assert info[0] == temp_file_loc
        assert os.path.exists(info[0])
    except AssertionError as e:
        logger.error(f"Temp file {temp_file_loc} does not exist or is not correct.")
        logger.error(e)
        remove_file(info[0])
        remove_file(temp_file_loc)
        return JSONResponse(
            status_code=500,
            content={"message": "Internal server error"},
        )
    except Exception as e:
        logger.error(f"Error downloading file {body.url}.")
        logger.error(e)
        remove_file(temp_file_loc)
        return JSONResponse(
            status_code=500,
            content={"message": "Internal server error"},
        )

    # get encoding
    encoding = get_encoding(temp_file_loc)

    # load data into pandas df & clean
    logger.info("Loading data into pandas df.")
    if ending == "csv":
        df = pd.read_csv(info[0], header=0, sep=",", index_col=False, encoding=encoding)
    elif ending == "tsv":
        df = pd.read_table(
            info[0],
            header=0,
            sep="\t",
            index_col=False,
            encoding=encoding,
        )
    elif ending == "xlsx" or ending == "xls":
        df = pd.read_excel(info[0], header=0, index_col=False)

    df = clean_column_names(df)
    df = fill_na(df, {k: v.type for k, v in body.custom_schema.items()})  # type: ignore
    logger.info(f"Dataframe columns: {df.columns}.")
    logger.info(f"Dataframe shape: {df.shape}.")

    # setup dynamic classes for redis object model
    try:
        for k, d in CUSTOM_CLASSES.items():
            # ref: https://stackoverflow.com/questions/59412427/set-module-on-class-created-with-types-new-class
            # ref: https://python-course.eu/oop/dynamically-creating-classes-with-type.php

            # add dynamic class attribute annotations
            class_body = {
                "__annotations__": {
                    col: TYPE_ANNOTATIONS[t.type] for col, t in body.custom_schema.items()  # type: ignore
                },
            }

            # init with field values if hash
            if k == "hash":
                for col, t in body.custom_schema.items():  # type: ignore
                    if t.index:
                        class_body[col] = Field(
                            index=t.index,
                            full_text_search=t.full_text_search,
                        )
            # else add examples for open api spec
            elif k == "base":
                class_body["model_config"] = {
                    "json_schema_extra": {  # type: ignore
                        "examples": [
                            {col: random_value(8, t.type) for col, t in body.custom_schema.items()},  # type: ignore
                        ],
                    },
                }

            # create new class
            name = get_proper_class_name(body.name) + k.capitalize()
            CUSTOM_CLASSES[k][body.name] = get_base_class(k)
            CUSTOM_CLASSES[k][body.name]["instance"] = class_with_types(
                body.name,
                (CUSTOM_CLASSES[k][body.name]["model"],),
                class_body,
            )

    except Exception:
        logger.error(traceback.format_exc())
        del df
        remove_file(temp_file_loc)
        return JSONResponse(
            status_code=500,
            content={"message": "Internal server error"},
        )

    # flush redis db
    logger.info(f"Flushing redis db for index {body.name}.")
    clear_ns(r, body.name)

    print(CUSTOM_CLASSES)

    # load data into redis db
    errors = 0
    for line_num, (i, row) in enumerate(df.iterrows()):
        try:
            name = get_proper_class_name(body.name) + "Hash"
            x = CUSTOM_CLASSES["hash"][name]["instance"](
                **row.to_dict(),
            )  # Upload to Redis
            x.save()
        except Exception as e:
            logger.error(e)
            logger.error(msg=row.to_dict())
            errors += 1

        if ((line_num + 1) % 100) == 0:
            logger.info(
                f"Total rows processed: {line_num+1}. ({round(100*(line_num + 1)/len(df), 1)})%",
            )
    logger.info(
        f"Final data load into Redis: {len(df)} total rows processed. {errors} missed rows.",
    )

    # create indices by running Migrate for Redis OM
    try:
        logger.info("Creating indices.")
        Migrator().run()
    except Exception as e:
        logger.error("Error creating indices.")
        logger.error(e)
        remove_file(temp_file_loc)
        if df:
            del df
        return JSONResponse(
            status_code=500,
            content={"message": "Internal server error"},
        )

    logger.info("Refresh complete.")
    remove_file(temp_file_loc)
    if df:
        del df

    return JSONResponse(
        status_code=200,
        content={"message": "Refresh complete. Data loaded into redis db."},
    )


@app.on_event("startup")
async def startup_event():
    # ping redis db
    if not ping_redis():
        return
