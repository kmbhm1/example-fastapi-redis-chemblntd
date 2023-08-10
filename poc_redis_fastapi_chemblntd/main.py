import logging
import os
import tempfile

import redis
from fastapi import FastAPI
from fastapi.params import Path
from fastapi.responses import JSONResponse
from fastapi_pagination import add_pagination
from poc_redis_fastapi_chemblntd.chebi import RedisNameLookup

from poc_redis_fastapi_chemblntd.helpers import *
from poc_redis_fastapi_chemblntd.openapi import Item, ReponseData, responses, Message

# logger setup
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s:\t  %(name)s [%(lineno)s] - %(message)s",
)

# inits
logger = logging.getLogger(__name__)  # get root logger
app = FastAPI()  # FastApi
add_pagination(app)  # add pagination
REDIS_HOST = os.environ.get("REDIS_HOST", "127.0.0.1")  # for docker-compose
r = redis.Redis(host=REDIS_HOST, port=6379)  # Redis connection object
rnl = RedisNameLookup(r)  # RedisNameLookup

# constants
TEMP_DIR = tempfile.gettempdir()

# responses
_refresh_responses = responses.copy()
del _refresh_responses[404]


@app.on_event("startup")
async def startup_event():
    # ping redis db
    if not ping_redis(r):
        return

    _ = rnl.load()


@app.post("/refresh", response_model=Message, responses=_refresh_responses)
async def refresh():
    """
    Refreshes the ChEBI Redis database with the latest ChEBI data.
    """
    # ping redis db
    if not ping_redis(r):
        return JSONResponse(
            status_code=500,
            content={"message": "Internal server error"},
        )

    # refresh RedisNameLookup
    try:
        rnl.flush()
        total = rnl.load(force=True)
    except Exception as e:
        logger.error(e)
        return JSONResponse(
            status_code=500,
            content={"message": "Internal server error"},
        )

    msg = {"message": "Refresh complete.", "totals": total}
    logger.info(msg["message"])
    return JSONResponse(status_code=200, content=msg)


@app.get("/search/{search_term}", response_model=Item[ReponseData], responses=responses)
async def search(
    search_term: str = Path(
        title="The search term.",
        description="The search term to search for in the Redis database.",
        example=["terbinafine", r"%20CCO%20%20%20%20"],
    ),
):
    """
    Returns a list of ChEBI entries that contain information on the given search term in the Redis database.
    """
    try:
        result = rnl[search_term]
        logger.info(result)
        msg = {"id": search_term, "value": result}

        return JSONResponse(status_code=200, content=msg)
    except KeyError:
        return JSONResponse(status_code=404, content={"message": "Item not found"})
    except Exception as e:
        logger.error(e)
        return JSONResponse(
            status_code=500,
            content={"message": "Internal server error"},
        )
