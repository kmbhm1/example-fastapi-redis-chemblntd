import datetime
import logging
import os
import tempfile
import gzip
from lxml import etree

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


@app.get("/sitemap")
async def sitemap():
    try:
        results = rnl.get_all()
        now_str = datetime.datetime.now().strftime("%Y-%m-%d")

        # make dir if not exists
        d = os.path.join(os.path.join(os.path.expanduser("~")), "Desktop", "sitemaps")
        if not os.path.exists(d):
            os.makedirs(d)

        paths = []
        index_sm = '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
        for m in ["epoxidation", "quinone", "reactivity", "phase1", "ndealk", "ugt", "_"]:
            xml = '<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">'
            for r in results:
                xml += f"<url><loc>https://xenosite.org/{m}/{r}</loc><lastmod>{now_str}</lastmod></url>"
            xml += "</urlset>"
            root = etree.fromstring(bytes(xml, "utf-8"))
            tree = bytes(
                '<?xml version="1.0" encoding="UTF-8"?>\n' + etree.tostring(root, pretty_print=True).decode(),
                "utf-8",
            )

            path = os.path.join(d, f"sitemap_{m}.xml.gz")
            with gzip.open(path, "wb") as f:
                f.write(tree)
            paths.append(path)
            index_sm += f"<sitemap><loc>https://xenosite.org/sitemap_{m}.xml.gz</loc></sitemap>"

        index_sm += "</sitemapindex>"
        root = etree.fromstring(bytes(index_sm, "utf-8"))
        tree = '<?xml version="1.0" encoding="UTF-8"?>\n' + etree.tostring(root, pretty_print=True).decode()
        path = os.path.join(d, "sitemap_index.xml")
        with open(path, "w") as f:
            f.write(tree)
        paths.append(path)

        return JSONResponse(status_code=200, content={"message": paths})
    except Exception as e:
        logger.error(e)
        return JSONResponse(
            status_code=500,
            content={"message": "Internal server error"},
        )


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
