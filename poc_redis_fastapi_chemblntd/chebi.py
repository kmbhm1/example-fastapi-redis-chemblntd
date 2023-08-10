import json
import logging
from typing import Union
import ast
import requests
import gzip
import msgpack
import redis
from redis_lock import Lock
from rdkit import Chem
from rdkit import RDLogger


# disable rdkit logging
RDLogger.DisableLog("rdApp.*")

# get root logger
logger = logging.getLogger(__name__)


class RedisNameLookup:
    """Redis Name Lookup Utility

    Attributes:
        redis_conn (redis.Redis): Redis connection object.

    Raises:
        KeyError: If the key is not found in the Redis database.
    """

    chebi_url: str = "https://swami.wustl.edu/~jswami/chebi.msgpack.gz"

    def __init__(self, redis_conn=None):
        self.redis: redis.Redis = redis_conn or redis.Redis()

    def _name_lookup(self, name: str) -> dict:
        """Lookup a name in the Redis database."""
        try:
            logger.info(f"Looking up name: {name}")
            i = self.redis.hget("name-lookup", name)
            i = msgpack.loads(i)
            n = msgpack.loads(self.redis.hget("name-data", i))
            n["chebi"] = i

            logger.info(f"Name lookup result: {json.dumps(n, indent=2)}")
            return n
        except Exception as e:
            logger.info(e)
            raise e

    def _sanitized_smiles_str(self, smi: str) -> str:
        """Sanitize SMILES string"""
        logger.info(f"Sanitizing SMILES string: {smi}")
        smi = smi.strip().split()[0]
        logger.info(f"Sanitized SMILES string: {smi}")

        # TODO: add code here to ensure only SMILES allowable characters
        # if smi contains non-allowable characters:
        #   raise ValueError()

        return smi

    def __getitem__(self, q):
        """Lookup a name or SMILES string in the Redis database."""
        logger.info(f"Checking for '{q}' in chebi database.")
        try:
            mol = Chem.MolFromSmiles(self._sanitized_smiles_str(q))
            smiles = Chem.MolToSmiles(mol, isomericSmiles=False)
            n = self._name_lookup(smiles)
            r = ast.literal_eval(mol.GetProp("_smilesAtomOutputOrder"))
            n["reordering"] = r
            return n
        except Exception as e:
            logger.info(f"SMILES lookup method failed: {e}")
            pass

        try:
            n = self._name_lookup(q.lower())
            return n
        except Exception as e:
            logger.info(f"Name lookup method failed: {e}")
            pass

        raise KeyError(f"Cannot resolve: {q}")

    def download_data(self) -> dict:
        """Download the ChEBI data from the URL.

        Returns:
            dict: The ChEBI data.
        """
        logger.info(f"Downloading ChEBI data from {self.chebi_url}")
        r: requests.Response = requests.get(self.chebi_url)
        d: bytes = gzip.decompress(r.content)
        l: dict = msgpack.loads(d, strict_map_key=False)
        logger.info("Download complete.")

        return l

    def loaded(self) -> bool:
        """Check if the ChEBI data is loaded in the Redis database.

        Returns:
            bool: True if the ChEBI data is loaded in the Redis database.
        """
        return self.redis.hlen("name-lookup") > 1000

    def load(self, force: bool = False) -> Union[bool, dict[str, int]]:
        """Load the ChEBI data into the Redis database.

        Args:
            force (bool, optional): Whether to force a data load. Defaults to False.

        Returns:
            int: The number of records loaded.
        """
        if not force:
            if self.loaded():
                logger.info("ChEBI data loaded previously.")
                return False

        with Lock(self.redis, "name-lock", expire=30, auto_renewal=True) as _:
            D = self.download_data()

            lens: dict[str, list[int]] = {}

            for term in ["data", "lookup"]:
                logger.info(f"Loading ChEBI {term} data into Redis.")
                lens[term] = []
                pipe = self.redis.pipeline()
                n = 1
                for k in D[term]:
                    d = msgpack.dumps(D[term][k])
                    pipe.hset(f"name-{term}", k, d)
                    n += 1
                    if (n % 64) == 0:
                        lens[term].append(len(pipe.execute()))
                        pipe = self.redis.pipeline()
                lens[term].append(len(pipe.execute()))

            results = {k: sum(l) for k, l in lens.items()}
            logger.debug(results)
            return results

    def flush(self):
        logger.info("Flushing Redis database.")
        self.redis.flushdb()
