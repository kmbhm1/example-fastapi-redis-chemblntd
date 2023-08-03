from typing import List, Literal, Optional
from pydantic import BaseModel
from fastapi import Body

from poc_redis_fastapi_chemblntd.chemblntd import Chembtlntd

# Api Documentation
description = """
# ChEMBL-NTD Redis API

## Introduction

This API is a proof of concept for a Redis API for [ChEMBL-NTD](https://chembl.gitbook.io/chembl-ntd/). 
It is based on the [Redis Object Mapper](https://redis.com/blog/introducing-redis-om-for-python/) and 
[FastAPI](https://fastapi.tiangolo.com/).

## Usage

The API is read-only and provides the following endpoints:

- `/chemblntd/hash`: returns a list of all valid hashes ids in the database
- `/chemblntd/hash/{hash}`: returns the ChEMBL-NTD entry for the given hash
- `/chemblntd/search/cid/{cid}`: returns a list of ChEMBL-NTD entries for the given CID
- `/chemblntd/search/sid/{sid}`: returns a list of ChEMBL-NTD entries for the given SID
- `/chemblntd/search/smiles/{smiles}`: returns a list of ChEMBL-NTD entries with the given SMILES string
- `/chemblntd/refresh`: initiates a refresh of the database from the ChEMBL-NTD FTP site
"""

fast_api_metadata = {
    "title": "ChEMBL-NTD Redis API",
    "description": description,
    "summary": "A proof of concept Redis API for ChEMBL-NTD data",
    "version": "0.1.1",
    "terms_of_service": "https://docs.github.com/en/site-policy/github-terms/github-terms-of-service",
    "contact": {"name": "K", "email": "kmbhm1@gmail.com"},
    "license_info": {"name": "Apache 2.0", "identifier": "MIT"},
}


class RefreshHashModelInfo(BaseModel):
    type: Literal["int", "float", "str", "bool"]
    index: bool = False
    full_text_search: bool = False


class RefreshBody(BaseModel):
    name: str
    url: str
    custom_schema: Optional[dict[str, RefreshHashModelInfo]]  # key = column name


class GetColumnsBody(BaseModel):
    url: str


# Reponses
responses = {
    404: {"message": "Item not found"},
    500: {"message": "Internal server error"},
}


class SmilesBody(BaseModel):
    smiles: str = Body(
        title="The SMILES string",
        description="The [SMILES](https://en.wikipedia.org/wiki/Simplified_molecular-input_line-entry_system) string to search for",
    )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "smiles": "CCC1C(=O)NC(=O)NC1=O",
                }
            ]
        }
    }


class Message(BaseModel):
    message: str


class Hashes(BaseModel):
    hashes: List[str]

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "hashes": [
                        "01H5T42K2AEA7682MR9XD7Y2S4",
                        "01H5T42K2F35GKYAP3TBDJXBBF",
                        "01H5T42K2H54ST253B5J3H26PB",
                        "01H5T42K2JZB0HAKFTXZXJE9MC",
                        "01H5T42K2MFJT1P3Q9V90YZ3DW",
                    ]
                }
            ]
        }
    }


class Items(BaseModel):
    id: str
    value: List[Chembtlntd]


class Item(BaseModel):
    id: str
    value: Chembtlntd

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "id": "01H5T42K2AEA7682MR9XD7Y2S4",
                    "value": {
                        "pk": "01H5T42K2AEA7682MR9XD7Y2S4",
                        "numrow": "1",
                        "sid": "121363756",
                        "cid": "54735847.0",
                        "bioassay_source": "ICCB-Longwood/NSRB Screening Facility, Harvard Medical School",
                        "rankscore": 100,
                        "outcome": "Inactive",
                        "depositdate": "2012/06/28",
                        "luminescence_parasite_a": 214,
                        "luminescence_parasite_b": 230,
                        "luminescence_liver_a": 4178640.0,
                        "luminescence_liver_b": 3648600.0,
                        "parasite_pct_control_a_pct": 1.56,
                        "parasite_pct_control_b_pct": 1.85,
                        "liver_pct_control_a_pct": 57.36,
                        "liver_pct_control_b_pct": 40.18,
                        "activity_parasite_a_pct": 0.06,
                        "activity_parasite_b_pct": 0.16,
                        "sid_smiles": "CN(C)c1ccc2[n+](C)c(C=Cc3cc(C)n(c3C)-c3ccccc3)ccc2c1.CN(C)c1ccc2[n+](C)c(C=Cc3cc(C)n(c3C)-c3ccccc3)ccc2c1.CN(C)c1ccc2[n+](C)c(C=Cc3cc(C)n(c3C)-c3ccccc3)ccc2c1.Oc1c(Cc2c(O)c(cc3ccccc23)C([O-])=O)c2ccccc2cc1C([O-])=O",
                        "pubchem_substance_synonym": "HMS2098O21",
                    },
                }
            ]
        }
    }
