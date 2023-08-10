from typing import List, Optional, TypeVar, Generic

from pydantic import BaseModel

# Api Documentation
description = """
# ChEMBL-NTD Redis API

## Introduction

This API is a proof of concept for a Redis API for [ChEMBL-NTD](https://chembl.gitbook.io/chembl-ntd/).
It is based on the [Redis Object Mapper](https://redis.com/blog/introducing-redis-om-for-python/) and
[FastAPI](https://fastapi.tiangolo.com/).

## Usage

The API is read-only and provides the following endpoints:

- `/refresh`: initiates a refresh of the database from the ChEMBL-NTD FTP site
- `/searchs/{search_term}`: returns a list of ChEMBL-NTD entries with the given SMILES string
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


# Reponses
responses = {
    404: {"message": "Item not found"},
    500: {"message": "Internal server error"},
}


class Message(BaseModel):
    message: str
    totals: Optional[dict[str, int]] | None

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "message": "Refresh complete.",
                    "totals": {
                        "data": 1234,
                        "lookup": 1234,
                    },
                },
            ],
        },
    }


class ReponseData(BaseModel):
    name: str | None
    description: str | None
    smiles: str | None
    ismiles: str | None
    chebi: Optional[int] | None
    reordering: Optional[List[int]] | None

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "name": "terbinafine",
                    "description": "A tertiary amine that is N-methyl-1-naphthalenemethylamine in which the amino hydrogen is replaced by a 3-(tertbutylethynyl)allyl group. An antifungal agent administered orally (generally as the hydrochloride salt) for the treatment of skin and nail infections.",  # noqa: E501
                    "smiles": "CN(CC=CC#CC(C)(C)C)Cc1cccc2ccccc12",
                    "ismiles": "CN(C/C=C/C#CC(C)(C)C)Cc1cccc2ccccc12",
                    "chebi": 9448,
                    "reordering": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21],
                },
            ],
        },
    }


T = TypeVar("T")


class Items(BaseModel, Generic[T]):
    id: str
    value: List[T]


class Item(BaseModel, Generic[T]):
    id: str
    value: T
