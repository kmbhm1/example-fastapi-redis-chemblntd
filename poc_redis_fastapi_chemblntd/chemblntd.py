import datetime

from pydantic import BaseModel
from redis_om import Field, HashModel


class StrictDate(datetime.date):
    @classmethod
    def __get_validators__(cls) -> "CallableGenerator":  # type: ignore # noqa: F821
        yield cls.validate

    @classmethod
    def validate(cls, value: datetime.date, **kwargs) -> datetime.date:
        if isinstance(value, str):
            try:
                return datetime.datetime.strptime(value, "%Y/%m/%d").date()
            except ValueError:
                raise ValueError("Value must be a datetime.date object")
        elif not isinstance(value, datetime.date):
            raise ValueError("Value must be a datetime.date object")
        return value


# HashModel for Redis Object Mapper
class ChembtlntdRedis(HashModel):
    numrow: str
    sid: str = Field(index=True)
    cid: str = Field(index=True)
    bioassay_source: str = Field(index=True)
    rankscore: int
    outcome: str = Field(index=True)
    depositdate: str = Field(index=True)
    luminescence_parasite_a: int
    luminescence_parasite_b: int
    luminescence_liver_a: float
    luminescence_liver_b: float
    parasite_pct_control_a_pct: float
    parasite_pct_control_b_pct: float
    liver_pct_control_a_pct: float
    liver_pct_control_b_pct: float
    activity_parasite_a_pct: float
    activity_parasite_b_pct: float
    sid_smiles: str = Field(index=True, full_text_search=True)
    pubchem_substance_synonym: str


# Model for OpenAPI
# TODO: conflict occurred while using BaseModel & HashModel metaclasses
class Chembtlntd(BaseModel):
    numrow: str
    sid: str
    cid: str
    bioassay_source: str
    rankscore: int
    outcome: str
    depositdate: str
    luminescence_parasite_a: int
    luminescence_parasite_b: int
    luminescence_liver_a: float
    luminescence_liver_b: float
    parasite_pct_control_a_pct: float
    parasite_pct_control_b_pct: float
    liver_pct_control_a_pct: float
    liver_pct_control_b_pct: float
    activity_parasite_a_pct: float
    activity_parasite_b_pct: float
    sid_smiles: str
    pubchem_substance_synonym: str

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "numrow": "1",
                    "sid": "123456789",
                    "cid": "123456789.0",
                    "bioassay_source": "foo",
                    "rankscore": 33,
                    "outcome": "Inactive",
                    "depositdate": "2019/12/31",
                    "luminescence_parasite_a": 0,
                    "luminescence_parasite_b": 0,
                    "luminescence_liver_a": 0.0,
                    "luminescence_liver_b": 0.0,
                    "parasite_pct_control_a_pct": 0.0,
                    "parasite_pct_control_b_pct": 0.0,
                    "liver_pct_control_a_pct": 0.0,
                    "liver_pct_control_b_pct": 0.0,
                    "activity_parasite_a_pct": 0.0,
                    "activity_parasite_b_pct": 0.0,
                    "sid_smiles": "CC1=CC=C(C=C1)C(=O)NC2=CC=C(C=C2)C(=O)NC3=CC=C(C=C3)C(=O)NC4=CC=C(C=C4)C(=O)NC5=CC=C(C=C5)C(=O)NC6=CC=C(C=C6)C(=O)NC7=CC=C(C=C7)C(=O)NC8=CC=C(C=C8)C(=O)NC9=CC=C(C=C9",  # noqa: E501
                    "pubchem_substance_synonym": "PubChem Substance Synonym",
                },
            ],
        },
    }
