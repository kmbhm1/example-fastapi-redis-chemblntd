import datetime

from redis_om import Field, HashModel


class StrictDate(datetime.date):
    @classmethod
    def __get_validators__(cls) -> "CallableGenerator":  # noqa: F821
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


class Chembtlntd(HashModel):
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
