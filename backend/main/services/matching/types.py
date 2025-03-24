from enum import Enum

import pandas as pd

type SplinkResultPartialDF = pd.DataFrame

type SplinkResultPartialRow = tuple[int, float, int, int]


class SplinkResultPartialRowField(Enum):
    row_number = 0
    match_probability = 1
    person_record_l_id = 2
    person_record_r_id = 3


type PersonCrosswalkDF = pd.DataFrame

type PersonCrosswalkRow = tuple[int, str, int, int, int]


class PersonCrosswalkRowField(Enum):
    id = 0
    created = 1
    version = 2
    record_count = 3
    person_record_id = 4
