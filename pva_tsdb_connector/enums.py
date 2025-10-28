from enum import StrEnum, IntEnum


class MetricCategories(StrEnum):
    EXTREME_VALUES = "Extreme Values"
    CHANGE_RATE = "Rate of Change"


class MetricUnits(StrEnum):
    PROPORTION = "proportion"
    SECONDS = "seconds"


class MetricDefaultOrders(StrEnum):
    ASC = "asc"
    DESC = "desc"


class TSStatusCodesEnum(IntEnum):
    SUCCESS = 0
    UNKNOWN_ERROR = 1
    NO_NEW_DATA = 2
    API_ERROR = 3
    API_PARSE_ERROR = 4
    DB_ERROR = 5
    API_RATE_LIMITED = 6
    INTERNAL_ERROR = 7


class AllOrAnyTags(StrEnum):
    ALL = "all"
    ANY = "any"
