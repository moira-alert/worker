OK = "OK"
WARN = "WARN"
ERROR = "ERROR"
NODATA = "NODATA"
EXCEPTION = "EXCEPTION"
DEL = "DEL"

SCORES = {
    "OK": 0,
    "DEL": 0,
    "WARN": 1,
    "ERROR": 100,
    "NODATA": 1000,
    "EXCEPTION": 100000
}


def to_metric_state(state):
    if state == DEL:
        return NODATA
    return state
