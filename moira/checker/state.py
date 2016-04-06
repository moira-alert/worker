OK = "OK"
WARN = "WARN"
ERROR = "ERROR"
NODATA = "NODATA"
EXCEPTION = "EXCEPTION"
DEL = "DEL"


def to_metric_state(state):
    if state == DEL:
        return NODATA
    return state
