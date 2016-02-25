OK = "OK"
WARN = "WARN"
ERROR = "ERROR"
NODATA = "NODATA"
EXCEPTION = "EXCEPTION"
DEL = "DEL"


def toMetricState(state):
    if state == DEL:
        return NODATA
    return state
