import ast
import operator

from moira.checker import state

_default = ast.parse("ERROR if compare_operator(t1, error_value) else \
    WARN if compare_operator(t1, warn_value) else OK", mode='eval')
DEFAULT = compile(_default, '<string>', mode='eval')


class ExpressionError(Exception):
    pass


cache = {}


def compile_expression(exp):
    cached = cache.get(exp)
    if cached is not None:
        return cached
    _exp = ast.parse(exp)
    nodes = [node for node in ast.walk(_exp)]
    if len(nodes) < 2 or not isinstance(nodes[1], ast.Expr):
        raise ExpressionError("%s is not Expression" % exp)
    for node in nodes:
        if isinstance(node, ast.Call):
            raise ExpressionError("Call method is forbidden")
        if isinstance(node, ast.Lambda):
            raise ExpressionError("Lambda is strongly forbidden")
    result = compile(exp, '<string>', mode='eval')
    cache[exp] = result
    return result


def getExpression(trigger_expression=None, **kwargs):
    global_dict = {"OK": state.OK,
                   "WARN": state.WARN,
                   "WARNING": state.WARN,
                   "ERROR": state.ERROR,
                   "NODATA": state.NODATA}
    for k, v in kwargs.iteritems():
        global_dict[k] = v
    if not trigger_expression:
        global_dict['compare_operator'] = operator.ge if global_dict['warn_value'] <= global_dict['error_value'] \
            else operator.le
        return eval(DEFAULT, global_dict)
    else:
        return eval(compile_expression(trigger_expression), global_dict)
