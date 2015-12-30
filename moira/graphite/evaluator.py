"""Copyright 2008 Orbitz WorldWide

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""

import re

from moira.graphite.grammar import grammar
from moira.graphite.datalib import TimeSeries, fetchData
from twisted.internet import defer


@defer.inlineCallbacks
def evaluateTarget(requestContext, target):
    tokens = grammar.parseString(target)
    result = yield evaluateTokens(requestContext, tokens)
    if isinstance(result, TimeSeries):
        # we have to return a list of TimeSeries objects
        defer.returnValue([result])
    else:
        defer.returnValue(result)


@defer.inlineCallbacks
def evaluateTokens(requestContext, tokens, replacements=None):
    if tokens.template:
        arglist = dict()
        if tokens.template.kwargs:
            arglist.update(dict([(kwarg.argname, (yield evaluateTokens(requestContext, kwarg.args[0])))
                                 for kwarg in tokens.template.kwargs]))
        if tokens.template.args:
            arglist.update(dict([(str(i + 1), (yield evaluateTokens(requestContext, arg)))
                           for i, arg in enumerate(tokens.template.args)]))
        if 'template' in requestContext:
            arglist.update(requestContext['template'])
        result = yield evaluateTokens(requestContext, tokens.template, arglist)
        defer.returnValue(result)
        raise StopIteration

    elif tokens.expression:
        result = yield evaluateTokens(requestContext, tokens.expression, replacements=replacements)
        for exp in tokens.expression:
            if type(exp) is not unicode:
                continue
            for r in result:
                if not isinstance(r, TimeSeries):
                    continue
                resolve = requestContext['graphite_patterns'].get(exp, set())
                resolve.add(r.name)
                requestContext['graphite_patterns'][exp] = resolve
        defer.returnValue(result)
        raise StopIteration

    elif tokens.pathExpression:
        expression = tokens.pathExpression
        if replacements:
            for name in replacements:
                if expression == '$' + name:
                    val = replacements[name]
                    if not isinstance(
                            val,
                            str) and not isinstance(
                            val,
                            basestring):
                        defer.returnValue(val)
                        raise StopIteration
                    elif re.match('^-?[\d.]+$', val):
                        defer.returnValue(float(val))
                        raise StopIteration
                    else:
                        defer.returnValue(val)
                        raise StopIteration
                else:
                    expression = expression.replace(
                        '$' + name, str(replacements[name]))
        timeseries = yield fetchData(requestContext, expression)
        defer.returnValue(timeseries)
        raise StopIteration

    elif tokens.call:

        if tokens.call.funcname == 'template':
            # if template propagates down here, it means the grammar didn't match the invocation
            # as tokens.template. this generally happens if you try to pass
            # non-numeric/string args
            raise ValueError(
                "invaild template() syntax, only string/numeric arguments are allowed")

        func = SeriesFunctions[tokens.call.funcname]
        args = [(yield evaluateTokens(requestContext, arg, replacements=replacements)) for arg in tokens.call.args]
        kwargs = dict([(kwarg.argname, (yield evaluateTokens(requestContext, kwarg.args[0], replacements=replacements)))
                       for kwarg in tokens.call.kwargs])
        try:
            defer.returnValue((yield func(requestContext, *args, **kwargs)))
        except NormalizeEmptyResultError:
            defer.returnValue([])
        raise StopIteration

    elif tokens.number:
        if tokens.number.integer:
            defer.returnValue(int(tokens.number.integer))
            raise StopIteration
        elif tokens.number.float:
            defer.returnValue(float(tokens.number.float))
            raise StopIteration
        elif tokens.number.scientific:
            defer.returnValue(float(tokens.number.scientific[0]))
            raise StopIteration

    elif tokens.string:
        defer.returnValue(tokens.string[1:-1])
        raise StopIteration

    elif tokens.boolean:
        defer.returnValue(tokens.boolean[0] == 'true')
        raise StopIteration
    else:
        raise ValueError("unknown token in target evaulator")


# Avoid import circularities
from moira.graphite.functions import (SeriesFunctions, NormalizeEmptyResultError)  # noqa
