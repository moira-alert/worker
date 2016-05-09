class TargetTimeSeries(dict):

    def __init__(self, *arg, **kwargs):
        super(TargetTimeSeries, self).__init__(*arg, **kwargs)
        self.other_targets_names = {}

    def get_expression_values(self, t1, timestamp):
        expression_values = {}
        for target_number in xrange(1, len(self) + 1):
            target_name = "t%s" % target_number
            tN = self[target_number][0] if target_number > 1 else t1
            value_index = (timestamp - tN.start) / tN.step
            tN_value = tN[value_index] if len(tN) > value_index else None
            expression_values[target_name] = tN_value
            if tN_value is None:
                break
        return expression_values

    def set_state_value(self, metric_state, expression_values, tN):
        if expression_values is None:
            if "value" in metric_state:
                del metric_state["value"]
        else:
            metric_state["value"] = expression_values[tN]

    def update_state(self, t1, check, expression_state, expression_values, timestamp):
        metric_state = check["metrics"][t1.name]
        metric_state["state"] = expression_state
        metric_state["timestamp"] = timestamp
        self.set_state_value(metric_state, expression_values, "t1")

        for tN, tName in self.other_targets_names.iteritems():
            other_metric_state = check["metrics"][tName]
            other_metric_state["state"] = expression_state
            other_metric_state["timestamp"] = timestamp
            self.set_state_value(other_metric_state, expression_values, tN)
