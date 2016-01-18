def trigger_reformat(trigger, trigger_id, tags):
    if trigger_id:
        trigger["id"] = trigger_id
    trigger["tags"] = list(tags)
    for field in ["warn_value", "error_value"]:
        value = trigger.get(field)
        if value is not None:
            trigger[field] = float(value)
        else:
            trigger[field] = None
    ttl = trigger.get("ttl")
    if ttl:
        trigger["ttl"] = int(ttl)
    else:
        trigger["ttl"] = None
    return trigger
