def trigger_reformat(trigger, trigger_id, tags):
    if trigger_id:
        trigger["id"] = trigger_id
    trigger["tags"] = list(tags)
    trigger["warn_value"] = float(trigger["warn_value"])
    trigger["error_value"] = float(trigger["error_value"])
    ttl = trigger.get("ttl")
    if ttl:
        trigger["ttl"] = int(ttl)
    else:
        trigger["ttl"] = None
    return trigger
