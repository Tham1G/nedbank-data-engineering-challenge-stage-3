import os
import yaml


def load_pipeline_config():
    candidates = [
        "/data/config/pipeline_config.yaml",
        "config/pipeline_config.yaml"
    ]

    for path in candidates:
        if os.path.exists(path):
            with open(path, "r") as f:
                return yaml.safe_load(f)

    raise FileNotFoundError(
        "pipeline_config.yaml not found in /data/config or config/"
    )


def load_dq_rules():
    candidates = [
        "/data/config/dq_rules.yaml",
        "config/dq_rules.yaml"
    ]

    for path in candidates:
        if os.path.exists(path):
            with open(path, "r") as f:
                return yaml.safe_load(f)

    raise FileNotFoundError(
        "dq_rules.yaml not found in /data/config or config/"
    )