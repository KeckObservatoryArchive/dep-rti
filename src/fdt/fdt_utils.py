import yaml
from pathlib import Path

def read_config(cfg_file):
    """
    Read YAML configuration file.
    """
    with open(cfg_file) as f:
        cfg = yaml.safe_load(f)

    return cfg


def validate_cfg(cfg_dict):
    """
    Validate the configuration dict

    Raises:
        ValueError: If required configuration is missing or invalid.
    """

    required = {
        "DATABASE": ["host", "user", "pwd", "db", ],
        "FDT_PROCESS": ["koa_base_path", "pkg_timeout", ],
        "SCALES": ["max_pkg_size", "inst_prefixes"]
    }

    fdt_instruments = {'SCALES'}

    # Required sections and parameters
    for section, keys in required.items():
        if section not in cfg_dict:
            raise ValueError(f"Missing configuration section '{section}'.")

        for key in keys:
            if key not in cfg_dict[section]:
                raise ValueError(f"Missing configuration value '{section}.{key}'.")

    # Validate instrument sections
    for inst in fdt_instruments:
        # if not in config,  this is okay,  each instrument is optional
        if inst not in cfg_dict:
            continue

        if "max_pkg_size" not in cfg_dict[inst]:
            raise ValueError(f"Missing '{inst}.max_pkg_size'.")

    # Type/range checks
    timeout = cfg_dict["FDT_PROCESS"]["pkg_timeout"]
    if not isinstance(timeout, int) or timeout <= 0:
        raise ValueError("FDT_PROCESS.pkg_timeout must be a positive integer.")


def define_data_path(ctx, filepath):
    if filepath:
        return Path(filepath)

    return Path(f"{ctx.cfg['FDT_PROCESS']['koa_base_path']}/"
                f"{ctx.inst}/{ctx.lev}/")


def define_tar_path(ctx, tar_path):
    if tar_path:
        return Path(tar_path)

    return Path(f"{ctx.cfg['FDT_PROCESS']['koa_base_path']}/"
                f"{ctx.inst}/tarfiles/{ctx.lev_str}/")

