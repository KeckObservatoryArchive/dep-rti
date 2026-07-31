import yaml
from pathlib import Path

# error reporting via slack / email
import check_dep_status_errors

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
        "GENERAL": [
            "koa_base_path", "max_errors", "max_lock_retries",
            "lock_chk_period", "dev", "admin_email"
        ],
        "LOGGING": ["log_dir", "xfr_level", "pkg_level"],
        "FDT_PKG": ["pkg_timeout", "monitor_period"],
        "FDT_XFR": [
            "xfr_timeout", "monitor_period",
            "dtn_jar", "dtn_server", "dtn_port"
        ],
        "SCALES": ["max_pkg_size", "inst_prefixes"]
    }

    fdt_instruments = cfg_dict.get("GENERAL", {}).get("instruments", [])

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

    # Type/range checks
    timeout = cfg_dict["FDT_PKG"]["pkg_timeout"]
    if not isinstance(timeout, int) or timeout <= 0:
        raise ValueError("FDT_PKG.pkg_timeout must be a positive integer.")


def define_data_path(ctx, filepath):
    """
    Define the data path to be used for the monitor session.

    The data path can be defined on the command line at startup.
    """
    if filepath:
        return Path(filepath)

    return Path(f"{ctx.cfg['GENERAL']['koa_base_path']}/"
                f"{ctx.inst}/{ctx.lev}/")


def define_tar_path(ctx, tar_path):
    """
    Define the tar file path to be used for the monitor session.

    The tar path can be defined on the command line at startup.
    """
    if tar_path:
        return Path(tar_path)

    return Path(f"{ctx.cfg['GENERAL']['koa_base_path']}/"
                f"{ctx.inst}/tarfiles/{ctx.lev_str}/")


def chk_for_errors(ctx, db_obj):
    """
    Used to send an email and slack message when the database status
    is in ERROR.
    """
    errors = db_obj.chk_for_errors()

    # turn off slack when in dev mode
    if ctx.dev:
        send_slack = False
    else:
        send_slack = True

    if errors:
        check_dep_status_errors.main(
            admin_email=ctx.admin_email, slack=send_slack,
            dev=ctx.dev
        )

