from configparser import ConfigParser
import os
import sys
import logging
import colorlog
import argparse
import importlib.util

import fundermapssdk.util
from fundermapssdk.app import App

logger = logging.getLogger("app")


def load_script_module(script_name: str):
    if not script_name.endswith(".py"):
        script_name += ".py"

    current_directory = os.path.dirname(os.path.realpath(__file__))
    script_path = os.path.join(current_directory, "scripts", script_name)

    spec = importlib.util.spec_from_file_location(script_name, script_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[script_name] = module
    spec.loader.exec_module(module)


parser = argparse.ArgumentParser(description="FunderMaps SDK Script Runner")

parser.add_argument("-c", "--config", help="path to the configuration file")
parser.add_argument("-l", "--log-level", help="log level", default="INFO")
parser.add_argument("script", help="path to the script to run")

args = parser.parse_args()

handler = colorlog.StreamHandler()
handler.setFormatter(
    colorlog.ColoredFormatter("%(log_color)s%(levelname)-8s %(name)s : %(message)s")
)

# Set up logging to console
logging.basicConfig(
    level=args.log_level, handlers=[handler], format="%(asctime)s - %(message)s"
)

logger.debug("DBG Starting script")
logger.info("Starting script")

# Find and read the configuration file
if args.config:
    config = ConfigParser()
    config.read(args.config)
else:
    config = fundermapssdk.util.find_config()

load_script_module(args.script)

App(config, logger).run()
