"""
script to load constance from enviorement,
with default values in case the variable name wasn't found
"""
import os

from dotenv import load_dotenv

load_dotenv()

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
# kafka
BENCHMARK = os.environ.get("BENCHMARK", "False")
