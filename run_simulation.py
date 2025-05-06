#!/usr/bin/env python3
"""
HTCondor Simulation Runner

This script runs a complete simulation of an HTCondor-like job pipeline,
generating school registry data and processing it into Parquet files.
"""

import os
import time
import argparse
import logging
import subprocess
import threading
import signal
import sys
import atexit
from pathlib import Path
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("simulation.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("Simulation")


def run_generator(args):
    """Run the JSON generator process."""
    cmd = [
        "python", "json_generator.py",
        "--output-dir", args.input_dir,
        "--rate", str(args.generator_rate),
        "--std-dev", str(args.generator_stddev),
        "--min-size", str(args.min_batch_size),
        "--max-size", str(args.max_batch_size)
    ]
    
    if args.num_results:
        cmd.extend(["--num-iterations", str(args.num_results)])
    
    if args.seed is not None:
        cmd.extend(["--seed", str(args.seed)])
    
    logger.info(f"Starting