import argparse
import json
import os
from pathlib import Path
import csv
import sys
import logging

from flowparser.utils.parser import LogParser
from flowparser.utils.render import Render
from flowparser.utils.logger import Logger
from flowparser.config import *
# Increase csv size
csv.field_size_limit(sys.maxsize)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help='Input can be either a single file or a folder')
    parser.add_argument("-o", "--outputDir", required=True, help='Output location will be created automatically based on value')
    parser.add_argument("--output_type", nargs='+', default = OUTPUT_DEFAULTS, help="excel | json | html")
    parser.add_argument("--waiting-threshold", default=PARSING_WAITING_THRESHOLD, help='Threshold to indicate waiting for resources in seconds')
    parser.add_argument("--log-level", default = "info")
    
    args = parser.parse_args()

    
    levels = {
    'critical': logging.CRITICAL,
    'error': logging.ERROR,
    'warn': logging.WARNING,
    'warning': logging.WARNING,
    'info': logging.INFO,
    'debug': logging.DEBUG
}
    level = levels.get(args.log_level.lower())
    if level is None:
        raise ValueError(
            f"log level given: {args.log_level}"
            f" -- must be one of: {' | '.join(levels.keys())}")
    logger_obj = Logger(level=level)
    logger = logger_obj._logger

    waiting_threshold = float(args.waiting_threshold)
    aggergate_job_details = []
    outputDir = Path(args.outputDir)

    files_to_parse = []
    filename_to_parse = []
    if os.path.isfile(args.input):
        files_to_parse.append(args.input)
        filename_to_parse.append(str(args.input).split('/')[-1])
    else:
        for filename in os.listdir(args.input):
            t = os.path.join(args.input, filename)
            if os.path.isfile(t) :
                files_to_parse.append(t)
                filename_to_parse.append(filename)

    if logger:
        logger.info(f'Parsing the following files {filename_to_parse}')
    

    for file, filename in zip(files_to_parse, filename_to_parse):
        if file.endswith("json"):
            try:
                log_detail = json.load(open(file))
            except:
                if logger:
                    logger.error(f'Cannot parse json file for {filename}')
                continue
        elif file.endswith("csv"):
            reader = csv.DictReader(open(file))
            log_detail = list()
            for items in reader:
                log_detail.append(items)
        else:
            if logger:
                logger.info(f'Skipping non log file {filename}')
            continue
        
        log_parser = LogParser(logger=logger, waiting_threshold=waiting_threshold,)
        if logger:
            logger.info(f'Log parsing started for {filename}')
        job_details, err = log_parser.parse_from_json(log_detail, filename=filename)
        if err:
            if logger:
                logger.error(f'Failed to process {filename} due to {err}')
            continue
        if logger:
            logger.info(f'Log parsing finished for {filename}')

        aggergate_job_details.append(job_details)
        if 'json' in args.output_type:
            render = Render(job_details, outputDir)
            render.json()
        if 'html' in args.output_type:
            render = Render(job_details, outputDir)
            render.html()
        if 'excel' in args.output_type:
            render = Render(job_details, outputDir)
            render.excel()
    
    # Aggregation of data
    if 'json' in args.output_type:
        render = Render(aggergate_job_details, outputDir, True)
        render.json()

if __name__ == "__main__":
    main()