import argparse
import json
import os
from pathlib import Path
import csv
import sys

from .parser import LogParser
from .render import Render

# Increase csv size
csv.field_size_limit(sys.maxsize)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help='Input can be either a single file or a folder')
    parser.add_argument("-o", "--outputDir", required=True, help='Output location will be created automatically based on value')
    parser.add_argument("--output_type", nargs='+', default = 'json excel html', help="excel | json | html")
    parser.add_argument("--waiting-threshold", default=1, help='Threshold to indicate waiting for resources in seconds')
    
    args = parser.parse_args()

    waiting_threshold = float(args.waiting_threshold)
    aggergate_job_details = []
    outputDir = Path(args.outputDir)

    files_to_parse = []
    if os.path.isfile(args.input):
        files_to_parse.append(args.input)
    else:
        for filename in os.listdir(args.input):
            t = os.path.join(args.input, filename)
            if os.path.isfile(t) :
                files_to_parse.append(t)
    

    for file in files_to_parse:
        if file.endswith("json"):
            try:
                log_detail = json.load(open(file))
            except:
                print(f'Cannot parse {file}')
                continue
        elif file.endswith("csv"):
            reader = csv.DictReader(open(file))
            log_detail = list()
            for items in reader:
                log_detail.append(items)
        else:
            continue
        
        log_parser = LogParser(waiting_threshold=waiting_threshold)

        job_details = log_parser.parse_from_json(log_detail)
        # job_details_agg = log_parser.aggergate_details(job_details)

        aggergate_job_details.append(job_details)

        if 'html' in args.output_type:
            render = Render(job_details, outputDir)
            render.html()

        if 'json' in args.output_type:
            render = Render(job_details, outputDir)
            render.json()

        if 'excel' in args.output_type:
            render = Render(job_details, outputDir)
            render.excel()
    
    # Aggregation of data
    if 'json' in args.output_type:
        render = Render(aggergate_job_details, outputDir, True)
        render.json()

if __name__ == "__main__":
    main()