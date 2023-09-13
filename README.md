# Instabase Flow Log Parser
A simple utility which will parse Instabase Flow Logs

* Free software: MIT license
* HTML | JSON | EXCEL export functionality

## Setup
1. Download the latest flow parser from https://github.com/instabase/flow-parser

2. 
    ```shell
    cd ./flow-parser
   pip install .
    ```
   
1. ```ibflowparser``` is now an executable to run 
   ```shell
    ❯ ibflowparser -h                                  
    usage: ibflowparser [-h] --input INPUT -o OUTPUTDIR [--output_type OUTPUT_TYPE [OUTPUT_TYPE ...]]

    optional arguments:
    -h, --help            show this help message and exit
    --input INPUT         Input can be either a single file or a folder
    -o OUTPUTDIR, --outputDir OUTPUTDIR
                            Output location will be created automatically based on value
    --output_type OUTPUT_TYPE [OUTPUT_TYPE ...]
                            excel | json | html
   ```

## Usage

Instabase Flow Log Parser leverages the json or csv log files provided direclty from Flow Dashboard. To get started, please leverage Flow Dashboard > Select Job Logs > Download JSON | CSV.

### Parse and Export
You can provide a single log file or a folder into the parser. Excel | Json | Html are valid output types

```shell
ibflowparser --input ./logs -o ./output --output_type excel json html
```

### HTML Output
Within the chart, you can drill into each element. Process Files and Refiner will allow for further drill downs