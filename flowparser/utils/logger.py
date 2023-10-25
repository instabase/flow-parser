import logging
import datetime

from flowparser.config import *

class Logger():
    def __init__(self,
        output_log=OUTPUT_LOG,                  
        log_path=LOG_PATH,
        log_file_prefix=LOG_FILE_PREFIX,
        print_console=PRINT_TO_CONSOLE,
        suppress_logging=SUPPRESS_LOGGING,
        inherit_logging_config=INHERIT_LOGGING_CONFIG,
        level=logging.DEBUG):
        if not suppress_logging:
            self._logger = logging.getLogger(__name__)

            if not inherit_logging_config:
                self._logger.setLevel(level)

                formatter = logging.Formatter(
                    fmt='%(asctime)s %(name)12s: %(levelname)8s > %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S'
                )
                handler_console = logging.StreamHandler()
                handler_console.setFormatter(formatter)

                if output_log:
                    if log_path and log_path[-1] != '/':
                        log_path += '/'
                    self._log_file = f'{log_path}{log_file_prefix}_log__{datetime.datetime.now():%Y-%m-%d_%H-%M-%S}.log'
                    handler_log = logging.FileHandler(
                        filename=self._log_file
                    )
                    handler_log.setFormatter(formatter)

                if output_log and not self._logger.hasHandlers():
                    self._logger.addHandler(handler_log)
                    if print_console:
                        handler_console.setLevel(logging.INFO)
                        self._logger.addHandler(handler_console)
                elif print_console and not self._logger.hasHandlers():
                    self._logger.addHandler(handler_console)
        else:
            self._logger = None