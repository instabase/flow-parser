# Package Constants
PARSING_WAITING_THRESHOLD = 1

OUTPUT_DEFAULTS = 'json excel html'

# Create an output log file?
OUTPUT_LOG = True

# Path to output log; by default, working directory of script if not specified
LOG_PATH = ''

# Log file name appended with date and timestamp
LOG_FILE_PREFIX = 'instabase_'

# Print output logging to console?
PRINT_TO_CONSOLE = True

# Disable all logging? You're on your own then!
SUPPRESS_LOGGING = False

# You might integrate the library in an application with a predefined logging scheme. If so, you may not need the
# library's default logging handlers, formatters etc.--instead, you can inherit an external logger instance.
INHERIT_LOGGING_CONFIG = False

