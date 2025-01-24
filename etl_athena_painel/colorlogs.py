from logging import Formatter
import logging
import re


class fg:
    BLACK = "\033[30m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    WHITE = "\033[37m"
    RESET = "\033[39m"


class bg:
    BLACK = "\033[40m"
    RED = "\033[41m"
    GREEN = "\033[42m"
    YELLOW = "\033[43m"
    BLUE = "\033[44m"
    MAGENTA = "\033[45m"
    CYAN = "\033[46m"
    WHITE = "\033[47m"
    RESET = "\033[49m"


class style:
    BRIGHT = "\033[1m"
    DIM = "\033[2m"
    NORMAL = "\033[22m"
    RESET_ALL = "\033[0m"


class codes:
    grey = "\x1b[38;21m"
    green = "\x1b[1;32m"
    yellow = "\x1b[38;5;226m"
    red = "\x1b[38;5;196m"
    bold_red = "\x1b[31;1m"
    blue = "\x1b[38;5;39m"
    light_blue = "\x1b[1;36m"
    purple = "\x1b[1;35m"
    magenta = "\x1b[35m"
    reset = "\x1b[0m"


class ColorLogg(Formatter):
    FORMATS = {
        logging.DEBUG: codes.grey,
        logging.INFO: codes.blue,
        logging.WARNING: codes.yellow,
        logging.ERROR: codes.red,
        logging.CRITICAL: codes.bold_red,
    }

    def format(self, record: logging.LogRecord) -> str:
        color_level = self.FORMATS.get(record.levelno)
        color_asc = codes.green
        color_msg = codes.purple
        reset = codes.reset

        orig_format = self._fmt
        orig_msg = record.msg

        asc_compilar = r"(%\(asctime\).*?s)"
        level_compilar = r"(%\(levelname\).*?s)"
        msg_compile = r"((bronze|silver|SubstatementType))"

        orig_format = re.sub(asc_compilar, rf"{color_asc}\1{reset}", orig_format)
        orig_format = re.sub(level_compilar, rf"{color_level}\1{reset}", orig_format)

        orig_msg = re.sub(msg_compile, rf"{color_msg}\1{reset}", orig_msg)
        record.msg = orig_msg

        new_format = logging.Formatter(orig_format, datefmt=self.datefmt)

        return new_format.format(record)
