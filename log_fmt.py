import logging
import os
import datetime
import sys

from colorama import Fore


def init_all(level=logging.INFO, file_prefix: str = None):
  # if not os.path.exists('log'):
  #   os.mkdir('log')

  root_logger = logging.getLogger()
  root_logger.setLevel(level)

  log_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(threadName)s: %(message)s")

  # if file_prefix:
  #   if not os.path.exists(file_prefix):
  #     os.mkdir(file_prefix)
  #   file_handler = logging.FileHandler(os.path.join('log', str(datetime.datetime.now()).replace(':', '_') + '.log'))
  #   file_handler.setFormatter(log_formatter)
  #   root_logger.addHandler(file_handler)

  console_handler = logging.StreamHandler(stream=sys.stdout)
  console_handler.setFormatter(LogFormatter())
  root_logger.addHandler(console_handler)


class LogFormatter(logging.Formatter):
  def __init__(self, style='{'):
    logging.Formatter.__init__(self, style=style)

  def format(self, record):
    stdout_template = '{levelname}' + Fore.RESET + '] {threadName}: ' + '{message}'
    stdout_head = '[%s'

    all_formats = {
      logging.DEBUG: logging.StrFormatStyle(stdout_head % Fore.LIGHTBLUE_EX + stdout_template),
      logging.INFO: logging.StrFormatStyle(stdout_head % Fore.GREEN + stdout_template),
      logging.WARNING: logging.StrFormatStyle(stdout_head % Fore.LIGHTYELLOW_EX + stdout_template),
      logging.ERROR: logging.StrFormatStyle(stdout_head % Fore.LIGHTRED_EX + stdout_template),
      logging.CRITICAL: logging.StrFormatStyle(stdout_head % Fore.RED + stdout_template)
    }

    self._style = all_formats.get(record.levelno, logging.StrFormatStyle(logging._STYLES['{'][1]))  # TODO
    self._fmt = self._style._fmt
    result = logging.Formatter.format(self, record)

    return result
