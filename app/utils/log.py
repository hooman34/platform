from typing import NoReturn
import os
from contextlib import contextmanager
import datetime
import logging
from logging import (getLogger, FileHandler, StreamHandler, Formatter, INFO, DEBUG)
import pathlib
import platform
import time


_TIME_FMT = datetime.datetime.today().strftime('%Y%m%d_%H%M%S')
_FILE_FMT = 'log_{}.txt'

_FORMATTER = Formatter('%(asctime)s [%(name)s] [%(levelname)s] %(message)s')

_LOG_DIR = pathlib.Path(os.path.join('log'))

if not _LOG_DIR.exists():
    _LOG_DIR.mkdir()


def get_logger(name: str) -> logging.Logger:
    """
    Log to file using logging.FileHandler
    Log to console also.
    Also set level or severity to the events tracked.

    Args:
        name (str): Name of the logger.

    Raises:
        ValueError: If level of severity is not supported

    Returns:
        logger (logging.Logger): Logger

    """
    log_file = _LOG_DIR / _FILE_FMT.format(_TIME_FMT)

    logger = getLogger(name)

    # for file output
    fout = FileHandler(filename=str(log_file), mode='a')
    fout.setFormatter(_FORMATTER)
    logger.addHandler(fout)

    # for stdout
    stdout = StreamHandler()
    stdout.setFormatter(_FORMATTER)
    logger.addHandler(stdout)

    # level = CONFIG['log']['level'].upper()
    level = 'DEBUG'
    if level == 'INFO':
        logger.setLevel(INFO)
    elif level == 'DEBUG':
        logger.setLevel(DEBUG)
    else:
        msg = 'log level must be either INFO or DEBUG, given: {}'
        raise ValueError(msg.format(level))
    return logger


def print_info() -> NoReturn:
    """
    Print out logs for platform, and paths in this app.

    Returns:
        None

    """
    logger = get_logger(__name__)

    # platform info
    logger.info('Platform: {}'.format(platform.platform()))
    logger.info('Processor: {}'.format(platform.processor()))

    # app info
    logger.info('Working directory: {}'.format(pathlib.Path.cwd()))
    logger.info('Log directory: {}'.format(_LOG_DIR))
    p = pathlib.Path(__file__)
    logger.info('Library path: {}'.format(p.parent.parent))


@contextmanager
def output_performance(logger, process_name: str) -> NoReturn:
    """
    Under influence of @contextmanager, enableing __enter__() and __exit__(),
        which can be used with `with`, and release resource after execution.
    Logs execution time.

    Args:
        logger (logging.Logger): Logger object
        process_name (str): Process name

    Returns:
        None

    """
    ts = time.perf_counter()
    yield
    te = time.perf_counter()

    msg = 'Execution time of {}: {:.5f} (sec)'
    logger.debug(msg.format(process_name, te - ts))


@contextmanager
def output_progress(logger, process_name: str, skip_on_error: bool = False) -> NoReturn:
    """
    Under influence of @contextmanager, enableing __enter__() and __exit__(),
        which can be used with `with`, and release resource after execution.
    Logs which processes skipped.

    Args:
        logger (logging.Logger): Logger object
        process_name (str): Process name
        skip_on_error (bool): False. If false, raise exception when error

    Raises:
        Exception

    Returns:
        None

    """
    logger.info('Started {}'.format(process_name))
    try:
        yield
    except Exception as e:
        if skip_on_error:
            msg = 'Unhandled {} is raised: {}'
            logger.error(msg.format(e.__class__.__name__, e))
            # dump stack trace
            logger.exception(e)
            logger.info('Skipped {}'.format(process_name))
            return
        else:
            raise

    logger.info('Finished {}'.format(process_name))
