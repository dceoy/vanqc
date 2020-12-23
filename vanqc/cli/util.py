#!/usr/bin/env python

import logging
import os
from pathlib import Path

import luigi


def print_log(message):
    logger = logging.getLogger(__name__)
    logger.debug(message)
    print(f'>>\t{message}', flush=True)


def fetch_executable(cmd, ignore_errors=False):
    executables = [
        cp for cp in [
            str(Path(p).joinpath(cmd))
            for p in os.environ['PATH'].split(os.pathsep)
        ] if os.access(cp, os.X_OK)
    ]
    if executables:
        return executables[0]
    elif ignore_errors:
        return None
    else:
        raise RuntimeError(f'command not found: {cmd}')


def build_luigi_tasks(*args, **kwargs):
    r = luigi.build(
        *args,
        **{
            k: v for k, v in kwargs.items() if (
                k not in {'logging_conf_file', 'hide_summary'}
                or (k == 'logging_conf_file' and v)
            )
        },
        local_scheduler=True, detailed_summary=True
    )
    if not kwargs.get('hide_summary'):
        print(
            os.linesep
            + os.linesep.join(['Execution summary:', r.summary_text, str(r)])
        )
