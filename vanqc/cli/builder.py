#!/usr/bin/env python

import os

import luigi


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
