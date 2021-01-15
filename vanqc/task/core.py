#!/usr/bin/env python

from pathlib import Path

from ftarc.task.core import ShellTask


class VanqcTask(ShellTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def generate_version_commands(commands):
        for c in ([commands] if isinstance(commands, str) else commands):
            n = Path(c).name
            if n in {'java', 'snpEff'} or n.endswith('.jar'):
                yield f'{c} -version'
            elif n == 'wget':
                yield f'{c} --version | head -1'
            elif n == 'vep':
                yield f'{c} | grep -6 -e "Versions:"'
            else:
                yield f'{c} --version'

    @staticmethod
    def generate_gatk_java_options(n_cpu=1, memory_mb=4096):
        return ' '.join([
            '-Xmx{}m'.format(int(memory_mb)), '-XX:+UseParallelGC',
            '-XX:ParallelGCThreads={}'.format(int(n_cpu))
        ])

    @classmethod
    def tabix_tbi(cls, tsv_path, tabix='tabix', preset='vcf', **kwargs):
        cls.run_shell(
            args=f'set -e && {tabix} --preset {preset} {tsv_path}',
            input_files_or_dirs=tsv_path,
            output_files_or_dirs=f'{tsv_path}.tbi', **kwargs
        )

    @classmethod
    def tar_xf(cls, tar_path, dest_dir_path, remove_tar=True, **kwargs):
        cls.run_shell(
            args=f'set -e && tar xvf {tar_path}', cwd=dest_dir_path,
            input_files_or_dirs=tar_path, **kwargs
        )
        if remove_tar:
            cls.remove_files_and_dirs(tar_path)
