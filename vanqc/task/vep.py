#!/usr/bin/env python

import re
from pathlib import Path

import luigi

from .core import VanqcTask


class DownloadTar(VanqcTask):
    src_url = luigi.ListParameter()
    dest_dir_path = luigi.Parameter(default='.')
    extract_tar = luigi.BoolParameter(default=True)
    n_cpu = luigi.IntParameter(default=1)
    wget = luigi.Parameter(default='wget')
    pbzip2 = luigi.Parameter(default='pbzip2')
    pigz = luigi.Parameter(default='pigz')
    log_dir_path = luigi.Parameter(default='')
    remove_if_failed = luigi.BoolParameter(default=True)
    quiet = luigi.BoolParameter(default=False)
    priority = 10

    def requires(self):
        return DownloadResourceFiles(
            src_urls=[self.src_url], dest_dir_path=self.dest_dir_path,
            n_cpu=self.n_cpu, wget=self.wget, bgzip=self.bgzip,
            log_dir_path=self.log_dir_path,
            remove_if_failed=self.remove_if_failed, quiet=self.quiet
        )

    def output(self):
        tar_path = self.input()[0].path
        return luigi.LocalTarget(
            Path(self.dest_dir_path).resolve().joinpath(
                Path(Path(tar_path).stem).stem
            ) if self.extract_tar else tar_path
        )

    def run(self):
        run_id = 'data'
        self.print_log(f'Extract tar files:\t{run_id}')
        tar = Path(self.input()[0].path)
        output_dir = Path(self.output().path)
        self.setup_shell(
            run_id=run_id, log_dir_path=self.log_dir_path,
            commands=[self.pigz, self.pbzip2], cwd=output_dir.parent,
            remove_if_failed=self.remove_if_failed, quiet=self.quiet
        )
        self.tar_xf(
            tar_path=tar, dest_dir_path=output_dir, pigz=self.pigz,
            pbzip2=self.pbzip2, n_cpu=self.n_cpu,
            remove_tar=self.remove_tar_files
        )


class DownloadResourceFiles(VanqcTask):
    src_urls = luigi.ListParameter()
    dest_dir_path = luigi.Parameter(default='.')
    n_cpu = luigi.IntParameter(default=1)
    wget = luigi.Parameter(default='wget')
    bgzip = luigi.Parameter(default='bgzip')
    log_dir_path = luigi.Parameter(default='')
    remove_if_failed = luigi.BoolParameter(default=True)
    quiet = luigi.BoolParameter(default=False)
    priority = 10

    def output(self):
        dest_dir = Path(self.dest_dir_path)
        for u in self.src_urls:
            p = str(dest_dir.joinpath(Path(u).name))
            if u.endswith('.bgz'):
                yield luigi.LocalTarget(re.sub(r'\.bgz$', '.gz', p))
            elif u.endswith(('.vcf', '.bed')):
                yield luigi.LocalTarget(f'{p}.gz')
            else:
                yield luigi.LocalTarget(p)

    def run(self):
        run_id = 'data'
        self.print_log(f'Download resource files:\t{run_id}')
        dest_dir = Path(self.dest_dir_path).resolve()
        self.setup_shell(
            run_id=run_id, log_dir_path=self.log_dir_path,
            commands=[self.wget, self.bgzip], cwd=dest_dir,
            remove_if_failed=self.remove_if_failed, quiet=self.quiet
        )
        for u, o in zip(self.src_urls, self.output()):
            t = dest_dir.joinpath(
                (Path(u).stem + '.gz') if u.endswith('.bgz') else Path(u).name
            )
            p = o.path
            self.run_shell(
                args=f'set -e && {self.wget} -qSL {u} -O {t}',
                output_files_or_dirs=t
            )
            if t == p:
                pass
            elif p.endswith('.gz'):
                self.run_shell(
                    args=f'set -e && {self.bgzip} -@ {self.n_cpu} {t}',
                    input_files_or_dirs=t, output_files_or_dirs=p
                )


if __name__ == '__main__':
    luigi.run()
