#!/usr/bin/env python

import re
from pathlib import Path

import luigi

from .core import VanqcTask


class DownloadEnsemblVepCache(VanqcTask):
    src_url = luigi.ListParameter()
    dest_dir_path = luigi.Parameter(default='.')
    extract_tar = luigi.BoolParameter(default=True)
    wget = luigi.Parameter(default='wget')
    pigz = luigi.Parameter(default='pigz')
    pbzip2 = luigi.Parameter(default='pbzip2')
    n_cpu = luigi.IntParameter(default=1)
    sh_config = luigi.DictParameter(default=dict())
    priority = 10

    def requires(self):
        return DownloadResourceFiles(
            src_urls=[self.src_url], dest_dir_path=self.dest_dir_path,
            n_cpu=self.n_cpu, wget=self.wget, bgzip=self.bgzip,
            sh_config=self.sh_config
        )

    def output(self):
        tar_path = self.input()[0].path
        return luigi.LocalTarget(
            Path(self.dest_dir_path).resolve().joinpath(
                re.sub(r'_vep_.*$', '',  Path(tar_path).stem)
            ) if self.extract_tar else tar_path
        )

    def run(self):
        tar = Path(self.input()[0].path)
        self.print_log(f'Extract tar files:\t{tar}')
        output_dir = Path(self.output().path)
        self.setup_shell(
            run_id=tar.stem, commands=[self.pigz, self.pbzip2],
            cwd=output_dir.parent, **self.sh_config
        )
        self.tar_xf(
            tar_path=tar, dest_dir_path=output_dir, pigz=self.pigz,
            pbzip2=self.pbzip2, n_cpu=self.n_cpu, remove_tar=True
        )


class DownloadResourceFiles(VanqcTask):
    src_urls = luigi.ListParameter()
    dest_dir_path = luigi.Parameter(default='.')
    run_id = luigi.Parameter(default='data')
    wget = luigi.Parameter(default='wget')
    bgzip = luigi.Parameter(default='bgzip')
    n_cpu = luigi.IntParameter(default=1)
    sh_config = luigi.DictParameter(default=dict())
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
        dest_dir = Path(self.dest_dir_path).resolve()
        self.print_log(f'Download resource files:\t{dest_dir}')
        self.setup_shell(
            run_id=self.run_id, commands=[self.wget, self.bgzip], cwd=dest_dir,
            **self.sh_config
        )
        for u, o in zip(self.src_urls, self.output()):
            t = dest_dir.joinpath(
                (Path(u).stem + '.gz') if u.endswith('.bgz') else Path(u).name
            )
            self.run_shell(
                args=f'set -e && {self.wget} -qSL {u} -O {t}',
                output_files_or_dirs=t
            )
            if t.suffix != '.gz' and o.path.endswith('.gz'):
                self.run_shell(
                    args=f'set -e && {self.bgzip} -@ {self.n_cpu} {t}',
                    input_files_or_dirs=t, output_files_or_dirs=o.path
                )


if __name__ == '__main__':
    luigi.run()
