#!/usr/bin/env python

import re
from pathlib import Path

import luigi

from .base import ShellTask
from .bcftools import NormalizeVCF


class DownloadSnpeffDataSource(ShellTask):
    dest_dir_path = luigi.Parameter(default='.')
    snpeff = luigi.Parameter(default='snpEff')
    genome_version = luigi.Parameter(default='GRCh38')
    snpeff_config_path = luigi.Parameter(default='')
    memory_mb = luigi.FloatParameter(default=4096)
    log_dir_path = luigi.Parameter(default='')
    quiet = luigi.BoolParameter(default=False)

    def output(self):
        dir_data_paths = self._fetch_existing_snpeff_data()
        if dir_data_paths:
            return luigi.LocalTarget(dir_data_paths[0])
        else:
            return super().output()

    def complete(self):
        return bool(self._fetch_existing_snpeff_data())

    def _fetch_existing_snpeff_data(self):
        dest_dir = Path(self.dest_dir_path).resolve()
        snpeff_config = dest_dir.joinpath('snpEff.config')
        if not snpeff_config.is_file():
            return list()
        else:
            data_dir = dest_dir.joinpath('data')
            with open(snpeff_config, 'r') as f:
                for s in f:
                    if re.match(r'\s*data\.dir\s*=\s*\./data/', s):
                        data_dir = dest_dir.joinpath(
                            re.sub(r'\s*data\.dir\s*=\s*', '', s.strip())
                        )
                        break
            if not data_dir.is_dir():
                return list()
            else:
                return [
                    str(o) for o in data_dir.iterdir()
                    if o.name.startswith(self.genome_version) and o.is_dir()
                ]

    def run(self):
        dest_dir = Path(self.dest_dir_path).resolve()
        self.print_log(f'Download SnpEff data source:\t{dest_dir}')
        if self.snpeff_config_path:
            src_config = Path(self.snpeff_config_path).resolve()
        else:
            src_config = (
                Path(self.snpeff.split(' ')[-1]).resolve().parent
                if self.snpeff.endswith('.jar') else
                Path(self.snpeff).resolve().parent.parent
            ).joinpath('snpEff.config')
        dest_config = dest_dir.joinpath(src_config.name)
        self.setup_shell(
            run_id=dest_dir.name, log_dir_path=self.log_dir_path,
            commands=self.snpeff, cwd=dest_dir, quiet=self.quiet,
            env={'JAVA_TOOL_OPTIONS': '-Xmx{}m'.format(int(self.memory_mb))}
        )
        self.run_shell(
            args=[
                f'set -e && cp {src_config} {dest_config}',
                (
                    'set -e && '
                    + f'{self.snpeff} databases'
                    + f' | grep -e "^{self.genome_version}[\\.0-9]*\\s"'
                    + ' | cut -f 1'
                    + f' | xargs {self.snpeff} download'
                    + f' -verbose -config {dest_config}'
                )
            ],
            input_files_or_dirs=src_config,
            output_files_or_dirs=dest_config
        )


class AnnotateVcfWithSnpeff(ShellTask):
    input_vcf_path = luigi.Parameter()
    fa_path = luigi.Parameter()
    snpeff_config_path = luigi.Parameter()
    dest_dir_path = luigi.Parameter(default='.')
    ref_version = luigi.Parameter(default='hg38')
    snpeff_genome_version = luigi.Parameter(default='')
    normalize_vcf = luigi.BoolParameter(default=False)
    norm_dir_path = luigi.Parameter(default='')
    bcftools = luigi.Parameter(default='bcftools')
    snpeff = luigi.Parameter(default='snpeff')
    bgzip = luigi.Parameter(default='bgzip')
    tabix = luigi.Parameter(default='tabix')
    n_cpu = luigi.IntParameter(default=1)
    memory_mb = luigi.FloatParameter(default=4096)
    log_dir_path = luigi.Parameter(default='')
    remove_if_failed = luigi.BoolParameter(default=True)
    quiet = luigi.BoolParameter(default=False)
    priority = 10

    def requires(self):
        if self.normalize_vcf:
            return NormalizeVCF(
                input_vcf_path=self.input_vcf_path, fa_path=self.fa_path,
                dest_dir_path=(self.norm_dir_path or self.dest_dir_path),
                n_cpu=self.n_cpu, memory_mb=self.memory_mb,
                bcftools=self.bcftools, log_dir_path=self.log_dir_path,
                remove_if_failed=self.remove_if_failed, quiet=self.quiet
            )
        else:
            return super().requires()

    def output(self):
        output_vcf = Path(self.dest_dir_path).resolve().joinpath(
            re.sub(
                r'\.vcf$', '',
                Path(
                    self.input()[0].path if self.normalize_vcf
                    else self.input_vcf_path
                ).stem
            ) + '.snpeff.vcf.gz'
        )
        return [luigi.LocalTarget(f'{output_vcf}{s}') for s in ['', '.tbi']]

    def run(self):
        input_vcf = Path(
            self.input()[0].path if self.normalize_vcf else self.input_vcf_path
        ).resolve()
        run_id = Path(input_vcf.stem).stem
        self.print_log(f'Annotate variants with SnpEff:\t{run_id}')
        snpeff_config = Path(self.snpeff_config_path).resolve()
        output_vcf = Path(self.output()[0].path)
        dest_dir = output_vcf.parent
        tmp_dir = dest_dir.joinpath(run_id)
        tmp_files = [
            tmp_dir.joinpath(n) for n
            in ['snpeff.vcf.gz', 'snpEff_genes.txt', 'snpEff_summary.html']
        ]
        genome_version = (
            self.snpeff_genome_version or [
                o.name for o in snpeff_config.parent.joinpath('data').iterdir()
                if (
                    o.name.startswith(
                        {'hg38': 'GRCh38', 'hg19': 'GRCh37'}[self.ref_version]
                    ) and o.is_dir()
                )
            ][0]
        )
        self.setup_shell(
            run_id=run_id, log_dir_path=self.log_dir_path,
            commands=[self.snpeff, self.bgzip, self.tabix], cwd=dest_dir,
            remove_if_failed=self.remove_if_failed, quiet=self.quiet,
            env={'JAVA_TOOL_OPTIONS': '-Xmx{}m'.format(int(self.memory_mb))}
        )
        self.run_shell(args=f'mkdir {tmp_dir}', output_files_or_dirs=tmp_dir)
        self.run_shell(
            args=(
                f'set -eo pipefail && cd {tmp_dir} && '
                + f'{self.snpeff} -verbose -config {snpeff_config}'
                + f' {genome_version} {input_vcf}'
                + f' | {self.bgzip} -@ {self.n_cpu} -c > {tmp_files[0]}'
            ),
            input_files_or_dirs=[input_vcf, tmp_dir],
            output_files_or_dirs=[tmp_files[0], tmp_dir]
        )
        for t in tmp_files:
            if t.is_file():
                o = dest_dir.joinpath(f'{run_id}.{t.name}')
                self.run_shell(
                    args=f'mv {t} {o}', input_files_or_dirs=t,
                    output_files_or_dirs=o
                )
        self.run_shell(args=f'rm -rf {tmp_dir}', input_files_or_dirs=tmp_dir)
        _tabix_tbi(
            shelltask=self, tabix=self.tabix, tsv_path=str(output_vcf),
            preset='vcf'
        )


def _tabix_tbi(shelltask, tabix, tsv_path, preset='vcf'):
    shelltask.run_shell(
        args=f'set -e && {tabix} --preset {preset} {tsv_path}',
        input_files_or_dirs=tsv_path, output_files_or_dirs=f'{tsv_path}.tbi'
    )


if __name__ == '__main__':
    luigi.run()
