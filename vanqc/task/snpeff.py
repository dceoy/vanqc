#!/usr/bin/env python

import re
from pathlib import Path

import luigi

from .bcftools import NormalizeVcf
from .core import VanqcTask


class DownloadSnpeffDataSources(VanqcTask):
    dest_dir_path = luigi.Parameter(default='.')
    snpeff = luigi.Parameter(default='snpEff')
    genome_version = luigi.Parameter(default='GRCh38')
    data_dir_name = luigi.Parameter(default='snpeff_data')
    snpeff_db = luigi.Parameter(default='')
    memory_mb = luigi.FloatParameter(default=4096)
    log_dir_path = luigi.Parameter(default='')
    quiet = luigi.BoolParameter(default=False)
    priority = 10

    def output(self):
        return luigi.LocalTarget(
            Path(self.dest_dir_path).resolve().joinpath(self.data_dir_name)
        )

    def run(self):
        self.print_log(f'Download SnpEff data sources:\t{self.data_dir_name}')
        data_dir = Path(self.output().path)
        self.setup_shell(
            run_id=self.data_dir_name, log_dir_path=self.log_dir_path,
            commands=self.snpeff, cwd=data_dir.parent, quiet=self.quiet,
            env={'JAVA_TOOL_OPTIONS': '-Xmx{}m'.format(int(self.memory_mb))}
        )
        self.run_shell(
            args=(
                (
                    f'set -e && {self.snpeff} download'
                    + f' -verbose -configOption data.dir={data_dir}'
                    + f' {self.snpeff_db}'
                ) if self.snpeff_db else (
                    f'set -eo pipefail && {self.snpeff} databases'
                    + f' | grep -e "^{self.genome_version}[\\.0-9]*\\s"'
                    + ' | cut -f 1'
                    + f' | xargs {self.snpeff} download'
                    + f' -verbose -configOption data.dir={data_dir}'
                )
            ),
            output_files_or_dirs=data_dir
        )


class AnnotateVcfWithSnpeff(VanqcTask):
    input_vcf_path = luigi.Parameter()
    fa_path = luigi.Parameter()
    data_dir_path = luigi.Parameter()
    dest_dir_path = luigi.Parameter(default='.')
    ref_version = luigi.Parameter(default='hg38')
    snpeff_db = luigi.Parameter(default='')
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
            return NormalizeVcf(
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
        data_dir = Path(self.data_dir_path).resolve()
        output_vcf = Path(self.output()[0].path)
        dest_dir = output_vcf.parent
        tmp_dir = dest_dir.joinpath(run_id)
        tmp_files = [
            tmp_dir.joinpath(n) for n
            in ['snpeff.vcf.gz', 'snpEff_genes.txt', 'snpEff_summary.html']
        ]
        genome_version = (
            self.snpeff_db or [
                o.name for o in data_dir.iterdir() if (
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
                + f'{self.snpeff} -verbose -configOption data.dir={data_dir}'
                + f' {genome_version} {input_vcf}'
                + f' | {self.bgzip} -@ {self.n_cpu} -c > {tmp_files[0]}'
            ),
            input_files_or_dirs=[input_vcf, data_dir, tmp_dir],
            output_files_or_dirs=[tmp_files[0], tmp_dir]
        )
        for t in tmp_files:
            if t.is_file():
                o = dest_dir.joinpath(f'{run_id}.{t.name}')
                self.run_shell(
                    args=f'mv {t} {o}', input_files_or_dirs=t,
                    output_files_or_dirs=o
                )
        self.remove_files_and_dirs(tmp_dir)
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
