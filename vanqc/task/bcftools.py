#!/usr/bin/env python

import re
from pathlib import Path

import luigi

from .core import VanqcTask


class NormalizeVcf(VanqcTask):
    input_vcf_path = luigi.Parameter()
    fa_path = luigi.Parameter()
    dest_dir_path = luigi.Parameter(default='.')
    bcftools = luigi.Parameter(default='bcftools')
    tabix = luigi.Parameter(default='')
    add_norm_args = luigi.ListParameter(
        default=['--check-ref', 'w', '--rm-dup', 'exact']
    )
    n_cpu = luigi.IntParameter(default=1)
    memory_mb = luigi.FloatParameter(default=4096)
    sh_config = luigi.DictParameter(default=dict())
    priority = 10

    def output(self):
        output_vcf = Path(self.dest_dir_path).resolve().joinpath(
            re.sub(r'\.vcf$', '', Path(self.input_vcf_path).stem)
            + '.norm.vcf.gz'
        )
        return [luigi.LocalTarget(f'{output_vcf}{s}') for s in ['', '.tbi']]

    def run(self):
        run_id = Path(self.input_vcf_path).stem
        self.print_log(f'Normalize VCF:\t{run_id}')
        input_vcf = Path(self.input_vcf_path).resolve()
        fa = Path(self.fa_path).resolve()
        output_vcf = Path(self.output()[0].path)
        self.setup_shell(
            run_id=run_id, commands=self.bcftools, cwd=output_vcf.parent,
            **self.sh_config
        )
        self.run_shell(
            args=(
                f'set -eo pipefail && {self.bcftools} reheader'
                + f' --fai {self.fa_path}.fai'
                + f' --threads {self.n_cpu}'
                + f' {input_vcf}'
                + f' | {self.bcftools} sort'
                + ' --max-mem {}M'.format(int(self.memory_mb))
                + f' --temp-dir {output_vcf}.sort -'
                + f' | {self.bcftools} norm'
                + f' --fasta-ref {fa}'
                + f' --threads {self.n_cpu}'
                + ' --output-type z'
                + ''.join(f' {a}' for a in self.add_norm_args)
                + f' --output {output_vcf} -'
            ),
            input_files_or_dirs=[input_vcf, fa, f'{fa}.fai'],
            output_files_or_dirs=output_vcf
        )
        if self.tabix:
            self.tabix_tbi(tsv_path=output_vcf, tabix=self.tabix, preset='vcf')
        else:
            self.run_shell(
                args=(
                    f'set -e && {self.bcftools} index --threads {self.n_cpu}'
                    + f' --tbi {output_vcf}'
                ),
                input_files_or_dirs=output_vcf,
                output_files_or_dirs=f'{output_vcf}.tbi'
            )


class CollectVcfStats(VanqcTask):
    input_vcf_path = luigi.Parameter()
    fa_path = luigi.Parameter()
    dest_dir_path = luigi.Parameter(default='.')
    bcftools = luigi.Parameter(default='bcftools')
    plot_vcfstats = luigi.Parameter(default='plot-vcfstats')
    perl = luigi.Parameter(default='perl')
    python3 = luigi.Parameter(default='python3')
    pdflatex = luigi.Parameter(default='pdflatex')
    add_stats_args = luigi.ListParameter(default=list())
    add_plot_vcfstats_args = luigi.ListParameter(default=list())
    n_cpu = luigi.IntParameter(default=1)
    sh_config = luigi.DictParameter(default=dict())
    priority = 10

    def output(self):
        output_path_prefix = str(
            Path(self.dest_dir_path).resolve().joinpath(
                Path(self.input_vcf_path).stem + '.stats'
            )
        )
        return [
            luigi.LocalTarget(output_path_prefix + s) for s in ['.txt', '']
        ]

    def run(self):
        run_id = Path(self.input_vcf_path).stem
        self.print_log(f'Collect VCF stats:\t{run_id}')
        input_vcf = Path(self.input_vcf_path).resolve()
        fa = Path(self.fa_path).resolve()
        output_txt = Path(self.output()[0].path)
        plot_dir = Path(self.output()[1].path)
        self.setup_shell(
            run_id=run_id,
            commands=[self.bcftools, self.perl, self.python3, self.pdflatex],
            cwd=output_txt.parent, **self.sh_config
        )
        self.run_shell(
            args=(
                f'set -e && {self.bcftools} stats'
                + f' --fasta-ref {fa}'
                + f' --threads {self.n_cpu}'
                + ''.join(f' {a}' for a in self.add_stats_args)
                + f' {input_vcf} > {output_txt}'
            ),
            input_files_or_dirs=[input_vcf, fa, f'{fa}.fai'],
            output_files_or_dirs=output_txt
        )
        self.run_shell(
            args=(
                f'set -e && {self.plot_vcfstats}'
                + ''.join(f' {a}' for a in self.add_plot_vcfstats_args)
                + f' --prefix {plot_dir}'
                + f' {output_txt}'
            ),
            input_files_or_dirs=output_txt,
            output_files_or_dirs=[plot_dir, plot_dir.joinpath('summary.pdf')]
        )


if __name__ == '__main__':
    luigi.run()
