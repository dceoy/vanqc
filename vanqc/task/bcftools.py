#!/usr/bin/env python

import re
from pathlib import Path

import luigi

from .core import VanqcTask


class NormalizeVCF(VanqcTask):
    input_vcf_path = luigi.Parameter()
    fa_path = luigi.Parameter()
    dest_dir_path = luigi.Parameter(default='.')
    bcftools = luigi.Parameter(default='bcftools')
    n_cpu = luigi.IntParameter(default=1)
    memory_mb = luigi.FloatParameter(default=4096)
    log_dir_path = luigi.Parameter(default='')
    remove_if_failed = luigi.BoolParameter(default=True)
    quiet = luigi.BoolParameter(default=False)
    priority = 10

    def output(self):
        output_vcf = Path(self.dest_dir_path).resolve().joinpath(
            re.sub(r'\.vcf$', '', Path(self.input_vcf_path).stem)
            + '.norm.vcf.gz'
        )
        return [luigi.LocalTarget(f'{output_vcf}{s}') for s in ['', '.tbi']]

    def run(self):
        input_vcf = Path(self.input_vcf_path).resolve()
        run_id = Path(input_vcf.stem).stem
        fa = Path(self.fa_path).resolve()
        output_vcf = Path(self.output()[0].path)
        self.print_log(f'Normalize VCF:\t{run_id}')
        self.setup_shell(
            run_id=run_id, log_dir_path=self.log_dir_path,
            commands=self.bcftools, cwd=output_vcf.parent,
            remove_if_failed=self.remove_if_failed, quiet=self.quiet
        )
        self.run_shell(
            args=(
                f'set -eo pipefail && {self.bcftools} reheader'
                + f' --fai {self.fa_path}.fai --threads {self.n_cpu}'
                + f' {input_vcf}'
                + f' | {self.bcftools} sort'
                + ' --max-mem {}M'.format(int(self.memory_mb))
                + f' --temp-dir {output_vcf}.sort -'
                + f' | {self.bcftools} norm --fasta-ref {fa}'
                + ' --check-ref w --rm-dup exact --output-type z'
                + f' --threads {self.n_cpu} --output {output_vcf} -'
            ),
            input_files_or_dirs=[input_vcf, fa, f'{fa}.fai'],
            output_files_or_dirs=output_vcf
        )
        self.run_shell(
            args=(
                f'set -e && {self.bcftools} index --threads {self.n_cpu}'
                + f' --tbi {output_vcf}'
            ),
            input_files_or_dirs=output_vcf,
            output_files_or_dirs=f'{output_vcf}.tbi'
        )


if __name__ == '__main__':
    luigi.run()
