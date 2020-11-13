#!/usr/bin/env python

from pathlib import Path

import luigi

from .core import VanqcTask
from .gatk import generate_gatk_java_options


class CollectVariantCallingMetrics(VanqcTask):
    input_vcf_path = luigi.Parameter()
    dbsnp_vcf_path = luigi.Parameter()
    fa_path = luigi.Parameter()
    dest_dir_path = luigi.Parameter(default='.')
    picard = luigi.Parameter(default='picard')
    n_cpu = luigi.IntParameter(default=1)
    memory_mb = luigi.FloatParameter(default=4096)
    log_dir_path = luigi.Parameter(default='')
    remove_if_failed = luigi.BoolParameter(default=True)
    quiet = luigi.BoolParameter(default=False)
    priority = 10

    def output(self):
        return luigi.LocalTarget(
            Path(self.dest_dir_path).resolve().joinpath(
                Path(Path(self.input_vcf_path).stem).stem
                + '.CollectVariantCallingMetrics.txt'
            )
        )

    def run(self):
        input_vcf = Path(self.input_vcf_path).resolve()
        run_id = Path(input_vcf.stem).stem
        self.print_log(f'Collect variant calling metrics:\t{run_id}')
        dbsnp_vcf = Path(self.dbsnp_vcf_path).resolve()
        fa = Path(self.fa_path).resolve()
        fa_dict = fa.parent.joinpath(f'{fa.stem}.dict')
        output_txt = Path(self.output().path)
        self.setup_shell(
            run_id=run_id, log_dir_path=self.log_dir_path,
            commands=self.picard, cwd=input_vcf.parent,
            remove_if_failed=self.remove_if_failed, quiet=self.quiet,
            env={
                'JAVA_TOOL_OPTIONS': generate_gatk_java_options(
                    n_cpu=self.n_cpu, memory_mb=self.memory_mb
                )
            }
        )
        self.run_shell(
            args=(
                f'set -e && {self.picard} CollectVariantCallingMetrics'
                + f' --INPUT {input_vcf}'
                + f' --DBSNP {dbsnp_vcf}'
                + F' --REFERENCE_SEQUENCE {fa}'
                + f' --SEQUENCE_DICTIONARY {fa_dict}'
                + F' --OUTPUT {output_txt}'
            ),
            input_files_or_dirs=[input_vcf, fa, f'{fa}.fai', fa_dict],
            output_files_or_dirs=output_txt
        )


if __name__ == '__main__':
    luigi.run()
