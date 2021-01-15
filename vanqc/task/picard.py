#!/usr/bin/env python

from pathlib import Path

import luigi

from .core import VanqcTask


class CollectVariantCallingMetrics(VanqcTask):
    input_vcf_path = luigi.Parameter()
    fa_path = luigi.Parameter()
    dbsnp_vcf_path = luigi.Parameter()
    dest_dir_path = luigi.Parameter(default='.')
    picard = luigi.Parameter(default='picard')
    n_cpu = luigi.IntParameter(default=1)
    memory_mb = luigi.FloatParameter(default=4096)
    sh_config = luigi.DictParameter(default=dict())
    priority = 10

    def output(self):
        return luigi.LocalTarget(
            Path(self.dest_dir_path).resolve().joinpath(
                Path(Path(self.input_vcf_path).stem).stem
                + '.CollectVariantCallingMetrics.txt'
            )
        )

    def run(self):
        target_vcf = Path(self.input_vcf_path)
        run_id = Path(target_vcf.stem).stem
        self.print_log(f'Collect variant calling metrics:\t{run_id}')
        input_vcf = target_vcf.resolve()
        dbsnp_vcf = Path(self.dbsnp_vcf_path).resolve()
        fa = Path(self.fa_path).resolve()
        fa_dict = fa.parent.joinpath(f'{fa.stem}.dict')
        output_txt = Path(self.output().path)
        self.setup_shell(
            run_id=run_id, commands=self.picard, cwd=input_vcf.parent,
            **self.sh_config,
            env={
                'JAVA_TOOL_OPTIONS': self.generate_gatk_java_options(
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
