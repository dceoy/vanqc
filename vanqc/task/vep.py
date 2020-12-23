#!/usr/bin/env python

import re
from pathlib import Path

import luigi

from .bcftools import NormalizeVcf
from .core import VanqcTask


class DownloadEnsemblVepCache(VanqcTask):
    dest_dir_path = luigi.Parameter(default='.')
    genome_version = luigi.Parameter(default='GRCh38')
    species = luigi.Parameter(default='homo_sapiens')
    vep = luigi.Parameter(default='vep')
    wget = luigi.Parameter(default='wget')
    pigz = luigi.Parameter(default='pigz')
    n_cpu = luigi.IntParameter(default=1)
    sh_config = luigi.DictParameter(default=dict())
    priority = 10

    def output(self):
        return luigi.LocalTarget(
            Path(self.dest_dir_path).resolve().joinpath('vep_cache').joinpath(
                self.species
            )
        )

    def run(self):
        output_target = Path(self.output().path)
        self.print_log(f'Download VEP cache data:\t{output_target}')
        dest_dir = output_target.parent
        tar = dest_dir.joinpath(output_target.name + '.tar.gz')
        self.setup_shell(
            run_id=Path(tar.stem).stem, commands=[self.wget, self.pigz],
            cwd=dest_dir, **self.sh_config
        )
        self.run_shell(
            args=(
                f'set -e && {self.vep} --help'
                + ' | grep -oe \'ensembl-vep \\+: \\+[0-9]\\+\''
                + ' | grep -oe \'[0-9]\\+$\''
                + f' | xargs -I @ {self.wget} -qSL -O {tar} '
                + 'ftp://ftp.ensembl.org/'
                + 'pub/release-@/variation/indexed_vep_cache/'
                + f'homo_sapiens_vep_@_{self.genome_version}.tar.gz'
            ),
            output_files_or_dirs=tar
        )
        self.tar_xf(
            tar_path=tar, dest_dir_path=dest_dir, pigz=self.pigz,
            n_cpu=self.n_cpu, remove_tar=True,
            output_files_or_dirs=output_target
        )


class AnnotateVariantsWithEnsemblVep(VanqcTask):
    input_vcf_path = luigi.Parameter()
    fa_path = luigi.Parameter()
    cache_dir_path = luigi.Parameter()
    dest_dir_path = luigi.Parameter(default='.')
    species = luigi.Parameter(default='homo_sapiens')
    normalize_vcf = luigi.BoolParameter(default=False)
    norm_dir_path = luigi.Parameter(default='')
    bcftools = luigi.Parameter(default='bcftools')
    vep = luigi.Parameter(default='vep')
    pigz = luigi.Parameter(default='pigz')
    n_cpu = luigi.IntParameter(default=1)
    memory_mb = luigi.FloatParameter(default=4096)
    sh_config = luigi.DictParameter(default=dict())
    priority = 10

    def requires(self):
        if self.normalize_vcf:
            return NormalizeVcf(
                input_vcf_path=self.input_vcf_path, fa_path=self.fa_path,
                dest_dir_path=(self.norm_dir_path or self.dest_dir_path),
                n_cpu=self.n_cpu, memory_mb=self.memory_mb,
                bcftools=self.bcftools, sh_config=self.sh_config
            )
        else:
            return super().requires()

    def output(self):
        return luigi.LocalTarget(
            Path(self.dest_dir_path).resolve().joinpath(
                re.sub(
                    r'\.vcf$', '',
                    Path(
                        self.input()[0].path if self.normalize_vcf
                        else self.input_vcf_path
                    ).stem
                ) + '.ensembl_vep.txt.gz'
            )
        )

    def run(self):
        input_vcf = Path(
            self.input()[0].path if self.normalize_vcf else self.input_vcf_path
        ).resolve()
        run_id = Path(input_vcf.stem).stem
        self.print_log(f'Annotate variants with Ensembl VEP:\t{run_id}')
        cache_dir = Path(self.cache_dir_path).resolve()
        output_txt = Path(self.output().path)
        dest_dir = output_txt.parent
        tmp_txt = dest_dir.joinpath(output_txt.stem)
        self.setup_shell(
            run_id=run_id, commands=[self.ensembl_vep, self.pigz],
            cwd=dest_dir, **self.sh_config
        )
        self.run_shell(
            args=(
                f'set -e && {self.ensembl_vep}'
                + f' --cache --species {self.species}'
                + f' --dir {cache_dir}'
                + f' --input_file {input_vcf}'
                + f' --output_file {tmp_txt}'
            ),
            input_files_or_dirs=[input_vcf, cache_dir],
            output_files_or_dirs=tmp_txt
        )
        self.run_shell(
            args=f'set -e && {self.pigz} -p {self.n_cpu} {tmp_txt}',
            input_files_or_dirs=tmp_txt, output_files_or_dirs=output_txt
        )


if __name__ == '__main__':
    luigi.run()
