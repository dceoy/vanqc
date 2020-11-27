#!/usr/bin/env python

import re
from pathlib import Path

import luigi

from .bcftools import NormalizeVcf
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
