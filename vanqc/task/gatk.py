#!/usr/bin/env python

import re
from pathlib import Path

import luigi

from .bcftools import NormalizeVcf
from .core import VanqcTask


class DownloadFuncotatorDataSources(VanqcTask):
    dest_dir_path = luigi.Parameter(default='.')
    gatk = luigi.Parameter(default='gatk')
    pigz = luigi.Parameter(default='pigz')
    n_cpu = luigi.IntParameter(default=1)
    memory_mb = luigi.FloatParameter(default=4096)
    sh_config = luigi.DictParameter(default=dict())
    priority = 10

    def output(self):
        dir_data_dict = self._fetch_existing_data_dirs()
        if ({'s', 'g'} <= set(dir_data_dict.keys())):
            return [luigi.LocalTarget(dir_data_dict[k]) for k in ['s', 'g']]
        else:
            return super().output()

    def complete(self):
        return bool({'s', 'g'} <= set(self._fetch_existing_data_dirs().keys()))

    def _fetch_existing_data_dirs(self):
        return {
            o.name[-1]: o for o in Path(self.dest_dir_path).resolve().iterdir()
            if re.search(r'^funcotator_dataSources\.v[0-9\.]+[sg]$', o.name)
        }

    def run(self):
        dest_dir = Path(self.dest_dir_path).resolve()
        self.print_log(f'Download Funcotator data sources:\t{dest_dir}')
        self.setup_shell(
            run_id=dest_dir.name, commands=[self.gatk, self.pigz],
            cwd=dest_dir, **self.sh_config,
            env={
                'JAVA_TOOL_OPTIONS': self.generate_gatk_java_options(
                    n_cpu=self.n_cpu, memory_mb=self.memory_mb
                )
            }
        )
        tar_dict = {
            k: dest_dir.joinpath(f'funcotator_{k}_data_sources.tar.gz')
            for k in ['germline', 'somatic']
            if k[0] not in self._fetch_existing_data_dirs()
        }
        for k, v in tar_dict.items():
            self.run_shell(
                args=(
                    f'set -e && {self.gatk} FuncotatorDataSourceDownloader'
                    + f' --validate-integrity --{k} --output {v}'
                ),
                output_files_or_dirs=v
            )
            self.tar_xf(
                tar_path=v, dest_dir_path=dest_dir, pigz=self.pigz,
                n_cpu=self.n_cpu, remove_tar=True
            )
            src_dir = self._fetch_existing_data_dirs().get(k[0])
            assert (src_dir and src_dir.is_dir()), f'{k} data not found'
            for f in src_dir.iterdir():
                if f.name.endswith('.tar.gz'):
                    self.tar_xf(
                        tar_path=f, dest_dir_path=src_dir, pigz=self.pigz,
                        n_cpu=self.n_cpu, remove_tar=True,
                        output_files_or_dirs=src_dir.joinpath(
                            Path(Path(f).stem).stem
                        )
                    )


class AnnotateVariantsWithFuncotator(VanqcTask):
    input_vcf_path = luigi.Parameter()
    data_src_dir_path = luigi.Parameter()
    fa_path = luigi.Parameter()
    ref_version = luigi.Parameter(default='hg38')
    dest_dir_path = luigi.Parameter(default='.')
    normalize_vcf = luigi.BoolParameter(default=False)
    norm_dir_path = luigi.Parameter(default='')
    bcftools = luigi.Parameter(default='bcftools')
    gatk = luigi.Parameter(default='gatk')
    n_cpu = luigi.IntParameter(default=1)
    memory_mb = luigi.FloatParameter(default=4096)
    output_file_format = luigi.Parameter(default='VCF')
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
        is_seg = (self.output_file_format == 'SEG')
        output_vcf = Path(self.dest_dir_path).resolve().joinpath(
            re.sub(
                r'\.vcf$', '',
                Path(
                    self.input()[0].path if self.normalize_vcf
                    else self.input_vcf_path
                ).stem
            ) + '.funcotator.' + self.output_file_format.lower()
            + ('.tsv' if is_seg else '.gz')
        )
        return [
            luigi.LocalTarget(f'{output_vcf}{s}')
            for s in ['', ('.gene_list.txt' if is_seg else '.tbi')]
        ]

    def run(self):
        input_vcf = Path(
            self.input()[0].path if self.normalize_vcf else self.input_vcf_path
        ).resolve()
        run_id = Path(input_vcf.stem).stem
        self.print_log(f'Annotate variants with Funcotator:\t{run_id}')
        fa = Path(self.fa_path).resolve()
        fa_dict = fa.parent.joinpath(f'{fa.stem}.dict')
        data_src_dir = Path(self.data_src_dir_path).resolve()
        output_files = [Path(o.path) for o in self.output()]
        self.setup_shell(
            run_id=run_id, commands=self.gatk, cwd=output_files[0].parent,
            **self.sh_config,
            env={
                'JAVA_TOOL_OPTIONS': self.generate_gatk_java_options(
                    n_cpu=self.n_cpu, memory_mb=self.memory_mb
                )
            }
        )
        self.run_shell(
            args=(
                f'set -e && {self.gatk} Funcotator'
                + f' --variant {input_vcf}'
                + f' --data-sources-path {data_src_dir}'
                + f' --reference {fa}'
                + f' --ref-version {self.ref_version}'
                + f' --output {output_files[0]}'
                + f' --output-file-format {self.output_file_format}'
            ),
            input_files_or_dirs=[input_vcf, fa, fa_dict, data_src_dir],
            output_files_or_dirs=output_files,
        )


class AnnotateSegWithFuncotateSegments(VanqcTask):
    input_seg_path = luigi.Parameter()
    data_src_dir_path = luigi.Parameter()
    fa_path = luigi.Parameter()
    ref_version = luigi.Parameter(default='hg38')
    dest_dir_path = luigi.Parameter(default='.')
    gatk = luigi.Parameter(default='gatk')
    n_cpu = luigi.IntParameter(default=1)
    memory_mb = luigi.FloatParameter(default=4096)
    sh_config = luigi.DictParameter(default=dict())
    priority = 10

    def output(self):
        return luigi.LocalTarget(
            Path(self.dest_dir_path).resolve().joinpath(
                Path(self.input_seg_path).stem + '.funcotator.seg.tsv'
            )
        )

    def run(self):
        input_tsv = Path(self.input_seg_path).resolve()
        run_id = input_tsv.stem
        self.print_log(f'Annotate segments with FuncotateSegments:\t{run_id}')
        data_src_dir = Path(self.data_src_dir_path).resolve()
        fa = Path(self.fa_path).resolve()
        fa_dict = fa.parent.joinpath(f'{fa.stem}.dict')
        output_tsv = Path(self.output().path)
        self.setup_shell(
            run_id=run_id, commands=self.gatk, cwd=output_tsv.parent,
            **self.sh_config,
            env={
                'JAVA_TOOL_OPTIONS': self.generate_gatk_java_options(
                    n_cpu=self.n_cpu, memory_mb=self.memory_mb
                )
            }
        )
        self.run_shell(
            args=(
                f'set -e && {self.gatk} FuncotateSegments'
                + f' --segments {input_tsv}'
                + f' --data-sources-path {data_src_dir}'
                + f' --reference {fa}'
                + f' --ref-version {self.ref_version}'
                + f' --output {output_tsv}'
                + ' --output-file-format SEG'
            ),
            input_files_or_dirs=[input_tsv, fa, fa_dict, data_src_dir],
            output_files_or_dirs=output_tsv
        )


if __name__ == '__main__':
    luigi.run()
