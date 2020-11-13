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
    pbzip2 = luigi.Parameter(default='pbzip2')
    extract_tar = luigi.BoolParameter(default=True)
    n_cpu = luigi.IntParameter(default=1)
    memory_mb = luigi.FloatParameter(default=4096)
    log_dir_path = luigi.Parameter(default='')
    quiet = luigi.BoolParameter(default=False)

    def output(self):
        dir_data_dict = self._fetch_existing_funcotator_data()
        if ({'s', 'g'} <= set(dir_data_dict.keys())):
            return [
                luigi.LocalTarget(
                    re.sub(r'\.tar\.gz$', '', dir_data_dict[k])
                    if self.extract_tar else dir_data_dict[k]
                ) for k in ['s', 'g']
            ]
        else:
            return super().output()

    def complete(self):
        return bool(
            {'s', 'g'} <= set(self._fetch_existing_funcotator_data().keys())
        )

    def _fetch_existing_funcotator_data(self):
        return {
            Path(o.stem).stem[-1]: str(o)
            for o in Path(self.dest_dir_path).resolve().iterdir() if (
                o.name.startswith('funcotator_dataSources.')
                and o.name.endswith('.tar.gz')
            )
        }

    def run(self):
        dest_dir = Path(self.dest_dir_path).resolve()
        self.print_log(f'Download Funcotator data sources:\t{dest_dir}')
        dir_data_dict = self._fetch_existing_funcotator_data()
        self.setup_shell(
            run_id=dest_dir.name, log_dir_path=self.log_dir_path,
            commands=self.gatk, cwd=dest_dir, quiet=self.quiet,
            env={
                'JAVA_TOOL_OPTIONS': self.generate_gatk_java_options(
                    n_cpu=self.n_cpu, memory_mb=self.memory_mb
                )
            }
        )
        self.run_shell(
            args=[
                (
                    f'set -e && {self.gatk} FuncotatorDataSourceDownloader'
                    + f' --validate-integrity --{k}'
                ) for k in ['germline', 'somatic'] if k[0] not in dir_data_dict
            ]
        )
        tar_paths = list(self._fetch_existing_funcotator_data().values())
        assert bool(tar_paths), 'output files not detected'
        if self.extract_tar:
            yield ExtractTarFiles(
                tar_paths=tar_paths, dest_dir_path=str(dest_dir),
                pigz=self.pigz, pbzip2=self.pbzip2, n_cpu=self.n_cpu,
                remove_tar_files=True, log_dir_path=self.log_dir_path,
                quiet=self.quiet
            )


class ExtractTarFiles(VanqcTask):
    tar_paths = luigi.ListParameter()
    dest_dir_path = luigi.Parameter(default='.')
    recursive = luigi.BoolParameter(default=True)
    pigz = luigi.Parameter(default='pigz')
    pbzip2 = luigi.Parameter(default='pbzip2')
    n_cpu = luigi.IntParameter(default=1)
    remove_tar_files = luigi.BoolParameter(default=False)
    log_dir_path = luigi.Parameter(default='')
    quiet = luigi.BoolParameter(default=False)
    priority = 10

    def output(self):
        dest_dir = Path(self.dest_dir_path).resolve()
        return [
            luigi.LocalTarget(dest_dir.joinpath(Path(Path(p).stem).stem))
            for p in self.tar_paths
        ]

    def run(self):
        dest_dir = Path(self.dest_dir_path).resolve()
        run_id = dest_dir
        self.print_log(f'Extract tar files:\t{run_id}')
        tars = [Path(t) for t in self.tar_path]
        output_targets = [Path(o.path) for o in self.output()]
        self.setup_shell(
            run_id=run_id, log_dir_path=self.log_dir_path,
            commands=[self.pigz, self.pbzip2], cwd=dest_dir
        )
        for i, o in zip(tars, output_targets):
            self._tar_xf(tar_path=i, dest_dir_path=dest_dir)
            if self.recursive and o.is_dir():
                for f in o.iterdir():
                    if f.name.endswith(('.tar.gz', '.tar.bz2')):
                        self._tar_xf(tar_path=f, dest_dir_path=o)

    def _tar_xf(self, tar_path, dest_dir_path):
        self.run_shell(
            args=(
                'set -eo pipefail && ' + (
                    f'{self.pbzip2} -p{self.n_cpu}'
                    if str(tar_path).endswith('.bz2') else
                    f'{self.pigz} -p {self.n_cpu}'
                ) + f' -dc {tar_path} | tar xmvf -'
            ),
            cwd=dest_dir_path, input_files_or_dirs=tar_path,
            output_files_or_dirs=Path(str(dest_dir_path)).joinpath(
                Path(Path(str(tar_path)).stem).stem
            )
        )
        if self.remove_tar_files:
            self.remove_files_and_dirs(tar_path)


class AnnotateVcfWithFuncotator(VanqcTask):
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
    log_dir_path = luigi.Parameter(default='')
    output_file_format = luigi.Parameter(default='VCF')
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
            run_id=run_id, log_dir_path=self.log_dir_path, commands=self.gatk,
            cwd=output_files[0].parent, remove_if_failed=self.remove_if_failed,
            quiet=self.quiet,
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
    log_dir_path = luigi.Parameter(default='')
    remove_if_failed = luigi.BoolParameter(default=True)
    quiet = luigi.BoolParameter(default=False)
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
            run_id=run_id, log_dir_path=self.log_dir_path, commands=self.gatk,
            cwd=output_tsv.parent, remove_if_failed=self.remove_if_failed,
            quiet=self.quiet,
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
