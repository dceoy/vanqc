#!/usr/bin/env python
"""
Variant Annotator and QC Checker for Human Genome Sequencing

Usage:
    vanqc download [--debug|--info] [--cpus=<int>] [--ref-ver=<str>]
        [--snpeff|--funcotator] [--snpeff-db=<name>] [--snpeff-jar=<path>]
        [--dest-dir=<path>]
    vanqc normalize [--debug|--info] [--cpus=<int>] [--skip-cleaning]
        [--dest-dir=<path>] <fa_path> <vcf_path>...
    vanqc snpeff [--debug|--info] [--cpus=<int>] [--skip-cleaning]
        [--ref-ver=<str>] [--snpeff-jar=<path>] [--snpeff-db=<name>]
        [--normalize-vcf] [--dest-dir=<path>] <data_dir_path> <fa_path>
        <vcf_path>...
    vanqc funcotator [--debug|--info] [--cpus=<int>] [--skip-cleaning]
        [--ref-ver=<str>] [--normalize-vcf] [--dest-dir=<path>] <data_dir_path>
        <fa_path> <vcf_path>...
    vanqc funcotatesegments [--debug|--info] [--cpus=<int>] [--skip-cleaning]
        [--ref-ver=<str>] [--dest-dir=<path>] <data_dir_path>
        <fa_path> <seg_path>...
    vanqc stats [--debug|--info] [--cpus=<int>] [--skip-cleaning]
        [--dest-dir=<path>] <fa_path> <vcf_path>...
    vanqc metrics [--debug|--info] [--cpus=<int>] [--skip-cleaning]
        [--dest-dir=<path>] <fa_path> <dbsnp_vcf_path> <vcf_path>...
    vanqc -h|--help
    vanqc --version

Commands:
    download                Download and process GRCh38 resource data
    normalize               Normalize VCF files using Bcftools
    snpeff                  Annotate VCF files using SnpEff
    funcotator              Annotate VCF files using GATK Funcotator
    funcotatesegments       Annotate SEG files using GATK FuncotateSegments
    stats                   Collect VCF stats using Bcftools
    metrics                 Collect variant calling metrics using GATK (Picard)

Options:
    -h, --help              Print help and exit
    --version               Print version and exit
    --debug, --info         Execute a command with debug|info messages
    --cpus=<int>            Limit CPU cores used
    --ref-ver=<str>         Specify a reference version [default: hg38]
                            {hg38, hg19}
    --snpeff, --funcotator  Specify the annotation tool to use
    --snpeff-jar=<path>     Specify a path to snpEff.jar
    --snpeff-db=<name>      Specify the SnpEff database
    --dest-dir=<path>       Specify a destination directory path [default: .]
    --skip-cleaning         Skip incomlete file removal when a task fails
    --normalize-vcf         Normalize VCF files

Args:
    <fa_path>               Path to an reference FASTA file
                            (The index and sequence dictionary are required.)
    <vcf_path>              Path to a VCF file
    <data_dir_path>         Path to a data source directory
    <seg_path>              Path to a segment TSV files
    <dbsnp_vcf_path>        Path to a reference dbSNP file
"""

import logging
import os
from math import floor
from pathlib import Path

from docopt import docopt
from psutil import cpu_count, virtual_memory

from .. import __version__
from ..task.bcftools import CollectVcfStats, NormalizeVcf
from ..task.gatk import (AnnotateSegWithFuncotateSegments,
                         AnnotateVcfWithFuncotator,
                         DownloadFuncotatorDataSources)
from ..task.picard import CollectVariantCallingMetrics
from ..task.snpeff import AnnotateVcfWithSnpeff, DownloadSnpeffDataSources
from .builder import build_luigi_tasks
from .util import fetch_executable, print_log


def main():
    args = docopt(__doc__, version=__version__)
    if args['--debug']:
        log_level = 'DEBUG'
    elif args['--info']:
        log_level = 'INFO'
    else:
        log_level = 'WARNING'
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S', level=log_level
    )
    logger = logging.getLogger(__name__)
    logger.debug(f'args:{os.linesep}{args}')
    assert args['--ref-ver'] not in {'hg38', 'hg19'}, 'invalid ref version'
    print_log(f'Start the workflow of vanqc {__version__}')
    n_cpu = int(args['--cpus'] or cpu_count())
    memory_mb = virtual_memory().total / 1024 / 1024 / 2
    if args['download']:
        if args['--snpeff']:
            snpeff = _fetch_snpeff_sh(jar_path=args['--snpeff-jar'])
            gatk = None
        elif args['--funcotator']:
            snpeff = None
            gatk = fetch_executable('gatk')
        else:
            snpeff = _fetch_snpeff_sh(jar_path=args['--snpeff-jar'])
            gatk = fetch_executable('gatk')
        build_luigi_tasks(
            tasks=(
                (
                    [
                        DownloadSnpeffDataSources(
                            dest_dir_path=args['--dest-dir'], snpeff=snpeff,
                            genome_version={
                                'hg38': 'GRCh38', 'hg19': 'GRCh37'
                            }[args['--ref-ver']],
                            memory_mb=memory_mb
                        )
                    ] if snpeff else list()
                ) + (
                    [
                        DownloadFuncotatorDataSources(
                            dest_dir_path=args['--dest-dir'], gatk=gatk,
                            **{
                                c: fetch_executable(c)
                                for c in ['pigz', 'pbzip2']
                            },
                            n_cpu=n_cpu, memory_mb=memory_mb
                        )
                    ] if gatk else list()
                )
            ),
            log_level=log_level
        )
    else:
        n_target = len(
            args['<seg_path>' if args['funcotatesegments'] else '<vcf_path>']
        )
        n_worker = min(n_target, n_cpu)
        common_kwargs = {
            'fa_path': args['<fa_path>'], 'dest_dir_path': args['--dest-dir'],
            'n_cpu': max(floor(n_cpu / n_worker), 1),
            'memory_mb': (memory_mb / n_worker),
            'remove_if_failed': (not args['--skip-cleaning'])
        }
        bcftools = fetch_executable('bcftools')
        if args['normalize']:
            kwargs = {'bcftools': bcftools, **common_kwargs}
            build_luigi_tasks(
                tasks=[
                    NormalizeVcf(input_vcf_path=p, **kwargs)
                    for p in args['<vcf_path>']
                ],
                workers=n_worker, log_level=log_level
            )
        elif args['snpeff']:
            kwargs = {
                'data_src_dir_path': args['<data_dir_path>'],
                'normalize_vcf': args['--normalize-vcf'],
                'ref_version': args['--ref-ver'],
                'snpeff_db': args['--snpeff-db'],
                'snpeff': _fetch_snpeff_sh(jar_path=args['--snpeff-jar']),
                'bcftools': bcftools,
                **{c: fetch_executable(c) for c in ['bgzip', 'tabix']},
                **common_kwargs
            }
            build_luigi_tasks(
                tasks=[
                    AnnotateVcfWithSnpeff(input_vcf_path=p, **kwargs)
                    for p in args['<vcf_path>']
                ],
                workers=n_worker, log_level=log_level
            )
        elif args['funcotator']:
            kwargs = {
                'data_src_dir_path': args['<data_dir_path>'],
                'normalize_vcf': args['--normalize-vcf'],
                'ref_version': args['--ref-ver'],
                'gatk': fetch_executable('gatk'), 'bcftools': bcftools,
                **common_kwargs
            }
            build_luigi_tasks(
                tasks=[
                    AnnotateVcfWithFuncotator(input_vcf_path=p, **kwargs)
                    for p in args['<vcf_path>']
                ],
                workers=n_worker, log_level=log_level
            )
        elif args['funcotator'] or args['funcotatesegments']:
            kwargs = {
                'data_src_dir_path': args['<data_dir_path>'],
                'ref_version': args['--ref-ver'],
                'gatk': fetch_executable('gatk'), **common_kwargs
            }
            build_luigi_tasks(
                tasks=[
                    AnnotateSegWithFuncotateSegments(
                        input_seg_path=p, **kwargs
                    ) for p in args['<seg_path>']
                ],
                workers=n_worker, log_level=log_level
            )
        elif args['stats']:
            kwargs = {
                'bcftools': bcftools,
                'plot_vcfstats': fetch_executable('plot-vcfstats'),
                **{k: v for k, v in common_kwargs.items() if k != 'memory_mb'}
            }
            build_luigi_tasks(
                tasks=[
                    CollectVcfStats(input_vcf_path=p, **kwargs)
                    for p in args['<vcf_path>']
                ],
                workers=n_worker, log_level=log_level
            )
        elif args['metrics']:
            kwargs = {
                'dbsnp_vcf_path': args['<dbsnp_vcf_path>'],
                'picard': (
                    fetch_executable('gatk', ignore_errors=True)
                    or fetch_executable('picard')
                ),
                **common_kwargs
            }
            build_luigi_tasks(
                tasks=[
                    CollectVariantCallingMetrics(input_vcf_path=p, **kwargs)
                    for p in args['<vcf_path>']
                ],
                workers=n_worker, log_level=log_level
            )


def _fetch_snpeff_sh(jar_path=None):
    return (
        '{0} -jar {1}'.format(
            fetch_executable('java'), Path(jar_path).resource()
        ) if jar_path else fetch_executable('snpEff')
    )
