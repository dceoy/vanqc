#!/usr/bin/env python
"""
Variant Annotator and QC Checker for Human Genome Sequencing

Usage:
    vanqc download [--debug|--info] [--cpus=<int>] [--hg19]
        [--snpeff|--funcotator|--vep] [--snpeff-db=<name>]
        [--snpeff-jar=<path>] [--dest-dir=<path>]
    vanqc normalize [--debug|--info] [--cpus=<int>] [--skip-cleaning]
        [--dest-dir=<path>] <fa_path> <vcf_path>...
    vanqc snpeff [--debug|--info] [--cpus=<int>] [--skip-cleaning]
        [--hg19] [--snpeff-jar=<path>] [--snpeff-db=<name>] [--normalize-vcf]
        [--dest-dir=<path>] <data_dir_path> <fa_path> <vcf_path>...
    vanqc funcotator [--debug|--info] [--cpus=<int>] [--skip-cleaning]
        [--hg19] [--normalize-vcf] [--dest-dir=<path>] <data_dir_path>
        <fa_path> <vcf_path>...
    vanqc funcotatesegments [--debug|--info] [--cpus=<int>] [--skip-cleaning]
        [--hg19] [--dest-dir=<path>] <data_dir_path> <fa_path> <seg_path>...
    vanqc vep [--debug|--info] [--cpus=<int>] [--skip-cleaning] [--hg19]
        [--normalize-vcf] [--dest-dir=<path>] <data_dir_path> <fa_path>
        <vcf_path>...
    vanqc stats [--debug|--info] [--cpus=<int>] [--skip-cleaning]
        [--dest-dir=<path>] <fa_path> <vcf_path>...
    vanqc metrics [--debug|--info] [--cpus=<int>] [--skip-cleaning]
        [--dest-dir=<path>] <fa_path> <dbsnp_vcf_path> <vcf_path>...
    vanqc -h|--help
    vanqc --version

Commands:
    download                Download and process annotation resource data
    normalize               Normalize VCF files using Bcftools
    snpeff                  Annotate variants using SnpEff
    funcotator              Annotate variants using GATK Funcotator
    funcotatesegments       Annotate segments using GATK FuncotateSegments
    vep                     Annotate variants using Ensembl VEP
    stats                   Collect VCF stats using Bcftools
    metrics                 Collect variant calling metrics using GATK (Picard)

Options:
    -h, --help              Print help and exit
    --version               Print version and exit
    --debug, --info         Execute a command with debug|info messages
    --cpus=<int>            Limit CPU cores used
    --hg19                  Use hg19 instead of hg38 (default) as a reference
    --snpeff, --funotator, --vep
                            Select only one of SnpEff, Funcotator, and VEP
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
                         AnnotateVariantsWithFuncotator,
                         DownloadFuncotatorDataSources)
from ..task.picard import CollectVariantCallingMetrics
from ..task.snpeff import AnnotateVariantsWithSnpeff, DownloadSnpeffDataSources
from ..task.vep import AnnotateVariantsWithEnsemblVep, DownloadEnsemblVepCache
from .builder import build_luigi_tasks
from .util import fetch_executable, load_default_dict, print_log


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
    print_log(f'Start the workflow of vanqc {__version__}')
    if args['--hg19']:
        ucsc_hg = 'hg19'
        ncbi_hg = 'GRCh37'
    else:
        ucsc_hg = 'hg38'
        ncbi_hg = 'GRCh38'
    n_cpu = int(args['--cpus'] or cpu_count())
    memory_mb = virtual_memory().total / 1024 / 1024 / 2
    sh_config = {
        'log_dir_path': None,
        'remove_if_failed': (not args['--skip-cleaning']), 'quiet': False,
        'executable': fetch_executable('bash')
    }
    if args['download']:
        anns = (
            {k for k in ['snpeff', 'funcotator', 'vep'] if args[f'--{k}']}
            or {'snpeff', 'funcotator', 'vep'}
        )
        common_kwargs = {
            'dest_dir_path': args['--dest-dir'], 'sh_config': sh_config
        }
        snpeff_kwargs = (
            {
                'snpeff': _fetch_snpeff_sh(jar_path=args['--snpeff-jar']),
                'genome_version': ncbi_hg, 'memory_mb': memory_mb,
                **common_kwargs
            } if 'snpeff' in anns else None
        )
        gatk_kwargs = (
            {
                **{c: fetch_executable(c) for c in ['gatk', 'pigz']},
                'n_cpu': n_cpu, 'memory_mb': memory_mb, **common_kwargs
            } if 'gatk' in anns else None
        )
        vep_kwargs = (
            {
                'src_url': load_default_dict('urls')[f'vep_{ncbi_hg}_cache'],
                **{c: fetch_executable(c) for c in ['wget', 'bgzip', 'pigz']},
                'n_cpu': n_cpu, **common_kwargs
            } if 'vep' in anns else None
        )
        build_luigi_tasks(
            tasks=(
                (
                    [DownloadSnpeffDataSources(**snpeff_kwargs)]
                    if snpeff_kwargs else list()
                ) + (
                    [DownloadFuncotatorDataSources(**gatk_kwargs)]
                    if gatk_kwargs else list()
                ) + (
                    [DownloadEnsemblVepCache(**vep_kwargs)]
                    if vep_kwargs else list()
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
            'memory_mb': (memory_mb / n_worker), 'sh_config': sh_config
        }
        if args['normalize']:
            kwargs = {
                'bcftools': fetch_executable('bcftools'), **common_kwargs
            }
            build_luigi_tasks(
                tasks=[
                    NormalizeVcf(input_vcf_path=p, **kwargs)
                    for p in args['<vcf_path>']
                ],
                workers=n_worker, log_level=log_level
            )
        elif args['snpeff']:
            kwargs = {
                'data_dir_path': args['<data_dir_path>'],
                'normalize_vcf': args['--normalize-vcf'],
                'genome_version': ncbi_hg, 'snpeff_db': args['--snpeff-db'],
                'snpeff': _fetch_snpeff_sh(jar_path=args['--snpeff-jar']),
                **{
                    c: fetch_executable(c)
                    for c in ['bcftools', 'bgzip', 'tabix']
                },
                **common_kwargs
            }
            build_luigi_tasks(
                tasks=[
                    AnnotateVariantsWithSnpeff(input_vcf_path=p, **kwargs)
                    for p in args['<vcf_path>']
                ],
                workers=n_worker, log_level=log_level
            )
        elif args['funcotator']:
            kwargs = {
                'data_src_dir_path': args['<data_dir_path>'],
                'normalize_vcf': args['--normalize-vcf'],
                'ref_version': ucsc_hg,
                **{c: fetch_executable(c) for c in ['gatk', 'bcftools']},
                **common_kwargs
            }
            build_luigi_tasks(
                tasks=[
                    AnnotateVariantsWithFuncotator(input_vcf_path=p, **kwargs)
                    for p in args['<vcf_path>']
                ],
                workers=n_worker, log_level=log_level
            )
        elif args['funcotatesegments']:
            kwargs = {
                'data_src_dir_path': args['<data_dir_path>'],
                'ref_version': ucsc_hg,
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
        elif args['vep']:
            kwargs = {
                'cache_dir_path': args['<data_dir_path>'],
                'normalize_vcf': args['--normalize-vcf'],
                **{
                    c: fetch_executable(c)
                    for c in ['vep', 'pigz', 'bcftools']
                },
                **common_kwargs
            }
            build_luigi_tasks(
                tasks=[
                    AnnotateVariantsWithEnsemblVep(input_vcf_path=p, **kwargs)
                    for p in args['<vcf_path>']
                ],
                workers=n_worker, log_level=log_level
            )
        elif args['stats']:
            kwargs = {
                'bcftools': fetch_executable('bcftools'),
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
