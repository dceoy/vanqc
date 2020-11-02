#!/usr/bin/env python
"""
Variant Annotator and QC Checker for Clinical Genome Sequencing

Usage:
    vanqc download [--debug|--info] [--cpus=<int>] [--ref-ver=<str>]
        [--snpeff-jar=<path>] [--snpeff|--funcotator] [--dest-dir=<path>]
    vanqc snpeff [--debug|--info] [--cpus=<int>] [--skip-cleaning]
        [--ref-ver=<str>] [--snpeff-jar=<path>] [--snpeff-genome=<ver>]
        [--normalize-vcf] [--dest-dir=<path>] <snpeff_config_path> <fa_path>
        <vcf_path>...
    vanqc funcotator [--debug|--info] [--cpus=<int>] [--skip-cleaning]
        [--ref-ver=<str>] [--normalize-vcf] [--dest-dir=<path>] <data_dir_path>
        <fa_path> <vcf_path>...
    vanqc funcotatesegments [--debug|--info] [--cpus=<int>] [--skip-cleaning]
        [--ref-ver=<str>] [--dest-dir=<path>] <data_dir_path>
        <fa_path> <seg_path>...
    vanqc -h|--help
    vanqc --version

Commands:
    download                Download and process GRCh38 resource data
    snpeff                  Annotate VCF files using SnpEff
    funcotator              Annotate VCF files using GATK Funcotator

Options:
    -h, --help              Print help and exit
    --version               Print version and exit
    --debug, --info         Execute a command with debug|info messages
    --cpus=<int>            Limit CPU cores used
    --ref-ver=<str>         Specify a reference version [default: hg38]
                            {hg38, hg19}
    --snpeff-jar=<path>     Specify a path to snpEff.jar
    --snpeff, --funcotator  Specify the annotation tool to use
    --dest-dir=<path>       Specify a destination directory path [default: .]
    --skip-cleaning         Skip incomlete file removal when a task fails
    --normalize-vcf         Normalize VCF files

Args:
    <snpeff_config_path>    Path to a SnpEff config file
    <fa_path>               Path to an reference FASTA file
                            (The index and sequence dictionary are required.)
    <vcf_path>              Path to a VCF file
    <data_dir_path>         Path to a Funcotator data source directory
    <seg_path>              Path to a segment TSV files
"""

import logging
import os
from math import floor
from pathlib import Path

from docopt import docopt
from psutil import cpu_count, virtual_memory

from .. import __version__
from ..task.gatk import (AnnotateSegWithFuncotateSegments,
                         AnnotateVcfWithFuncotator,
                         DownloadFuncotatorDataSources)
from ..task.snpeff import AnnotateVcfWithSnpeff, DownloadSnpeffDataSource
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
    dest_dir = Path(args['--dest-dir']).resolve()
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
                        DownloadSnpeffDataSource(
                            dest_dir_path=str(dest_dir), snpeff=snpeff,
                            genome_version={
                                'hg38': 'GRCh38', 'hg19': 'GRCh37'
                            }[args['--ref-ver']],
                            memory_mb=memory_mb
                        )
                    ] if snpeff else list()
                ) + (
                    [
                        DownloadFuncotatorDataSources(
                            dest_dir_path=str(dest_dir), gatk=gatk,
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
        n_vcf = len(args['<vcf_path>'])
        n_worker = min(n_vcf, n_cpu)
        common_kwargs = {
            'fa_path': str(Path(args['<fa_path>']).resolve()),
            'ref_version': args['--ref-ver'], 'dest_dir_path': str(dest_dir),
            'n_cpu': (floor(n_cpu / n_vcf) if n_cpu > n_vcf else 1),
            'memory_mb':
            int(virtual_memory().total / 1024 / 1024 / n_worker),
            'remove_if_failed': (not args['--skip-cleaning'])
        }
        bcftools_path = fetch_executable('bcftools')
        if args['snpeff']:
            kwargs = {
                'normalize_vcf': args['--normalize-vcf'],
                'snpeff_config_path':
                str(Path(args['<snpeff_config_path>']).resolve()),
                'snpeff_genome_version': args['--snpeff-genome'],
                'snpeff': _fetch_snpeff_sh(jar_path=args['--snpeff-jar']),
                'bcftools': bcftools_path,
                **{c: fetch_executable(c) for c in ['bgzip', 'tabix']},
                **common_kwargs
            }
            build_luigi_tasks(
                tasks=[
                    AnnotateVcfWithSnpeff(
                        input_vcf_path=str(Path(p).resolve()), **kwargs
                    ) for p in args['<vcf_path>']
                ],
                workers=n_worker, log_level=log_level
            )
        elif args['funcotator'] or args['funcotatesegments']:
            kwargs = {
                'data_src_dir_path':
                str(Path(args['<data_dir_path>']).resolve()),
                'gatk': fetch_executable('gatk'), **common_kwargs
            }
            build_luigi_tasks(
                tasks=(
                    [
                        AnnotateSegWithFuncotateSegments(
                            input_seg_path=str(Path(p).resolve()), **kwargs
                        ) for p in args['<seg_path>']
                    ] if args['funcotatesegments'] else [
                        AnnotateVcfWithFuncotator(
                            input_vcf_path=str(Path(p).resolve()),
                            normalize_vcf=args['--normalize-vcf'],
                            bcftools=bcftools_path, **kwargs
                        ) for p in args['<vcf_path>']
                    ]
                ),
                workers=n_worker, log_level=log_level
            )


def _fetch_snpeff_sh(jar_path=None):
    return (
        '{0} -jar {1}'.format(
            fetch_executable('java'), Path(jar_path).resource()
        ) if jar_path else fetch_executable('snpEff')
    )
