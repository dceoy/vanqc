vanqc
=====

Variant Annotator and QC Checker for Human Genome Sequencing

[![wercker status](https://app.wercker.com/status/97b0f087b3e5a0a6409aa399611a07bc/s/main "wercker status")](https://app.wercker.com/project/byKey/97b0f087b3e5a0a6409aa399611a07bc)
![Upload Python Package](https://github.com/dceoy/vanqc/workflows/Upload%20Python%20Package/badge.svg)

Installation
------------

```sh
$ pip install -U https://github.com/dceoy/vanqc/archive/main.tar.gz
```

Dependent commands:

- `pigz`
- `bgzip`
- `tabix`
- `bcftools` (and `plot-vcfstats`)
- `java`
- `snpEff` (`java -jar snpEff.jar`)
- `gatk`
- `vep`

Docker image
------------

Pull the image from [Docker Hub](https://hub.docker.com/r/dceoy/vanqc/).

```sh
$ docker image pull dceoy/vanqc
```

Usage
-----

- Normalize VCF files using Bcftools

  ```sh
  $ vanqc normalize /path/to/reference.fa /path/to/variants.vcf.gz
  ```

- Annotate variants using SnpEff

  ```sh
  $ vanqc download --snpeff --dest-dir=/path/to/resource
  $ vanqc snpeff \
      /path/to/resource/snpeff_data/GRCh38.86
      /path/to/reference.fa \
      /path/to/variants.vcf.gz
  ```

- Annotate variants using GATK Funcotator

  ```sh
  $ vanqc download --funcotator --dest-dir=/path/to/resource
  $ vanqc funcotator \
      /path/to/resource/funcotator_dataSources.v1.7.20200521 \
      /path/to/reference.fa \
      /path/to/variants.vcf.gz
  ```

- Annotate variants using Ensembl VEP

  ```sh
  $ vanqc download --vep --dest-dir=/path/to/resource
  $ vanqc vep \
      /path/to/resource/vep_cache/homo_sapiens \
      /path/to/reference.fa \
      /path/to/variants.vcf.gz
  ```

- Collect VCF stats using Bcftools

  ```sh
  $ vanqc stats /path/to/reference.fa /path/to/variants.vcf.gz
  ```

- Collect variant calling metrics using GATK (Picard)

  ```sh
  $ vanqc metrics \
      /path/to/reference.fa \
      /path/to/dbsnp.vcf.gz \
      /path/to/variants.vcf.gz
  ```

Run `vanqc --help` for more information.
