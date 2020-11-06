vanqc
=====

Variant Annotator and QC Checker for Human Genome Sequencing

[![wercker status](https://app.wercker.com/status/97b0f087b3e5a0a6409aa399611a07bc/s/main "wercker status")](https://app.wercker.com/project/byKey/97b0f087b3e5a0a6409aa399611a07bc)

Installation
------------

```sh
$ pip install -U https://github.com/dceoy/vanqc/archive/main.tar.gz
```

Dependent commands:

- pigz
- pbzip2
- bgzip
- tabix
- bcftools
- java
- snpEff (`java -jar snpEff.jar`)
- gatk

Docker image
------------

Pull the image from [Docker Hub](https://hub.docker.com/r/dceoy/vanqc/).

```sh
$ docker image pull dceoy/vanqc
```

Usage
-----

Run `vanqc --help` for more information.
