FROM ubuntu:20.04 AS builder

ENV DEBIAN_FRONTEND noninteractive

COPY --from=dceoy/bcftools:latest /usr/local/src/bcftools /usr/local/src/bcftools
COPY --from=dceoy/gatk:latest /opt/gatk /opt/gatk
COPY --from=dceoy/snpeff:latest /opt/snpEff /opt/snpEff
COPY --from=dceoy/vep:latest /usr/local/src/kent /usr/local/src/kent
COPY --from=dceoy/vep:latest /usr/local/src/bioperl-ext /usr/local/src/bioperl-ext
COPY --from=dceoy/vep:latest /usr/local/src/ensembl-xs /usr/local/src/ensembl-xs
COPY --from=dceoy/vep:latest /usr/local/src/ensembl-vep /usr/local/src/ensembl-vep
ADD https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh /tmp/miniconda.sh
ADD https://raw.githubusercontent.com/dceoy/print-github-tags/master/print-github-tags /usr/local/bin/print-github-tags
ADD . /tmp/vanqc

RUN set -e \
      && ln -sf bash /bin/sh

RUN set -e \
      && apt-get -y update \
      && apt-get -y dist-upgrade \
      && apt-get -y install --no-install-recommends --no-install-suggests \
        apt-transport-https apt-utils ca-certificates curl g++ gcc gnupg \
        libbz2-dev libc-dev libcurl4-gnutls-dev libfreetype6-dev libgsl-dev \
        liblzma-dev libncurses5-dev libperl-dev libpng-dev libssl-dev \
        libz-dev make pkg-config python r-base \
      && apt-get -y autoremove \
      && apt-get clean \
      && rm -rf /var/lib/apt/lists/*

RUN set -e \
      && /bin/bash /tmp/miniconda.sh -b -p /opt/conda \
      && /opt/conda/bin/conda update -n base -c defaults conda \
      && /opt/conda/bin/conda clean -ya \
      && ln -s /opt/conda/etc/profile.d/conda.sh /etc/profile.d/conda.sh \
      && echo '. /opt/conda/etc/profile.d/conda.sh' >> ~/.bashrc \
      && echo 'conda activate base' >> ~/.bashrc \
      && rm -f /tmp/miniconda.sh

RUN set -eo pipefail \
      && sed \
        -e 's/\(openssl\|pip\|python\|setuptools\|certifi\|wheel\|tk\|xz\|readline\|zlib\|sqlite\)=.*/\1/' \
        -e 's/\(gatk.*.zip\)/\/opt\/gatk\/\1/' \
        /opt/gatk/gatkcondaenv.yml > /tmp/gatkcondaenv.yml \
      && /opt/conda/bin/conda env create -n gatk -f /tmp/gatkcondaenv.yml \
      && /opt/conda/bin/conda clean -yaf \
      && find /opt/conda -follow -type f -name '*.a' -delete \
      && find /opt/conda -follow -type f -name '*.pyc' -delete

RUN set -e \
      && R -e "\
pkgs <- c('getopt', 'optparse', 'data.table', 'gsalib', 'ggplot2', 'dplyr', 'HMM'); \
options(repos = 'https://cran.rstudio.com/'); \
update.packages(ask = FALSE, dependencies = TRUE); \
install.packages(pkgs = pkgs, dependencies = TRUE, clean = TRUE); \
sapply(pkgs, library, character.only = TRUE);"

RUN set -e \
      && cd /usr/local/src/bcftools/htslib-* \
      && make clean \
      && ./configure \
      && make \
      && cd /usr/local/src/bcftools \
      && make clean \
      && ./configure --enable-libgsl --enable-perl-filters \
      && make \
      && make install \
      && cd /usr/local/src/kent/src/lib \
      && export KENT_SRC=/usr/local/src/kent/src \
      && export MACHTYPE=$(uname -m) \
      && export CFLAGS="-fPIC" \
      && export MYSQLINC=`mysql_config --include | sed -e 's/^-I//g'` \
      && export MYSQLLIBS=`mysql_config --libs` \
      && echo 'CFLAGS="-fPIC"' > ../inc/localEnvironment.mk \
      && make clean \
      && make \
      && cd ../jkOwnLib \
      && make clean \
      && make \
      && cpanm Bio::DB::BigFile Test::Warnings \
      && cd /usr/local/src/bioperl-ext/Bio/Ext/Align \
      && perl -pi -e "s|(cd libs.+)CFLAGS=\\\'|\$1CFLAGS=\\\'-fPIC |" \
        Makefile.PL \
      && perl Makefile.PL \
      && make \
      && make install \
      && cd /usr/local/src/ensembl-xs \
      && cpanm --installdeps --with-recommends . \
      && perl Makefile.PL \
      && make \
      && make install \
      && cd /usr/local/src/ensembl-vep \
      && cpanm --installdeps --with-recommends . \
      && perl INSTALL.pl \
      && find \
        /usr/local/src/ensembl-vep -maxdepth 1 -type f -executable \
        -exec ln -s {} /usr/local/bin \;

FROM ubuntu:20.04

ENV DEBIAN_FRONTEND noninteractive

COPY --from=builder /usr/local /usr/local
COPY --from=builder /opt /opt

RUN set -e \
      && ln -sf bash /bin/sh

RUN set -ea pipefail \
      && apt-get -y update \
      && apt-get -y dist-upgrade \
      && apt-get -y install --no-install-recommends --no-install-suggests \
        apt-transport-https apt-utils ca-certificates curl gnupg \
        libcurl3-gnutls libgsl23 libncurses5 openjdk-8-jre pbzip2 perl pigz \
        python r-base wget \
      && apt-get -y autoremove \
      && apt-get clean \
      && rm -rf /var/lib/apt/lists/*

RUN set -e \
      && unlink /usr/lib/ssl/openssl.cnf \
      && echo -e 'openssl_conf = default_conf' > /usr/lib/ssl/openssl.cnf \
      && echo >> /usr/lib/ssl/openssl.cnf \
      && cat /etc/ssl/openssl.cnf >> /usr/lib/ssl/openssl.cnf \
      && echo >> /usr/lib/ssl/openssl.cnf \
      && echo -e '[default_conf]' >> /usr/lib/ssl/openssl.cnf \
      && echo -e 'ssl_conf = ssl_sect' >> /usr/lib/ssl/openssl.cnf \
      && echo >> /usr/lib/ssl/openssl.cnf \
      && echo -e '[ssl_sect]' >> /usr/lib/ssl/openssl.cnf \
      && echo -e 'system_default = system_default_sect' >> /usr/lib/ssl/openssl.cnf \
      && echo >> /usr/lib/ssl/openssl.cnf \
      && echo -e '[system_default_sect]' >> /usr/lib/ssl/openssl.cnf \
      && echo -e 'MinProtocol = TLSv1.2' >> /usr/lib/ssl/openssl.cnf \
      && echo -e 'CipherString = DEFAULT:@SECLEVEL=1' >> /usr/lib/ssl/openssl.cnf

ENV PATH /opt/gatk/bin:/opt/snpEff/bin:/opt/conda/envs/gatk/bin:/opt/conda/bin:${PATH}
ENV BCFTOOLS_PLUGINS /usr/local/src/bcftools/plugins

ENTRYPOINT ["/opt/conda/bin/vanqc"]
