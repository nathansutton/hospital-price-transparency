FROM rocker/r-ver:4.0

RUN \
    apt-get update && \
    apt-get -y upgrade && \
    apt-get -y install libpq-dev && \
    Rscript -e 'install.packages("data.table")' && \
    Rscript -e 'install.packages("readxl")' && \
    Rscript -e 'install.packages("stringr")' && \
    Rscript -e 'install.packages("R.utils")' && \
    Rscript -e 'install.packages("jsonlite")' && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY etl/ /opt/etl

CMD Rscript /opt/etl/run.R
