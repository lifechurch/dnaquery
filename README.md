# DNAQuery

DNAQuery is a command line utility to take [logdna](https://logdna.com) exports and load them into [BigQuery](https://cloud.google.com/bigquery/). This allows long term queryable storage of logs in BigQuery which can be considerably most cost effective. In our use case we want real time access to lots of different logs and long term storage of a subset of those logs. This approach has helped us find a balance between access & cost.

## Getting Started

`go get -u github.com/lifechurch/dnaquery`

DNAQuery has been tested on Go v1.9

## Configuring

`cp example.toml dnaquery.toml`

edit dnaquery.toml

## Prerequisites

- logdna account must have [Archiving](https://docs.logdna.com/docs/archiving) to S3 enabled
- a [Google Cloud Platform](https://cloud.google.com) account

## Tests

## Dependencies

This project uses [dep](https://github.com/golang/de) for dependency management

```
go get -u github.com/golang/dep/cmd/dep
dep ensure -update; dep prune
```
