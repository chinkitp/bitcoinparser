# Parsing the Bitcoin Blockchain at Scale

## Background

Objective of this repository is to help you parse the bitcoin blockchain and convert it into a [property graph model](./docs/Bitcoin-Graph-2.png)

The raw bitcoin blocks are stored in 1350 `blk*.dat` files each `~128MB`. These raw files are input to our parser.

Processing is divided into 2 phases.

## **Quick Setup using Google Cloud Platform (GCP)**

### Phase 1 : extracts each block from the *.dat file

GCP Dataproc cluster for **block-extractor** with **1 hr execution time**
* 1 x Master `N1-standard-2` each _2 vCPU 7.5GB RAM_
* 4 x Worker Nodes `N1-standard-4` each _4 vCPU 15GB RAM_

### Phase 2 : Prase each block and convert to a Property Graph

GCP Kubernetes cluster for **block-parser** with **14 hrs execution time**
* 40 x Nodes each _4 vCPU 8GB RAM_

## Performance

The entire blockchain can be parsed in **<16Hrs**. It is a scale out architecture and adding more nodes can reduce parsing time.

## Why can't everything be done in spark?

The spark version of the parser has a dependency on Bitcoinj which has this issue:
[bitcoinj fails to deserialize the new 0.13+ block format from bitcoind](https://github.com/bitcoinj/bitcoinj/issues/1336)

## Acknowledgements

* [NBitcoin](https://github.com/MetacoSA/NBitcoin) - Comprehensive Bitcoin library for the .NET framework.
* [Bitcoinj](https://github.com/bitcoinj/bitcoinj) - a library for working with Bitcoin in java
* [Terry M](https://github.com/tmoschou)