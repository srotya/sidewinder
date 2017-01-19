# Sidewinder <img src="https://github.com/srotya/sidewinder/raw/master/grafana/src/img/logo.png" width="50">

[![Join the chat at https://gitter.im/srotya/sidewinder](https://badges.gitter.im/srotya/sidewinder.svg)](https://gitter.im/srotya/sidewinder?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Build Status](https://travis-ci.org/srotya/sidewinder.svg?branch=master)](https://travis-ci.org/srotya/sidewinder)
[![codecov](https://codecov.io/gh/srotya/sidewinder/branch/master/graph/badge.svg)](https://codecov.io/gh/srotya/sidewinder)

Sidewinder is an in-memory timeseries database designed for speed and scalability. It's purpose is to provide storage, retrieval and analysis for time series data
 generated over the last few days. It can be used to power dashboards and real-time correlations for time series data at scale.
 
**Note: Sidewinder is currently under heavy development and changes are extremely dynamic; it's yet NOT production ready.**

# Highlights

1. Fast: can ingest anywhere from 2-20 Million data points a second!
2. Scalable: has baked in sharding and clustering logic to scale
3. Ephemeral: it's purely in-memory, never bottlenecked by disks
4. Multi-purpose: store application metrics or IoT data 

# Features

### Correlations
Just like Gorilla, PPMCC (Pearson Product Moment Coefficient) based time series search is supported in Sidewinder as well.

### InfluxDB Wire Protocol
Besides it's own binary protocol format, Sidewinder also supports InfluxDB wire protocol format for ingestion and can be used with InfluxData Telegraf and any other agents that speak InfluxDB wire protocol.

### REST API (In-development)
Sidewinder offers a standard REST API to query and perform database operations.

### Grafana Support

[Grafana](http://grafana.org/) is an industry defacto for dashboards and visualizations. Sidewinder has it's own Grafana datasource which once installed can be used to connect to Sidewinder for creating visualizations. 

### SQL Support

Sidewinder uses Apache Calcite to offer standard SQL support with some additional Timseries functions to make it user friendly.

# Downloads

#### Releases

Releases can be access through the Sidewinder releases page: https://github.com/srotya/sidewinder/releases

#### Jar

Sidewinder Jars can be found on [Maven Central](http://search.maven.org/#search%7Cga%7C1%7Csidewinder)


#### Docker
Docker images are available ```docker pull srotya/sidewinder:latest```

# License

Sidewinder is licensed under Apache 2.0, Copyright 2017 Ambud Sharma and contains sources from other projects documented in the LICENSE file.

Sidewinder's storage is based on Facebook's [Gorilla](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf) compression algorithm.