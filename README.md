# Sidewinder <img src="https://github.com/srotya/sidewinder/raw/master/grafana/src/img/logo.png" width="50">

[![Join the chat at https://gitter.im/srotya/sidewinder](https://badges.gitter.im/srotya/sidewinder.svg)](https://gitter.im/srotya/sidewinder?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Build Status](https://travis-ci.org/srotya/sidewinder.svg?branch=master)](https://travis-ci.org/srotya/sidewinder)
[![codecov](https://codecov.io/gh/srotya/sidewinder/branch/master/graph/badge.svg)](https://codecov.io/gh/srotya/sidewinder)

Sidewinder is a timeseries database designed for speed and scalability. It's purpose is to provide storage, retrieval and analysis for time series data
 generated over the last few days. It can be used to power dashboards and real-time correlations for time series data at scale.
 
# What's new?
- **Disk based persistence**
- **Faster compression algorithm (Byzantine)**

# Highlights

1. Fast: ingest anywhere from 2-20 Million data points a second!
2. Multi-purpose: store application metrics or IoT data 
3. Disk & Memory: store data in-memory or persist to disk

# Features

### Speed
Sidewinder uses either pure in-memory buffers or memory mapped disk storage providing fast reads and writes without any need for expensive disks.

### InfluxDB Wire Protocol
Besides it's own binary protocol format, Sidewinder also supports InfluxDB wire protocol format for ingestion and can be used with InfluxData Telegraf and any other agents that speak InfluxDB wire protocol.

### REST API
Sidewinder offers a standard REST API to query and perform database operations.

### Grafana Support

[Grafana](http://grafana.org/) is an industry defacto for dashboards and visualizations. Sidewinder has it's own Grafana datasource which once installed can be used to connect to Sidewinder for creating visualizations. 

# Downloads

#### Releases

Releases can be access through the Sidewinder releases page: https://github.com/srotya/sidewinder/releases

#### Jar

Sidewinder Jars can be found on [Maven Central](http://search.maven.org/#search%7Cga%7C1%7Csidewinder)


#### Docker
Docker images are available ```docker pull srotya/sidewinder:latest```

# License

Sidewinder is licensed under Apache 2.0, Copyright 2017 Ambud Sharma and contains sources from other projects documented in the LICENSE file.
