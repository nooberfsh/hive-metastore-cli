#!/usr/bin/env bash

thrift -r --out hive-metastore-cli/src --gen rs thrift/hive_metastore.thrift