#!/bin/bash

for varname in ${!HADOOP*}; do unset ${varname}; done

exec python3 ./enviroplus.py "$@"
