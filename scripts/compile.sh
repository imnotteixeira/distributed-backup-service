#!/usr/bin/env bash

cd ..
mkdir -p out/production/distributed-backup-service
javac $(find ./src/* | grep .java) -d out/production/distributed-backup-service/