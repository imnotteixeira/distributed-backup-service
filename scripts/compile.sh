#!/usr/bin/env bash

cd ..
mkdir -p out
javac $(find ./src/* | grep .java) -d out