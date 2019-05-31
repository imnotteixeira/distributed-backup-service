#!/usr/bin/env bash
cd ../out/production/distributed-backup-service/
if [[ "$#" -lt 2 ]] || [[ "$#" -gt 4 ]]; then
    echo "Usage ./testapp.sh <peer_ap> <sub_protocol> <opnd_1> <opnd_2>"
else
    java TestApp $(echo $@)
fi