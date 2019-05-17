#!/usr/bin/env bash
cd ..
java -cp out/production/distributed-backup-service -Djavax.net.ssl.keyStore=jsse/server.keys -Djavax.net.ssl.keyStorePassword=123456 -Djavax.net.ssl.trustStore=jsse/truststore -Djavax.net.ssl.trustStorePassword=123456 Main $(echo $@)