#!/bin/sh -e
# Starts NiFi over plain HTTP without authentication, so Prometheus can scrape
# /nifi-api/flow/metrics/prometheus without a token. The stock image entrypoint
# only supports HTTPS. Local performance testing only.

cd /opt/nifi/nifi-current

set_prop() {
    sed -i "s|^$1=.*|$1=$2|" "$3"
}

props=conf/nifi.properties
set_prop nifi.web.http.host 0.0.0.0 "$props"
set_prop nifi.web.http.port 8080 "$props"
set_prop nifi.web.https.host '' "$props"
set_prop nifi.web.https.port '' "$props"
set_prop nifi.remote.input.secure false "$props"
set_prop nifi.security.user.login.identity.provider '' "$props"
for p in keystore keystoreType keystorePasswd keyPasswd truststore truststoreType truststorePasswd; do
    set_prop "nifi.security.$p" '' "$props"
done

set_prop java.arg.2 "-Xms${NIFI_JVM_HEAP_INIT:-2g}" conf/bootstrap.conf
set_prop java.arg.3 "-Xmx${NIFI_JVM_HEAP_MAX:-4g}" conf/bootstrap.conf

exec bin/nifi.sh run
