#!/usr/bin/env bash
# Starts Vespa in Docker, deploys the performance test application and runs the performance tests against it.
# Arguments go to Maven, for example -Dtest=FeedPerformanceTest#upsertJsonStrings -Dvespa.perf.records=500000.
set -euo pipefail
cd "$(dirname "$0")/.."

image=${VESPA_IMAGE:-vespaengine/vespa:8.781.11}
name=kcv-perf-vespa
feed_port=${VESPA_FEED_PORT:-28080}
config_port=${VESPA_CONFIG_PORT:-29071}

if ! docker container inspect "$name" >/dev/null 2>&1; then
    docker run --detach --name "$name" --hostname "$name" \
        -p "127.0.0.1:$feed_port:8080" -p "127.0.0.1:$config_port:19071" "$image" >/dev/null
fi

until curl -sf "http://127.0.0.1:$config_port/state/v1/health" >/dev/null; do sleep 2; done

(cd src/test/resources/performance/app && zip -qr - .) | curl -sf --header Content-Type:application/zip \
    --data-binary @- "http://127.0.0.1:$config_port/application/v2/tenant/default/prepareandactivate" >/dev/null

until curl -sf "http://127.0.0.1:$feed_port/search/?hits=0&yql=select%20*%20from%20item%20where%20true" \
    | grep -q totalCount; do sleep 2; done

mvn -B -ntp test -Pperformance -Dvespa.perf.endpoint="http://127.0.0.1:$feed_port" "$@"
