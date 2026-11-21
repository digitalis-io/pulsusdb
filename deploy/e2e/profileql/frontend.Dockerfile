# Issue #832: the front end for the profiles end-to-end scenario, with its
# three plugins installed at build time, so a run fetches nothing from the
# plugin catalogue. Published by .github/workflows/profileql-e2e-images.yml;
# the e2e stack pins the digest that workflow prints.
FROM docker.io/grafana/grafana:13.3.1@sha256:0615ec4fbd4072004b7d86b46ded63013a74b7d6eb1119273dd89e8fa25c13cf

ARG PROFILES_DATASOURCE_VERSION=13.0.10
ARG PROFILES_APP_VERSION=3.2.0
ARG TRACES_DATASOURCE_VERSION=13.4.2

RUN grafana cli --pluginsDir /var/lib/grafana/plugins plugins install grafana-pyroscope-datasource ${PROFILES_DATASOURCE_VERSION} \
 && grafana cli --pluginsDir /var/lib/grafana/plugins plugins install grafana-pyroscope-app ${PROFILES_APP_VERSION} \
 && grafana cli --pluginsDir /var/lib/grafana/plugins plugins install tempo ${TRACES_DATASOURCE_VERSION}

ENV GF_PLUGINS_PREINSTALL_DISABLED=true \
    GF_ANALYTICS_REPORTING_ENABLED=false \
    GF_ANALYTICS_CHECK_FOR_UPDATES=false \
    GF_ANALYTICS_CHECK_FOR_PLUGIN_UPDATES=false \
    GF_PLUGINS_PUBLIC_KEY_RETRIEVAL_DISABLED=true
