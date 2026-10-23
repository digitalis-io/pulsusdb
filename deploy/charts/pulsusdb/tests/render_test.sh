#!/usr/bin/env bash
# Render checks the goldens cannot make (issue #506), each over several
# renders of the chart:
#
#  A. The schema Job's name ends in the first eight hex digits of the
#     sha256 of its rendered `spec`. That is the whole property: a Job's pod
#     template is immutable, so a `spec` change under an unchanged name makes
#     `helm upgrade` fail, and a name that changes without the `spec`
#     changing reruns the Job, which drops the database.
#  B. A value the Job does not carry leaves the Job document byte-identical.
#  C. `pulsusdb.extraEnv` is the last environment entry of every pulsusdb
#     container, the Job included, so it can override the chart's own.
#  D. Every generated name is within its kind's length limit.
#
# Run from the repo root:
#   deploy/charts/pulsusdb/tests/render_test.sh
set -uo pipefail

CHART="deploy/charts/pulsusdb"
failures=0
fail() { echo "FAIL: $*"; failures=$((failures + 1)); }

# The Job document of a render, from its `# Source:` line to the next `---`.
job_doc() {
    awk '/^# Source: pulsusdb\/templates\/init-job.yaml$/ { inside = 1; next }
         inside && /^---$/ { exit }
         inside { print }'
}

# A. name suffix == sha256 of the spec, de-indented as the template wrote it.
check_digest() {
    local what="$1"; shift
    local doc name spec digest
    doc=$(helm template pulsusdb "$CHART" "$@" | job_doc) || { fail "$what: render failed"; return; }
    [ -n "$doc" ] || { fail "$what: no schema Job"; return; }
    name=$(awk '/^  name: / { print $2; exit }' <<<"$doc")
    spec=$(awk 'found { sub(/^  /, ""); print } /^spec:$/ { found = 1 }' <<<"$doc")
    digest=$(printf '%s' "$spec" | sha256sum | cut -c1-8)
    if [ "${name##*-}" = "$digest" ]; then
        echo "ok:   A $what ($name)"
    else
        fail "A $what: name $name, spec digest $digest"
    fi
}

check_digest "default"
check_digest "split" --set pulsusdb.split.enabled=true
check_digest "cluster" --set topology=cluster --set clickhouse.shards=3
check_digest "a schema parameter" --set pulsusdb.config.retention_days=14
check_digest "the image tag" --set image.tag=other
check_digest "image pull secrets" --set 'imagePullSecrets[0].name=regcred'
check_digest "the service account" --set serviceAccount.name=other
check_digest "the Job's resources" --set initJob.resources.limits.memory=2Gi
check_digest "the Job's retry limit" --set initJob.backoffLimit=3
check_digest "the pod security context" --set podSecurityContext.runAsUser=10002
check_digest "the container security context" --set securityContext.readOnlyRootFilesystem=false
check_digest "an extra variable" --set 'pulsusdb.extraEnv[0].name=X' --set-string 'pulsusdb.extraEnv[0].value=1'

# B. values the Job does not carry.
base=$(helm template pulsusdb "$CHART" | job_doc)
for args in "--set pulsusdb.config.reader.cache_ttl=30s" \
            "--set pulsusdb.auth.user=admin --set pulsusdb.auth.password=x"; do
    # shellcheck disable=SC2086
    other=$(helm template pulsusdb "$CHART" $args | job_doc)
    if [ "$other" = "$base" ]; then echo "ok:   B $args"; else fail "B $args: the Job changed"; fi
done

# C. extraEnv last in every pulsusdb container.
last_env() {
    awk '/^# Source: /{ src = $3 }
         match($0, /^ *env:$/) { ind = RLENGTH - 4; inenv = 1; last = ""; next }
         inenv {
             pre = sprintf("%" (ind + 2) "s", "")
             if (index($0, pre "- name: ") == 1) { last = substr($0, length(pre) + 9); next }
             match($0, /^ */)
             if (RLENGTH <= ind) { print src, last; inenv = 0 }
         }'
}
# The pulsusdb containers each render must hold, exactly.
expected_sources() {
    case "$1" in
        false) printf '%s\n' pulsusdb/templates/deployment-all.yaml pulsusdb/templates/init-job.yaml ;;
        true) printf '%s\n' pulsusdb/templates/deployment-reader.yaml pulsusdb/templates/deployment-writer.yaml \
                  pulsusdb/templates/init-job.yaml ;;
    esac
}
for split in false true; do
    out=$(helm template pulsusdb "$CHART" --set pulsusdb.split.enabled=$split \
        --set 'pulsusdb.extraEnv[0].name=ZZ_EXTRA' --set-string 'pulsusdb.extraEnv[0].value=1' | last_env)
    found=""
    while read -r src last; do
        case "$src" in
            pulsusdb/templates/deployment-*.yaml|pulsusdb/templates/init-job.yaml)
                found="$found$src"$'\n'
                [ "$last" = ZZ_EXTRA ] || fail "C split=$split: $src ends with $last" ;;
        esac
    done <<<"$out"
    if [ "$(printf '%s' "$found" | sort)" = "$(expected_sources "$split" | sort)" ]; then
        echo "ok:   C split=$split"
    else
        fail "C split=$split: containers $(printf '%s' "$found" | tr '\n' ' '), expected $(expected_sources "$split" | tr '\n' ' ')"
    fi
done

# D. Every generated name is within its kind's limit, with a release name at
#    Helm's 53-character limit and every optional object enabled. The limits
#    come from the API's own name rules (the upstream validation source):
#      Service       63   ValidateServiceName = NameIsDNS1035Label
#      Job           63   its pods must carry `batch.kubernetes.io/job-name`
#                         = the name, and a label value is at most 63
#      StatefulSet   52   NameIsDNSLabel (63), and its pods' revision label
#                         is `<name>-<hash>`, the hash at most 10 characters
#      Deployment   242   NameIsDNSSubdomain (253), and its ReplicaSet is
#                         `<name>-<hash>` (a DNS subdomain, hash at most 10);
#                         pod names are generated from it and cut to 58 plus
#                         5 random characters, so they add no limit
#      Pod, Ingress, HorizontalPodAutoscaler, ServiceAccount, ConfigMap,
#      Secret, PersistentVolumeClaim
#                   253   NameIsDNSSubdomain
#      PodDisruptionBudget
#                   253   no kind rule beyond the generic path-segment check;
#                         253 is this chart's bound
#    The datasource `uid`: 40. A kind this table does not name fails, so a new
#    kind is classified when it is added.
long_release=$(printf 'r%.0s' $(seq 1 53))
limit_for() {
    case "$1" in
        StatefulSet) echo 52 ;;
        Service|Job) echo 63 ;;
        Deployment) echo 242 ;;
        Pod|Ingress|HorizontalPodAutoscaler|ServiceAccount|ConfigMap|Secret|PersistentVolumeClaim|PodDisruptionBudget) echo 253 ;;
        *) echo 0 ;;
    esac
}
check_names() {
    local what="$1"; shift
    local out
    out=$(helm template "$@" "$CHART" --set ingress.enabled=true --set hpa.enabled=true \
        --set podDisruptionBudget.enabled=true --set grafana.datasourceProvisioning.enabled=true \
        --set pulsusdb.config.compat_endpoints=true --set pulsusdb.spool.persistence.enabled=true \
        --set pulsusdb.auth.user=a --set pulsusdb.auth.password=b | awk '
            /^---$/ { kind = ""; inmeta = 0 }
            /^kind: / { kind = $2 }
            /^metadata:$/ { inmeta = 1; next }
            /^[^ ]/ { inmeta = 0 }
            inmeta && /^  name: / { print kind, $2; inmeta = 0 }
            /^ +uid: / { print "uid", $2 }') || { fail "D $what: render failed"; return; }
    local n=0 kind name limit
    while read -r kind name; do
        n=$((n + 1))
        if [ "$kind" = uid ]; then limit=40; else limit=$(limit_for "$kind"); fi
        [ "$limit" -gt 0 ] || { fail "D $what: kind $kind has no limit in this test"; continue; }
        [ "${#name}" -le "$limit" ] || fail "D $what: $kind $name is ${#name} characters, over $limit"
    done <<<"$out"
    if [ "$n" -ge 10 ]; then echo "ok:   D $what ($n names)"; else fail "D $what: found only $n names"; fi
}
check_names "single" "$long_release"
check_names "cluster, split" "$long_release" --set topology=cluster --set clickhouse.shards=3 --set pulsusdb.split.enabled=true
check_names "a 63-character fullnameOverride" pulsusdb --set fullnameOverride="$(printf 'f%.0s' $(seq 1 63))" \
    --set topology=cluster --set clickhouse.shards=3 --set pulsusdb.split.enabled=true

echo "render_test: $failures failed"
[ "$failures" = 0 ]
