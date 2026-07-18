# Releasing

How to cut a PulsusDB release: publishing a versioned container image to
GHCR. This is the procedure `.github/workflows/release.yml` (issue #23)
automates once a `v*` tag is pushed — there is no separate "release" button
or draft-release step; the tag *is* the release trigger.

## 1. Cut a tag

```sh
git tag v1.2.3
git push origin v1.2.3
```

Tags must match `v*` (the workflow's only trigger) and should be a valid
[semver](https://semver.org/) after the `v` — e.g. `v1.2.3` or a prerelease
like `v1.2.3-rc.1`. Pushing the tag starts the `Release` workflow; nothing
else (no manual dispatch, no GitHub Releases UI action) is required.

## 2. What gets published

The workflow builds the repo's `Dockerfile` (the same one CI's `e2e-single`/
`e2e-cluster` legs build) with `PULSUS_BUILD_VERSION=<tag>` and
`PULSUS_BUILD_REVISION=<github.sha>` build-args, so the image's `--version`
output and `/status/buildinfo` response are stamped with the exact tag and
commit — not the `0.0.0`/local-git-SHA dev-build fallback. It is pushed to
`ghcr.io/digitalis-io/pulsusdb` **only after** a smoke test (`--version`
exact-match, `--help`, non-root `id`, spool write probe) passes against the
freshly built image; a failing smoke test aborts the job before anything is
pushed, so a tag can never end up pointing at a broken image on GHCR.

**Multi-arch (`linux/amd64` + `linux/arm64`).** The tag now resolves to a
multi-arch **manifest list (OCI image index)** with one child manifest per
arch, so `docker pull` picks the right image for the host automatically. Both
arches are built via buildx: amd64 natively, arm64 under QEMU emulation
(`docker/setup-qemu-action`) — the `Dockerfile` compiles the binary inside
the build stage unmodified, so its C-backed dependencies build natively in
the emulated arm64 stage with no cross-toolchain. Because `load: true` cannot
load a manifest list, each arch is first built, loaded, and smoke-tested
separately (arm64's smoke runs the same assertion set under
`docker run --platform linux/arm64`), and only then is a single cache-warm
multi-platform image pushed. The tag/label scheme is unchanged; only the
digest a tag points to is now an image **index** rather than a single image
manifest (pull-by-tag and pull-by-child-digest both still work).

A build-only dry-run is available via the workflow's `workflow_dispatch`
trigger (Actions → Release → Run workflow): it compiles both arches under
QEMU with no push and no load, so you can prove the arm64 build works before
merging a change to the release path — the tag-only trigger otherwise gives
no pre-merge signal.

The runtime stage runs as the non-root `pulsus` user (uid/gid `10001`) with
its working directory set to `/var/lib/pulsusdb`, owned by that user — so
the writer's insert-failure spool (`./spool/{poison,uncertain}/<table>/`,
docs/configuration.md §5) resolves to `/var/lib/pulsusdb/spool/` and is
writable. Mount a volume over `/var/lib/pulsusdb` if you need spooled
batches to survive a container restart.

See "Multi-arch" above: the published tag is a two-arch (`linux/amd64` +
`linux/arm64`) manifest list, so no separate per-arch build is needed to run
PulsusDB on arm64 — `docker pull` resolves the matching image automatically.

## 3. Tag → published-tags policy

Tags are derived by `docker/metadata-action@v5`'s `type=semver` with
`flavor: latest=auto`. **Invariant: `latest` always resolves to the newest
*stable* release — it never moves to point at a prerelease.** A prerelease
ref publishes *only* its exact version tag (no `major.minor`, no `latest`) —
this is `metadata-action`'s own documented `type=semver` behaviour, not
something this workflow computes itself.

| Pushed ref      | Tags published           | `latest` moved? |
|------------------|---------------------------|------------------|
| `v1.4.2`         | `1.4.2`, `1.4`, `latest`  | yes              |
| `v1.5.0-rc.1`    | `1.5.0-rc.1`              | no               |
| `v2.0.0`         | `2.0.0`, `2.0`, `latest`  | yes              |

On the first real release, diff `steps.meta.outputs.tags` (echoed into the
`meta` step's job log) against this table to confirm the policy held.
Re-check this table — and the release workflow's inline comment above its
`tags:` block — whenever `docker/metadata-action`'s pinned major version is
bumped; the exact-only-for-prereleases behaviour is an upstream contract,
not something enforced in this repo's YAML.

## 4. One-time: make the GHCR package public

The **first** push to `ghcr.io/digitalis-io/pulsusdb` creates the package as
**private** by default. This is a one-time, manual GitHub repo/org setting,
not something the workflow can do (a `GITHUB_TOKEN` cannot change package
visibility): after the first release lands, a repo owner must go to the
package's GHCR settings page and:

1. Set visibility to **Public** (so the §10 quickstart's `docker pull` works
   without authentication).
2. Link the package to this repository (so it shows up under the repo's
   "Packages" sidebar and inherits the repo's access controls going
   forward).

Every subsequent tag push reuses the same (now public, linked) package —
this step never needs repeating.

cosign attaches the signature and attestations (`*.sig`, `*.att`) as extra
OCI tags on this **same** package, so making the package Public also makes
them anonymously pullable — a consumer's `cosign verify` / `gh attestation
verify` (§5) would otherwise return 401/404 while the package is still
private. No separate visibility step is needed for the signature artifacts.

## 5. Verifying a release

```sh
docker pull ghcr.io/digitalis-io/pulsusdb:1.2.3
docker run --rm ghcr.io/digitalis-io/pulsusdb:1.2.3 --version
# pulsusdb 1.2.3 (<40-char commit sha>)
```

Confirm the tag is a two-arch index (the release job's own **Verify
multi-arch manifest** step asserts this and fails the release if an arch is
missing, but you can re-check by hand):

```sh
docker buildx imagetools inspect ghcr.io/digitalis-io/pulsusdb:1.2.3
# ...
# Manifests:
#   ... Platform: linux/amd64
#   ... Platform: linux/arm64
# (or `docker manifest inspect ghcr.io/digitalis-io/pulsusdb:1.2.3` to see
#  the raw index with an amd64 and an arm64 entry)
```

`/status/buildinfo` on a running container reports the same `version`/
`revision` pair (docs/api.md §7) — both are stamped from the same
`PULSUS_BUILD_VERSION`/`PULSUS_BUILD_REVISION` build-args the image was
built with, so `--version`, `/status/buildinfo`, and the image's
`org.opencontainers.image.version`/`.revision` labels always agree.

### Verifying signatures and attestations (image)

Each release is signed and attested with **keyless cosign** (Sigstore): the
release workflow's OIDC identity → a short-lived Fulcio certificate → the
Rekor public transparency log. There is no long-lived signing key to manage
or trust — verification checks that the artifact was produced by *this
repository's release workflow at a `v*` tag*. Every artifact is bound to the
immutable image **index digest** (which transitively covers both the amd64
and arm64 child manifests), never a mutable tag.

Install [cosign](https://docs.sigstore.dev/cosign/system_config/installation/)
and (for provenance) the [GitHub CLI](https://cli.github.com/). Resolve the
tag to its index digest, then verify each artifact with the verifier that
owns it:

```sh
DIGEST=$(docker buildx imagetools inspect \
  ghcr.io/digitalis-io/pulsusdb:1.2.3 --format '{{.Manifest.Digest}}')
REF="ghcr.io/digitalis-io/pulsusdb@${DIGEST}"

# 1. Signature (cosign):
cosign verify \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-identity-regexp '^https://github\.com/digitalis-io/pulsusdb/\.github/workflows/release\.yml@refs/tags/v' \
  "$REF"

# 2. SBOM attestation, SPDX-JSON (cosign):
cosign verify-attestation --type spdxjson \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-identity-regexp '^https://github\.com/digitalis-io/pulsusdb/\.github/workflows/release\.yml@refs/tags/v' \
  "$REF"

# 3. SLSA build provenance (SLSA v1.0). Verify with its NATIVE verifier,
#    `gh attestation verify` — NOT `cosign verify-attestation --type
#    slsaprovenance`, whose type maps to the older SLSA v0.2 predicate URI
#    and would silently fail to match this v1.0 attestation:
gh attestation verify "oci://${REF}" \
  --repo digitalis-io/pulsusdb \
  --signer-workflow digitalis-io/pulsusdb/.github/workflows/release.yml
```

The `--certificate-identity-regexp` pins any `v*` tag of this workflow; to
lock verification to one exact release, swap it for
`--certificate-identity https://github.com/digitalis-io/pulsusdb/.github/workflows/release.yml@refs/tags/v1.2.3`.
The SBOM is generated for a single platform (amd64) for now; per-arch SBOMs
are a possible follow-up. The release workflow runs these same three checks
against the freshly pushed digest as a post-publish self-check, so a release
that would not verify fails the job instead of shipping.

## 6. Releasing the Helm chart

The Helm chart under `deploy/charts/pulsusdb/` (issue #38) is versioned and
released **independently** of the application image above — a different
tag prefix, a different SemVer, a different workflow:

```sh
git tag helm-v0.2.0
git push origin helm-v0.2.0
```

`helm-v*` tags (never plain `v*`, which is the application image trigger
above) start `.github/workflows/helm-release.yml`, which:

1. Reads `deploy/charts/pulsusdb/Chart.yaml`'s `name`/`version`/`appVersion`.
2. Guards that `values.yaml`'s default `image.tag` (when non-empty) agrees
   with `Chart.yaml`'s `appVersion` — `appVersion` is the **released
   application image tag** this chart version was verified against, not
   the `pulsus-server` crate's `0.0.0` dev-build placeholder (Cargo.toml
   is not the version surface for this purpose).
3. Fails, rather than silently overwriting, if this chart version already
   exists at `oci://ghcr.io/digitalis-io/charts/pulsusdb` — the whole
   workflow runs under a single global `concurrency: helm-publish` group
   (no ref suffix) so two simultaneous tag pushes can never both pass this
   check before either publishes.
4. `helm package`s and `helm push`es to
   `oci://ghcr.io/digitalis-io/charts/pulsusdb`, then `helm pull`s the
   result back **by digest** (not by the mutable version tag) to verify
   the round trip before the job is considered successful.

Chart `version` must be bumped in `Chart.yaml` as part of the release PR —
the workflow's already-exists guard is what enforces this is not
forgotten, not a human process alone.

### Verifying the chart signature

The pushed OCI chart is signed with **keyless cosign** (issue #44), by its
immutable `helm push` digest, using the same Sigstore flow as the image
(OIDC → Fulcio → Rekor; no key material). The chart is **signature-only**
for v1 — a provenance/SBOM attestation for the chart is a follow-up. The
release workflow self-verifies the signature post-publish, so a chart that
would not verify fails the job.

Note cosign takes the **bare** registry path (no `oci://` prefix, which is a
Helm-only scheme):

```sh
CHART_DIGEST=$(docker buildx imagetools inspect \
  ghcr.io/digitalis-io/charts/pulsusdb:0.2.0 --format '{{.Manifest.Digest}}')

cosign verify \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-identity-regexp '^https://github\.com/digitalis-io/pulsusdb/\.github/workflows/helm-release\.yml@refs/tags/helm-v' \
  "ghcr.io/digitalis-io/charts/pulsusdb@${CHART_DIGEST}"
```

As with the image (§4/§5), the chart's `*.sig` artifact is an extra OCI tag
on the same package, so making the chart package Public (below) is also what
lets a consumer's `cosign verify` pull it anonymously.

### One-time: make the GHCR chart package public, and protect `main`

Same one-time manual step as §4 above, for the
`ghcr.io/digitalis-io/charts/pulsusdb` OCI package specifically (the first
`helm push` also creates it private by default) — set it Public and link
it to this repository.

**Also required (issue #38 AC #17), and equally something no workflow can
do on its own:** a repo owner must enable branch protection on `main`
requiring the `ci` workflow's checks **and** `chart-lint` /
`chart-unittest` / **`chart-test-kind`** (`.github/workflows/helm-chart.yml`)
to pass before merging. `chart-test-kind` in particular is not optional
once this is set up: `.github/workflows/helm-release.yml`'s AC #11
interpretation (§6 above) is that the tag-triggered publish workflow runs
only `helm lint --strict` + `helm-unittest` inline and deliberately does
**not** re-run the Kind behavioural suite — it relies on `chart-test-kind`
having already gated the `main` commit a `helm-v*` tag is cut from. If
`chart-test-kind` is not a required check, that reliance is false, and an
untested chart could be published. A `GITHUB_TOKEN` cannot modify
repository branch-protection settings, so this is a manual, one-time
action in the repository's Settings → Branches page.
