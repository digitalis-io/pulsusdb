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
exact-match, `--help`, non-root `id`) passes against the freshly built
image; a failing smoke test aborts the job before anything is pushed, so a
tag can never end up pointing at a broken image on GHCR.

The runtime stage runs as the non-root `pulsus` user (uid/gid `10001`) with
its working directory set to `/var/lib/pulsusdb`, owned by that user — so
the writer's insert-failure spool (`./spool/{poison,uncertain}/<table>/`,
docs/configuration.md §5) resolves to `/var/lib/pulsusdb/spool/` and is
writable. Mount a volume over `/var/lib/pulsusdb` if you need spooled
batches to survive a container restart.

**amd64-only for now.** The build does not (yet) produce an `arm64`
manifest — `linux/amd64` only. A multi-arch (arm64 via buildx/QEMU) build is
a tracked follow-up, not implemented here; tag/label/digest shape does not
change when it lands; if you need arm64 today, build the `Dockerfile`
yourself with `docker buildx build --platform linux/arm64 ...`.

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

## 5. Verifying a release

```sh
docker pull ghcr.io/digitalis-io/pulsusdb:1.2.3
docker run --rm ghcr.io/digitalis-io/pulsusdb:1.2.3 --version
# pulsusdb 1.2.3 (<40-char commit sha>)
```

`/status/buildinfo` on a running container reports the same `version`/
`revision` pair (docs/api.md §7) — both are stamped from the same
`PULSUS_BUILD_VERSION`/`PULSUS_BUILD_REVISION` build-args the image was
built with, so `--version`, `/status/buildinfo`, and the image's
`org.opencontainers.image.version`/`.revision` labels always agree.
