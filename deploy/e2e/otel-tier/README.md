# otel-tier

Three-tier (frontend → middletier → backend) HTTP checkout service, vendored
(copied, not a submodule) from `terraform-google-monitoring`'s
`traffic-gen/loaders/otel-tier` for use by
[`deploy/e2e/compose.tier.yaml`](../compose.tier.yaml)'s local trace demo.
One binary, three roles selected by `ROLE` (`frontend`/`middletier`/`backend`);
each hop propagates the W3C `traceparent` header so a single trace spans all
three services, with realistic child spans, injected slow queries (~20%) and
hard failures (~14% at the backend). See `compose.tier.yaml`'s header comment
for how to run it.

Source of truth is the origin repo — changes here are a point-in-time copy
and won't sync automatically.
