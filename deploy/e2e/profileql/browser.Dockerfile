# Issue #832: the headless browser for the profiles end-to-end scenario, with
# its script library installed at build time from the committed lock, so a
# run fetches nothing from a package registry. Its build context is
# e2e/profileql. Published by .github/workflows/profileql-e2e-images.yml; the e2e
# stack pins the digest that workflow prints.
FROM mcr.microsoft.com/playwright:v1.65.1-noble@sha256:1507cbfc550a63a6b7396791ca4f00df17beb56342d2447b3f106c306f525579

WORKDIR /opt/profileql
COPY package.json package-lock.json ./
RUN npm ci --no-audit --no-fund --ignore-scripts
