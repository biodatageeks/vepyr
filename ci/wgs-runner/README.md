# /parity-wgs runner kit

Everything the four self-hosted runners (`gh-runner-bdg-1`..`4`, flavour
`huge-c10m25`; 10 vCPU and about 25 GB each) need to run the whole-genome VEP
116 parity check (`/parity-wgs`). The job runs in a locally
built image with the staged data mounted read-only; `run-profile.sh` is the
in-container entrypoint (`run-profile.sh <profile> <wheel-path>`, repo at
`/src`, data at `/data`, scratch at `/work`).

Exit codes of `run-profile.sh`: 0 parity match, 1 md5 mismatch, 3 staged data
differs from `e2e-testing/golden/wgs-manifest.json`, anything else is an error.
It writes `/work/result.json` and `/work/summary.txt`.

## One-time setup

1. Install Docker, add the runner user to the `docker` group and restart the
   runner service.
2. Add the runner label `vepyr-wgs` (org settings, Actions, Runners, `gh-runner-bdg-1`).
3. Restrict the runner group to repository `biodatageeks/vepyr` and workflows
   `biodatageeks/vepyr/.github/workflows/parity-wgs.yml@refs/heads/master` and
   `.../parity-caches.yml@refs/heads/master` where the plan allows it. **Currently not done** (see "Accepted residual
   risk"): the runner is in the shared default group `bdg-runners`.
4. Create `/data/vepyr` and `/scratch/vepyr-wgs`, owned by the runner user.
5. **Mandatory: egress firewall.** Job containers run PR code with
   `--network bridge` (dependencies come from PyPI). They must reach only the
   public internet, never the host, the LAN or cloud metadata. From a checkout
   of `master`:

   ```bash
   # Containers must not use a LAN resolver once RFC1918 is dropped. Merge the
   # keys into any existing daemon.json (needs jq); do not overwrite it.
   sudo test -f /etc/docker/daemon.json || echo '{}' | sudo tee /etc/docker/daemon.json >/dev/null
   sudo jq '. + {"dns": ["1.1.1.1", "8.8.8.8"], "ipv6": false}' /etc/docker/daemon.json \
     | sudo tee /etc/docker/daemon.json.new >/dev/null
   sudo mv /etc/docker/daemon.json.new /etc/docker/daemon.json
   sudo systemctl restart docker
   sudo install -m 0755 ci/wgs-runner/egress-firewall.sh /usr/local/sbin/vepyr-wgs-egress
   sudo tee /etc/systemd/system/vepyr-wgs-egress.service >/dev/null <<'UNIT'
   [Unit]
   Description=Egress firewall for /parity-wgs job containers
   After=docker.service
   Requires=docker.service
   PartOf=docker.service

   [Service]
   Type=oneshot
   ExecStart=/usr/local/sbin/vepyr-wgs-egress
   RemainAfterExit=yes

   [Install]
   WantedBy=docker.service
   UNIT
   sudo systemctl daemon-reload
   sudo systemctl enable --now vepyr-wgs-egress.service
   sudo iptables -S DOCKER-USER; sudo iptables -S INPUT; sudo ip6tables -S INPUT   # expect the docker0 DROPs
   ```

   The script inserts, for traffic entering from `docker0`, a `DROP` in
   `DOCKER-USER` for 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16,
   169.254.0.0/16 and 100.64.0.0/10, and an `INPUT` `DROP` that blocks the
   docker0 host address (and every other host address), with the same `INPUT`
   rule in `ip6tables`. That `INPUT` drop also cuts every other container on
   this host's default bridge off from host services (a host DNS stub, a
   local proxy or registry); run such containers on a user-defined network
   or not on this host. The script is idempotent and
   the unit re-applies it whenever Docker starts, so the rules survive reboots
   and Docker restarts (a systemd unit rather than `netfilter-persistent`,
   whose boot-time restore would also replay stale Docker rules).
   `check.sh` fails if any rule is missing.
6. In a checkout of `master`, run `ci/wgs-runner/build-image.sh`.
7. Run `ci/wgs-runner/stage-data.sh --goldens-from <user@host:~/workspace/data_vepyr/output/116> --clinvar-input-from <user@host:~/workspace/data_vepyr/input/clinvar>`.
   Needs `uvx`, `curl`, `rsync`, `python3` (3.11+) on the host.
8. Run `ci/wgs-runner/check.sh` (as root or with passwordless sudo, so it can
   read the firewall rules; otherwise it warns and you check them by hand).
9. For `/parity-caches`, which runs every job on the host (see "Where the jobs
   run"): the runner service needs `gh` and `python3` (3.10 or newer) on its
   `PATH`, and `git` 2.28 or newer for the sparse checkouts. The `gha_runner`
   role installs none of them explicitly, so check each host as the runner
   user: `gh --version; python3 --version; git --version`. The jobs that call
   `gh` or `python3` fail in their first step when one is missing.

## Re-staging when the manifest changes

`wgs-manifest.json` pins the goldens, inputs, tabix indexes and cache revisions. When it
changes (a new cache revision or regenerated goldens), re-run step 7. The
script is idempotent: existing verified downloads are reused, and the final
`record-staged` rewrites `STAGED.json`. A cache at a new revision fetches only
the files that changed: it is staged in a hard-link copy of the current cache,
swapped in once complete, and stripped of files the new revision dropped. Until then every job exits 3 (stale
data) rather than comparing against the wrong files. `check.sh` shows the
same drift without running a job.

## Parallelism

Four hosts carry the `vepyr-wgs` label (`gh-runner-bdg-1`..`4`, `huge-c10m25`,
one runner each, all provisioned by the `openstack-bdg-runners` `gha_runner`
role with their own `/data/vepyr`, scratch and memory). A runner takes one job
at a time, and the profile matrix uses `max-parallel: 4`, so one `/parity-wgs`
request runs `merged`, `ensembl`, `refseq` and `clinvar_merged` on all four
hosts at once. Further jobs queue FIFO (up to 24 hours)
for a free host. The workflow still has no concurrency group, because GitHub
keeps only one pending run per group and would evict a queued admin run.

`/parity-caches` takes the same four hosts for every job, not only for its
target matrix (`max-parallel: 4`): the command check, the authorization, the
wheel build and the report each wait for a free host. While four long jobs
run, a new `/parity-caches` comment gets no reaction or reply, and a finished
run posts no statuses, until a host is free. GitHub cancels a job that has
queued for 24 hours; a run whose report was cancelled leaves its statuses
`pending`.

Do not register a second runner service on the same host: two jobs there would
share one `/data/vepyr`, scratch disk and memory budget, and one workspace.

## Runtime

Measured HG002 times on 2026-10-11 on `gh-runner-bdg-1` (then 16 vCPU, 64 GB):
`merged` 29 min, `ensembl` 23 min, `refseq` 21 min per job. They retain
`--md5-mode both` and have a 90-minute timeout to allow for the current
10-vCPU hosts until their runtime is measured there.

ClinVar was measured locally on 2026-11-01 with 10 workers: 25 contigs took
92 minutes, peak process RSS was 15.2 GiB, and peak temporary slices were
1.04 GiB with per-contig cleanup. `clinvar_merged` uses a 20 GiB container cap,
matching the measured run, and a 240-minute timeout (about 2.6x the measured
wall time). HG002 containers get a 22 GiB cap pending a measurement on the
10-vCPU hosts. ClinVar checks `strict-clin-sig`: it sorts only the known
VEP-nondeterministic `CLIN_SIG` groups (vepyr#238), keeping INFO order, QUAL,
FORMAT keys and all other record bytes strict. The other three profiles check
both strict and canonical md5. A diagnostic `--md5-mode both` run reports the
expected strict difference at chr22 and canonical concordance on all 25.

## Statuses

`parity/wgs` is the verdict to rely on: it is confirmed against the profile
jobs' result and is `error` (not `failure`) when any profile hit stale data, a
crash or a failed wheel build. `parity/wgs/<profile>` are informational and
taken from the profile's own result file; do not make them required checks.
A cancelled run posts nothing, so `parity/wgs` stays `pending` (never green).

## Rebuilding the image

When `Dockerfile` changes, bump `IMAGE` (for example `vepyr-wgs:2`) and the
workflow's `WGS_IMAGE` in the same commit, merge, then run `build-image.sh`
from a checkout of `master` on the runner. Jobs never pull the image; a tag
that was not built locally fails fast.

## Cache parity (`/parity-caches`)

`.github/workflows/parity-caches.yml` uses the same four hosts, image,
container hardening and admin-only authorization as `/parity-wgs`. Each job
runs `run-cache-parity.sh <target> <wheel-path>` in the container: it rebuilds
one published cache with the PR's wheel and compares it with the revision
pinned in `src/vepyr/_published.py`
(`e2e-testing/scripts/cache_parity.py`). A target is a cache type (`ensembl`,
`merged`, `refseq`) or `plugin_<name>`; the list comes from master's
`_published.py`, and timeouts, memory caps and the opt-in plugins from master's
`e2e-testing/golden/cache-parity-manifest.json`.

Exit codes of `run-cache-parity.sh`: 0 the rebuilt cache is the published one,
1 parity mismatch, 3 the runner is not ready (staged data missing or at
another revision, scratch too small), anything else is an error. It writes
`/work/result.json`, `/work/summary.txt` and
`/work/out/reports/cache-parity.json` (every shard, with the differing columns
and example rows).

### Where the jobs run

All five jobs (`command`, `authorize`, `build-wheel`, `cache`, `report`) run on
the `vepyr-wgs` hosts; the workflow uses no GitHub-hosted runner. `/parity-wgs`
is different: only its `profile` job runs here.

- `command`, `authorize` and `report` check out and run the workflow's own ref
  only, which `authorize` requires to be `master`: its helper, pins and
  manifest. They never check out the PR. A `/parity-caches` comment from
  anyone starts `command` on a host; it runs master's parser on the comment
  text and nothing else.
- `build-wheel` builds the PR's wheel as `publish_to_pypi.yml` builds the
  x86_64 release wheel on these hosts: `maturin-action` in the
  `quay.io/pypa/manylinux2014_x86_64` image pinned by digest, in a `--rm`
  container that installs Rust itself and mounts only the workspace. The
  container runs as root on Docker's default network (the egress firewall of
  step 5 applies), with `no-new-privileges`, without `NET_RAW` and `MKNOD`,
  with a pid limit and, as in the release build, without a memory cap. It is
  named and labelled `vepyr-wgs=1`, so a container that outlived a cancelled
  job is removed by the next job on that host.
- Around that container the host refuses a PR checkout that holds a symbolic
  link (`maturin-action` reads `rust-toolchain.toml` and `pyproject.toml` on
  the host), hides the runner's ssh agent and git credentials from
  `maturin-action`, shadows `sudo`, and takes exactly one regular,
  size-capped file with a wheel name out of `dist/` into a host-made
  directory, from which the artifact is uploaded.
- Every job starts by removing labelled containers and emptying the
  workspace, as root in the pinned image when the runner user cannot delete an
  entry (the wheel build writes as root), and fails if anything is left.
  `build-wheel` does the same at its end, also after a failure.

### What a job compares

- Core cache: `vepyr.build_cache(..., local_cache=<raw cache>)` into scratch,
  compared with `/data/vepyr/cache/<release>_<assembly>_<type>`, the copy
  `stage-data.sh` already stages for `/parity-wgs`. Its `.revision` marker must
  name the pinned revision, otherwise the job exits 3. Per entity and contig
  shard: row count, schema (names, order, types, nullability, key-value
  metadata) and every column in row order; also `chrom_manifest.json`,
  `reference_policy.json` and `chr_synonyms.txt`.
- Plugin cache: `vepyr.build_plugin_cache(...)` into scratch, for the contigs,
  source files and `vepyr-plugins` commit that the published `manifest.json`
  records (`chroms`, `sources`, `cache_source_version`), with tiers taken from
  the staged `merged` cache. Compared like a core cache, plus the warm/cold
  counts of the `tier` column and of `manifest.json`. `verify_source` is strict
  when the published build verified the upstream digest, and `warn` when its
  input was a derived file (AlphaMissense's BGZF re-compression).
- Rows in another order but otherwise identical pass with a note, for shards
  of up to 2,000,000 rows; a larger shard is compared in row order only.

### Staged inputs

`stage-cache-inputs.sh [--data /data/vepyr] [--select "<selectors>"]` stages
them next to the `/parity-wgs` data (run `stage-data.sh` first). It needs
`uvx`, `curl`, `python3`, `md5sum`, `tar`, and `bgzip`/`tabix` for
AlphaMissense. Without `--select` it stages every default target; an opt-in
plugin is staged only when named (`--select "core plugins cadd spliceai"`).

| Path under `/data/vepyr` | Content | Size |
|---|---|---|
| `cache/116_GRCh38_{ensembl,merged,refseq}` | published core caches (from `stage-data.sh`) | 97 GB |
| `vep-cache/homo_sapiens{,_merged,_refseq}/116_GRCh38` | raw Ensembl VEP caches, unpacked from `indexed_vep_cache/*_vep_116_GRCh38.tar.gz` and checked against Ensembl's `CHECKSUMS` | about 150 GB |
| `plugin-cache/116_GRCh38/plugin/<plugin>` | published plugin cache at the pinned revision, with a `.revision` marker | 81 MB ClinVar, 541 MB AlphaMissense, 2 MB PhenotypeOrthologous |
| `plugin-source/<plugin>/<file>` (+ index) | the files listed under `sources` in the published `manifest.json`, under the names recorded there, fetched from the recorded `url` and checked against the recorded `md5` | 192 MB ClinVar, 628 MB AlphaMissense, 3 MB PhenotypeOrthologous |

A plugin cache that is not staged, or is staged at another revision than the
PR pins, is downloaded inside the job with `vepyr.download_plugin_cache` (the
egress firewall allows the public internet, so Hugging Face is reachable from
the container); staging it avoids the download and is what the opt-in plugins
need. Core caches are never downloaded in a job: a staged core cache at
another revision ends the job with exit code 3. The plugin manifests are cloned from GitHub by
`build_plugin_cache` itself.

Opt-in plugins (`opt_in` in the manifest; not staged and not run unless
named):

| Plugin | Sources | Published cache | Why opt-in |
|---|---|---|---|
| CADD | 87.5 GB + 1.3 GB | 71 GB | size; free for non-commercial use only |
| SpliceAI | 28.5 GB | 23 GB | size; Illumina's licence restricts use |

dbNSFP has no published cache (its licence forbids redistribution), so it is
not a target. AlphaMissense's build input is a BGZF re-compression of the
upstream gzip file; the staging script prepares it as
`vepyr-plugins/plugins/alphamissense/README.md` describes. BGZF output is not
byte-stable across htslib versions, so the job reports a digest that differs
from the published build's as a note, not as a failure.

A core job needs about 60 GB of scratch for the rebuilt cache (`scratch_gb` in
the manifest; the job exits 3 when `/scratch` has less), which is more than the
20 GB `check.sh` asks for.

### Statuses

`parity/caches` is posted only by a run that covers every default target, and
follows the rules of `parity/wgs`: `error` when any target hit missing data or
a crash, `failure` for mismatches alone. `parity/caches/<target>` are
informational. A run of selected targets posts only those.

## Accepted residual risk

The runner agent and its `_work` directory persist between jobs. A hostile
fork PR, after an admin has commented `/parity-wgs` on it, could run code that
leaves files behind for a later job to read (design spec, section 5).
Mitigations:

- the staged data is mounted read-only and the job container is throw-away
  (no capabilities, read-only root, non-root user, pid and memory limits);
- the host never follows a path the PR controls: after checkout it does not
  touch the workspace, and it reads the container's scratch only after
  force-removing the (named, labelled) job container and deleting every symlink and special file in it, copying size-capped regular
  files to a host-made directory from which all artifacts are uploaded;
- the egress firewall (setup step 5) keeps job containers off the host and
  the LAN;
- no secrets are available to a job that runs PR code, and its GitHub token
  is read-only;
- privileged jobs of `/parity-wgs` (anything with secrets or write access)
  never run on this machine; for `/parity-caches` see below;
- `_work` and scratch are cleaned before and after each job;
- a fork PR must pin the reviewed head (`/parity-wgs <sha>`), so a push after
  the admin's comment cannot run; any pinned SHA must equal the head.

The main defence is the admin's review of the PR before commenting
`/parity-wgs`. The upgrade path, if that is not enough, is ephemeral runners.

**All `/parity-caches` jobs on these hosts (owner's decision, 2026-11-21).**
Two things differ from `/parity-wgs`:

- `authorize` and `report` run here with a token that can write commit
  statuses and PR comments. They run master's code in an emptied workspace,
  but a host on which earlier code gained the runner user (see the shared
  runner group below) can use that token while such a job runs, for example
  to post a green `parity/*` status.
- The wheel is built from PR code here, as root in a container that can write
  to the workspace. The job deletes that tree as root when it ends. If that
  step does not run (the host reboots or the runner is killed during a
  build), root-owned files written by PR code stay in the workspace. Every
  `/parity-caches` job and the release build delete them as root before
  anything else; the `profile` job of `/parity-wgs` ignores entries it cannot
  delete and then runs `actions/checkout` in that directory. Giving that job
  the same first step closes this.

**Shared runner group (accepted 2026-10-11).** `gh-runner-bdg-1` is in the
org's default runner group `bdg-runners`, which is not restricted to a
workflow and serves eight repositories (vepyr, polars-bio, the sequila repos,
openstack-bdg-runners, biodatageeks.github.io). The organization is on the
Free plan. Any workflow in those repositories that targets this runner's
labels runs **on the host**, outside the container, as the runner user
(docker group, so root-equivalent): an outside contributor's PR that is
auto-approved (vepyr's fork policy only gates first-time contributors), or
anyone with write access to one of those repositories, could alter
`/data/vepyr`, the image or the firewall, or leave files for later jobs. The
md5 verdicts are only as trustworthy as that host. To close it: a dedicated
VM registered as a vepyr repository runner, with fork-PR approval required for
all outside collaborators, or a workflow-restricted runner group (GitHub
Team).

Two further rules for the host:

- Docker-group membership is root-equivalent for anything running as the
  runner user, so treat the runner user as root on this machine.
- Never leave a private SSH key on the runner after the goldens rsync. Use
  agent forwarding, or push the goldens from the maintainer's machine.
