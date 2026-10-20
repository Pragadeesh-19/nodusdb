# Engineering review: the stable PyPI release, 0.1.0

Status: decisions D223 to D233 accepted on 2026-10-18. The work is built on branch release/0.1.0rc2. The release itself
(tagging, publishing, yanking) happens only after the branch is merged and each publish is approved by the owner.
Choices made while building are D234 to D243 in [`docs/decisions.md`](../decisions.md); where they differ from
this document they win.

The package on PyPI is `0.1.0rc1`, published on 2026-10-05 from commit `743f842`. Main is 182 commits ahead of it:
shipping, followers, the reachability index, erasure, watch and time travel have never been installed from PyPI by
anyone. Only a pre-release exists, so `pip install nodusdb` installs rc1 today while the README documents the later
features.

---

## 1. First principles

1. **A published version cannot be changed.** PyPI refuses a second upload under the same version and a yank only
   hides a release. Every check that can run before the upload must, and one check must run after it.
2. **Ship the artifact that was tested.** The native library in the wheel is the part of the package that decides
   speed and licensing. The toolchain that builds it must be the one the tests and the published numbers used.
3. **The wheel is a carrier.** Python loads the library through ctypes, so nothing in it depends on the CPython
   interpreter. The wheel tag should say so.
4. **Say what is promised.** A stable label tells people they can keep data in the product. The formats and API it
   covers are written down and each promise names the test behind it.
5. **One publishing path.** Trusted publishing behind a reviewer-gated environment, with no stored upload secret.

## 2. What was found

| Finding | Evidence |
|---|---|
| PyPI holds only `0.1.0rc1`; its page description is empty and the wheel has no license file | `METADATA` and the file list of `nodusdb-0.1.0rc1-cp312-cp312-win_amd64.whl` |
| The wheel toolchain moved from GraalVM CE 22.0.2 to Oracle GraalVM 25 after rc1 | commits `9cff696`, `9dd265b`, `1136b00` (2026-10-05 and 06) |
| Oracle GraalVM's free terms (GFTC) allow redistribution of native-image output only if no fee is charged for products that bundle it | Oracle's published summary of the license; not the full legal text and not legal advice |
| Every benchmark in the README was measured with CE 22.0.2 on the development laptop | `bench/baseline/MACHINE.md`, D57 |
| `native_ratio.py` allows the native library to be 50 times slower than the JVM; CI measured 2.9 to 8.2 | CI run 39051369528, `TODOS.md` |
| Wheels are built per CPython version (cp39 to cp313); Python 3.14 gets no wheel and there is no sdist | `wheels.yml`, PyPI file list |
| The publish job never compares the release tag with the package version, and keeps an unused API-token path | `wheels.yml` lines 146 to 184; rc1 run 37279187021 skipped the token step |
| `wheel_smoke.py` touches graph basics, a durable reopen, directory shipping and the lake table only | `python/tests/wheel_smoke.py` |
| HTTPS to a real S3 endpoint was tried only on Windows (a fake key returned HTTP 403 InvalidAccessKeyId) | probe run on 2026-10-18; `ca_bundle` is not in the README |
| setuptools refuses a `readme` or `license-files` entry outside the project directory | `DistutilsOptionError` from `_assert_local`, setuptools 80.9.0 |
| With `requires-python >=3.10` and rc1 the only release that fits 3.9, pip installs rc1 for 3.9 users | pip's pre-release rule; confirmed against a local index before release (D229) |

## 3. Decisions

| ID | Decision |
|---|---|
| D223 | Release path. Publish `0.1.0rc2`, install it from PyPI on clean machines, soak a day or two, then change only the version string and publish `0.1.0`. |
| D224 | Toolchain. The wheels and the native library the Python suite tests are built with GraalVM Community Edition, defined once in a local composite action. CE 25 is taken only if it measures within 10% of CE 22.0.2 on `native_ratio.py` and `compare.py` on the same machine; otherwise 22.0.2 as in rc1. Java tests stay on Oracle GraalVM 25. |
| D225 | Wheel tags. One `py3-none-<platform>` wheel per platform (five files), tag set in `python/setup.py`, built once per platform with the pinned cibuildwheel 2.23.4, then installed and smoke-tested on Python 3.10 to 3.14. `requires-python` is `>=3.10`. |
| D226 | Compatibility. README and CHANGELOG state the 0.1.x policy: directory format 2 is frozen for 0.1.x; rc1 directories open only after `upgrade()`, one way, originals kept in `pre-v2/`; older releases refuse upgraded directories; the Python API only grows; the C exports are internal. Each claim is checked against code and fixtures before it is worded. One check with the real rc1 wheel from PyPI is recorded. |
| D227 | Metadata. A short `python/README.md` with absolute links, LICENSE and NOTICE copied into `python/` by the workflow and listed in `license-files`, classifier Beta, classifiers for Python 3.10 to 3.14, Changelog and Documentation URLs, `nodusdb.__version__` from the installed metadata. |
| D228 | Publish job. A tested guard script fails the job unless the release tag is `v` plus the package version, the release's pre-release flag matches the version suffix, and the wheel set is exactly the expected five. `skip-existing: true` for re-runs. The API-token path is deleted. |
| D229 | Python 3.9. Keep `>=3.10` and yank rc1 once `0.1.0` is published, so 3.9 users get a clear "no matching distribution" instead of the alpha. |
| D230 | Installed-wheel checks. `wheel_smoke.py` makes one real call for each feature family and asserts LICENSE, NOTICE and `__version__`. A workflow step from the installed wheel with `JAVA_HOME` unset points `Shipping.s3` at `https://s3.amazonaws.com` with a fake key and requires the S3 authentication error; a certificate or handshake error fails the build. |
| D231 | Post-publish check. A `verify` job after `publish` installs `nodusdb==<version>` from the index on five platforms and two Pythons, with retries, and runs the smoke test. A manual trigger takes a version so it can be run against rc1 first. |
| D232 | Performance record. Toolchain measurements go in `bench/baseline/release-windows-dev.txt`, the README tables name the toolchain, the `native_ratio.py` limit becomes about twice the measured ratio per platform, and wheel sizes are recorded. |
| D233 | Outside voice skipped. |

## 4. The release flow

```
branch release/0.1.0rc2
  |  CI on every push: native (CE) -> py3-none wheel -> smoke on 3.10..3.14 -> HTTPS probe
  v
merge to main (owner approval)
  v
tag v0.1.0rc2 + GitHub release (pre-release)
  v
wheels.yml: build ----> guard (tag, flag, five files) ----> publish (reviewer approves) ----> verify (5 platforms)
  v
soak 1 to 2 days  --- problem? --> yank, fix, rc3
  v
commit: version string only, 0.1.0 + CHANGELOG cut     (same guard, same publish, same verify)
  v
yank rc1, test pip resolution on Python 3.9
```

## 5. Test plan

```
CODE PATHS                                              USER FLOWS
[+] python/setup.py (py3-none tag)                      [+] pip install nodusdb (fresh venv)
  |-- tag is py3-none-<platform>                          |-- five platforms, Python 3.10 to 3.14
  `-- repair keeps the tag                                |-- Python 3.9: clear error after rc1 is yanked
[+] native build (Community Edition)                      `-- unsupported platform: pip error, platforms on the page
  |-- builds on five platforms                          [+] first use after install
  `-- native_ratio gate at about 2x measured              |-- graph basics, durable reopen, directory shipping
[+] publish guard                                         |-- ship to S3 over HTTPS (probe, fake key)
  |-- tag equals v + version                              |-- changes, as_of, erase, follower, upgrade
  |-- pre-release flag matches suffix                     `-- rc1 directory upgrade (real rc1 wheel)
  |-- exactly five wheels
  `-- re-run after a partial upload
[+] metadata
  |-- __version__ equals installed version
  `-- LICENSE and NOTICE inside the wheel
[+] verify job after publish
```

New tests: a table-driven test of the guard script, wheel-name assertions, `__version__` and license assertions in
the smoke test, one call per feature family in the smoke test, the HTTPS probe, and the one-off rc1 upgrade check.

## 6. Failure modes

| Failure | Test | Handling | The user sees |
|---|---|---|---|
| Release tag does not match the version | guard test | job fails before upload | a red job with the two values |
| Upload stops halfway | none against PyPI | `skip-existing`, strict file check, re-run | partial files on PyPI, completed by the re-run |
| Repair drops the `py3-none` tag | wheel-name assertion | build fails | a red build |
| HTTPS trust missing on Linux or macOS | probe | build fails; document `ca_bundle` or find the system CA file | a red build before any release |
| Index not yet serving the files | none | retries for ten minutes | a red verify job only after that |
| Slower toolchain | `native_ratio`, `compare.py` | within-10% rule, tighter gate | a red CI step |
| Python 3.9 resolves to rc1 | local-index test | yank rc1 on release day | a clear "no matching distribution" |
| License missing from the wheel | smoke assertion | build fails | a red build |

No failure is untested, unhandled and silent.

## 7. Tasks

1. T1: shared native-toolchain action used by `ci.yml` and `wheels.yml`; toolchain measurements; README labels; tighter `native_ratio` limit.
2. T2: `py3-none` wheels and the install matrix on Python 3.10 to 3.14.
3. T3: package metadata, LICENSE and NOTICE.
4. T4: guard script and test, `skip-existing`, token path removed, `verify` job.
5. T5: broader smoke test and the HTTPS probe.
6. T6: compatibility policy and the real rc1 upgrade check.
7. T7: `0.1.0rc2` ceremony: version bump, CHANGELOG cut, tag, release, approval, verify, soak.
8. T8: `0.1.0`: version string only, release, verify, yank rc1, test 3.9 resolution.

T2, T3 and T4 and the workflow part of T1 share `wheels.yml` and go in sequence; T5 follows T3; T6 and the laptop
measurements are independent. T7 and T8 publish to a public index and are done only on the owner's instruction.

## 8. NOT in scope

- Wheels for Alpine (musl), Linux with glibc below 2.34, and Windows arm64 (`TODOS.md`).
- A source distribution that explains an unsupported platform (`TODOS.md`).
- Pinning third-party actions by commit SHA and enabling Dependabot (`TODOS.md`).
- A 1.0.0 version and a promise covering the C ABI.
- A permanent CI job for rc1 compatibility; the real-wheel check is run once.
- TestPyPI, the Production/Stable classifier, code signing or notarizing the native library, changing the
  cibuildwheel pin.

## 9. What already exists

`wheels.yml` builds the library on five platforms and tests each wheel; trusted publishing sits behind a
reviewer-gated `pypi` environment; `upgrade()` has a golden corpus and crash-resume tests; `native_ratio.py` and
`shipping_ratio.py` gate CI; CHANGELOG already holds every feature since rc1 under "Unreleased"; rc1 is an
immutable artifact that serves as a compatibility fixture.
