# Same-Day WantGoo Reconciliation

## Delivery Stages

- The existing 15:00 official-source fast push remains independent of WantGoo.
- The new reconciliation window is 17:00-18:59 Asia/Taipei on market business days.
- The Cloudflare configuration adds a 17:00 primary trigger and ten-minute retries until 18:50. Deploying that configuration requires an active Cloudflare authorization.
- GitHub Actions has the equivalent backup schedule. It is not a punctuality guarantee. Delayed automatic jobs outside the reconciliation window exit without fetching or notifying.

## Data Scope

The collector reads the public current tables using a standard Playwright browser. It does not use private cookies, CAPTCHA solvers, fingerprint spoofing, or a paid historical-date selector.

Reference fields include near-month futures concentration, aggregate weekly/monthly option concentration, institutional futures positions, and institutional option positions/amounts. Source dates, monthly contract codes, call/put identities, institutions, position rankings and long-minus-short arithmetic are checked before changes are allowed.

An individual weekly expiry is not interchangeable with the exchange's aggregate weekly contract. Weekly futures, market high/low prices, and other fields that have no matching reference remain official-source data. Cumulative changes remain derived from the explicitly displayed baseline date; they are not presented as directly published WantGoo figures. An unpublished daily change is excluded from comparison, not replaced by zero.

## Published State

`snapshots/YYYY-MM-DD.wantgoo.json` records the reference, fingerprint, checked date, differences and independent Telegram delivery receipts. The first pre-correction report is retained when corrections are needed. It is published together with the normal report and corrected PDF.

`meta.wantgooReconciliation` identifies matched, corrected or pending data in the webpage and screenshots. A complete same-day reference is required; a page shell, stale date, challenge page, missing institution or incomplete market scope cannot be reported as a match.

Late official full-report builds reapply a previously verified same-date reference so that evening corrections cannot be silently overwritten. The existing high-low/weekly/detail/email delivery markers are not reset by reconciliation.

An unchanged result sends one short confirmation. A correction sends a short difference summary plus the corrected high-low, summary and weekly images. Each item has its own positive Telegram message ID, so retries resume unsent items. Source-unavailable checks preserve official figures and stay pending; the final retry can send a single incomplete-reference notice.

## Validation

```sh
pip install -r requirements-test.txt
python3 -m unittest discover -s tests -v
node --test tests/test_reference_trigger.mjs
python3 wantgoo_reconcile.py --date YYYY/MM/DD --dry-run
```

A dry run never publishes, changes delivery state or sends messages. A successful workflow execution can still contain a `pending` source result; consult `wantgoo-artifacts/result.json`, not only the GitHub conclusion.

At implementation time, direct requests were challenged and the initial GitHub-hosted browser probe returned HTTP403. Cloudflare CLI authorization had also expired. Do not describe live WantGoo alignment or the Cloudflare trigger as verified until public source access and Cloudflare deployment have both been confirmed. The existing official fast push is unaffected by those limitations.
