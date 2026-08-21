# Deterministic fallback data

Run `scripts/generate_fallback.sh` from the repository root to create the complete compressed JSON fallback
pack. The generated manifest must use seed `22082026` and report exactly 100,000 transactions.

Generated entity files are intentionally not hand-edited. `manifest.json` is the release contract checked by
the workshop tests.
