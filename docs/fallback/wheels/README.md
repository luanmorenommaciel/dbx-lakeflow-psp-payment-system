# DQX wheel fallback

Prepare the exact DQX `0.16.0` wheel before the workshop:

```bash
./scripts/cache_dqx_wheel.sh
```

The script downloads the PyPI wheel named in `uv.lock`, verifies SHA-256
`71006c42cb89f4b8ad2333f19d4b51552040c57582ff9345300de4597b14c8b0`, and stores it under the ignored
`.workshop-evidence/fallback/wheels/` directory. The binary is third-party material and is not committed.

Use this only if normal resolution of the pinned pipeline dependency `databricks-labs-dqx==0.16.0` fails. An
available wheel is a recovery asset, not proof that DQX executed in Databricks. The instructor must still capture
pipeline update evidence before labeling the branch live.
