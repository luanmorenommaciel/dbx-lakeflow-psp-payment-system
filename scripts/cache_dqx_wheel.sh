#!/usr/bin/env bash
set -euo pipefail

wheel_name="databricks_labs_dqx-0.16.0-py3-none-any.whl"
wheel_url="https://files.pythonhosted.org/packages/97/81/298dfc0b34e69d6fbbd9fc3630b944d9a1e1ce5682f7eaa172603885c33f/$wheel_name"
expected_sha256="71006c42cb89f4b8ad2333f19d4b51552040c57582ff9345300de4597b14c8b0"
destination_dir=".workshop-evidence/fallback/wheels"
destination="$destination_dir/$wheel_name"
temporary="$destination.part"

mkdir -p "$destination_dir"
trap 'rm -f "$temporary"' EXIT
curl --fail --location --silent --show-error "$wheel_url" --output "$temporary"
actual_sha256="$(shasum -a 256 "$temporary" | awk '{print $1}')"

if [[ "$actual_sha256" != "$expected_sha256" ]]; then
  echo "DQX_WHEEL=FAIL reason=checksum expected=$expected_sha256 actual=$actual_sha256"
  exit 1
fi

mv "$temporary" "$destination"
trap - EXIT
echo "DQX_WHEEL=READY path=$destination sha256=$actual_sha256"
