#!/bin/bash
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# GCE startup script for the benchmark VM:
#   1. Installs and verifies Google Cloud Ops Agent so
#      `agent.googleapis.com/memory/percent_used{state=used}` is exported.
#   2. Installs `poc-runner.service` polling `gs://$OPS/jobs/*.sh` and uploading
#      live logs and `.exit` codes to `gs://$OPS/results/`.
set -euo pipefail

MDS="http://169.254.169.254/computeMetadata/v1"
for _ in $(seq 1 30); do
  if curl -fsS -H Metadata-Flavor:Google "$MDS/instance/attributes/ops-bucket" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

OPS="$(curl -fsS -H Metadata-Flavor:Google "$MDS/instance/attributes/ops-bucket")"
SA_EMAIL="$(curl -fsS -H Metadata-Flavor:Google "$MDS/instance/service-accounts/default/email")"

# Install Google Cloud Ops Agent if not already active.
if ! systemctl is-active --quiet google-cloud-ops-agent; then
  TMP_OPS="$(mktemp -d)"
  curl -sS https://dl.google.com/cloudagents/add-google-cloud-ops-agent-repo.sh \
    -o "$TMP_OPS/add-google-cloud-ops-agent-repo.sh"
  bash "$TMP_OPS/add-google-cloud-ops-agent-repo.sh" --also-install
  rm -rf "$TMP_OPS"
  systemctl enable --now google-cloud-ops-agent
fi

id poc >/dev/null 2>&1 || useradd --create-home --shell /bin/bash poc
echo 'poc ALL=(ALL) NOPASSWD:ALL' >/etc/sudoers.d/poc
chmod 0440 /etc/sudoers.d/poc

rm -f /home/poc/.config/gcloud/gce
su - poc -c "GCE_METADATA_HOST=169.254.169.254 GCE_METADATA_ROOT=169.254.169.254 gcloud config set account '$SA_EMAIL' >/dev/null 2>&1 || true"

cat >/usr/local/bin/poc-runner <<EOF
#!/bin/bash
# Polls gs://${OPS}/jobs/ and runs each new job once.
set -uo pipefail
export GCE_METADATA_HOST=169.254.169.254
export GCE_METADATA_ROOT=169.254.169.254
export CLOUDSDK_CORE_ACCOUNT="${SA_EMAIL}"
OPS="${OPS}"
EOF
cat >>/usr/local/bin/poc-runner <<'EOF'
DONE="$HOME/.poc-runner-done"
mkdir -p "$HOME/jobs" && touch "$DONE"
rm -f "$HOME/.config/gcloud/gce"
while true; do
  for url in $(gcloud storage ls "gs://$OPS/jobs/*.sh" 2>/dev/null | sort); do
    name="$(basename "$url" .sh)"
    grep -qx "$name" "$DONE" && continue
    gcloud storage cp --quiet "$url" "$HOME/jobs/$name.sh" || continue
    echo "$name" >>"$DONE"
    log="$HOME/jobs/$name.log"
    ( while sleep 30; do
        gcloud storage cp --quiet "$log" "gs://$OPS/results/$name.log" >/dev/null 2>&1
      done ) & uploader=$!
    ( cd "$HOME" && OPS="$OPS" bash "$HOME/jobs/$name.sh" ) >"$log" 2>&1
    rc=$?
    kill "$uploader" 2>/dev/null || true
    gcloud storage cp --quiet "$log" "gs://$OPS/results/$name.log"
    echo "$rc" | gcloud storage cp --quiet - "gs://$OPS/results/$name.exit"
  done
  sleep 10
done
EOF
chmod 0755 /usr/local/bin/poc-runner

cat >/etc/systemd/system/poc-runner.service <<'EOF'
[Unit]
Description=POC benchmark job runner (polls GCS)
After=network-online.target google-guest-agent.service
Wants=network-online.target

[Service]
User=poc
ExecStart=/usr/local/bin/poc-runner
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
systemctl enable poc-runner.service
systemctl restart poc-runner.service
echo "poc-runner restarted; ops bucket gs://${OPS} account=${SA_EMAIL} ops_agent=$(systemctl is-active google-cloud-ops-agent)"
