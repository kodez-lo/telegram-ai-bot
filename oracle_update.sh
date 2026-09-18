#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/opt/kodez-v4"
BRANCH="kodez-video-downloader-v4"

if [ "${EUID}" -ne 0 ]; then
  echo "Run with sudo: sudo bash oracle_update.sh"
  exit 1
fi

git -C "${APP_DIR}/repo" fetch origin "${BRANCH}"
git -C "${APP_DIR}/repo" checkout "${BRANCH}"
git -C "${APP_DIR}/repo" reset --hard "origin/${BRANCH}"
"${APP_DIR}/venv/bin/pip" install -r "${APP_DIR}/repo/requirements.txt"
systemctl restart kodez-v4
systemctl --no-pager --full status kodez-v4 || true
