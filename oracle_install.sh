#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/opt/kodez-v4"
REPO_URL="https://github.com/kodez-lo/telegram-ai-bot.git"
BRANCH="kodez-video-downloader-v4"
ENV_FILE="/etc/kodez-v4.env"
SERVICE_FILE="/etc/systemd/system/kodez-v4.service"

if [ "${EUID}" -ne 0 ]; then
  echo "Run with sudo: sudo bash oracle_install.sh"
  exit 1
fi

echo "==> Installing system packages..."
apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y \
  python3 python3-venv python3-pip ffmpeg git ca-certificates curl

echo "==> Preparing application..."
mkdir -p "${APP_DIR}"
if [ -d "${APP_DIR}/repo/.git" ]; then
  git -C "${APP_DIR}/repo" fetch origin "${BRANCH}"
  git -C "${APP_DIR}/repo" checkout "${BRANCH}"
  git -C "${APP_DIR}/repo" reset --hard "origin/${BRANCH}"
else
  rm -rf "${APP_DIR}/repo"
  git clone --single-branch --branch "${BRANCH}" "${REPO_URL}" "${APP_DIR}/repo"
fi

mkdir -p "${APP_DIR}/data"
python3 -m venv "${APP_DIR}/venv"
"${APP_DIR}/venv/bin/pip" install --upgrade pip
"${APP_DIR}/venv/bin/pip" install -r "${APP_DIR}/repo/requirements.txt"

echo
echo "Enter your Telegram bot token. It will be stored only on this VM."
read -rsp "BOT_TOKEN: " BOT_TOKEN
echo

echo "Enter the private owner claim code you want to use."
read -rsp "OWNER_CLAIM_CODE: " OWNER_CLAIM_CODE
echo

if [ -z "${BOT_TOKEN}" ]; then
  echo "BOT_TOKEN cannot be empty."
  exit 1
fi

if [ -z "${OWNER_CLAIM_CODE}" ]; then
  echo "OWNER_CLAIM_CODE cannot be empty."
  exit 1
fi

cat > "${ENV_FILE}" <<EOF
BOT_TOKEN=${BOT_TOKEN}
OWNER_CLAIM_CODE=${OWNER_CLAIM_CODE}
DATA_DIR=${APP_DIR}/data
EOF
chmod 600 "${ENV_FILE}"

cat > "${SERVICE_FILE}" <<EOF
[Unit]
Description=Kodez Videos Downloader V4 Telegram Bot
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=${APP_DIR}/repo
EnvironmentFile=${ENV_FILE}
ExecStart=${APP_DIR}/venv/bin/python ${APP_DIR}/repo/kodez_videos_downloader_v4.py
Restart=always
RestartSec=5
User=root

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable kodez-v4
systemctl restart kodez-v4

sleep 3
echo
echo "==> Service status:"
systemctl --no-pager --full status kodez-v4 || true
echo
echo "Useful commands:"
echo "  sudo systemctl status kodez-v4"
echo "  sudo journalctl -u kodez-v4 -f"
echo "  sudo systemctl restart kodez-v4"
