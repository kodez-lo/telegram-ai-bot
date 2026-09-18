# Oracle Cloud Always Free deployment

Recommended:
- Ubuntu 22.04 or 24.04
- VM.Standard.A1.Flex (Always Free eligible, subject to regional capacity)
- Public IPv4 enabled
- SSH key added during VM creation

After connecting through SSH:

```bash
git clone --single-branch --branch kodez-video-downloader-v4 https://github.com/kodez-lo/telegram-ai-bot.git
cd telegram-ai-bot
sudo bash oracle_install.sh
```

The installer:
- installs Python, FFmpeg and Git
- creates a virtualenv
- asks securely for BOT_TOKEN and OWNER_CLAIM_CODE
- stores secrets in /etc/kodez-v4.env with mode 600
- stores SQLite/download runtime data in /opt/kodez-v4/data
- creates and enables the kodez-v4 systemd service

Check the bot:
```bash
sudo systemctl status kodez-v4
sudo journalctl -u kodez-v4 -f
```

Update later:
```bash
cd ~/telegram-ai-bot
git pull
sudo bash oracle_update.sh
```
