#!/bin/bash
# EC2에서 실행되는 배포 스크립트
# GitHub Actions가 SSH로 이 스크립트를 호출함
set -e

APP_DIR="/home/ubuntu/workcafe"
VENV="$APP_DIR/venv"
SERVICE="workcafe"

echo "=== [1/4] 코드 pull ==="
cd "$APP_DIR"
git fetch origin main
git reset --hard origin/main

echo "=== [2/4] 의존성 설치 ==="
"$VENV/bin/pip" install -q --upgrade pip
"$VENV/bin/pip" install -q -r requirements.txt

echo "=== [3/4] 서비스 재시작 ==="
sudo systemctl daemon-reload
sudo systemctl restart "$SERVICE"

echo "=== [4/4] 상태 확인 ==="
sleep 2
sudo systemctl is-active --quiet "$SERVICE" && echo "서비스 정상 실행 중" || {
    echo "서비스 시작 실패. 로그:"
    sudo journalctl -u "$SERVICE" -n 30 --no-pager
    exit 1
}
