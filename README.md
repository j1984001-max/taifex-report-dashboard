# TAIFEX Report Dashboard

台指期貨 / 選擇權籌碼整理網站，會即時抓取臺灣期貨交易所資料並輸出 A 到 J 的完整報告。

## 本機啟動

```bash
cd "/Users/wujohnson/workspace/New-project"
python3 server.py
```

預設網址：

```text
http://127.0.0.1:8000
```

新增的股東會紀念品工具頁面：

```text
http://127.0.0.1:8000/shareholder-gift-tracker
```

## 寄送 Email（附 PDF）

需要先準備 Gmail App Password：

```bash
export GMAIL_USER="your@gmail.com"
export GMAIL_TO="target@example.com"
export GMAIL_APP_PASSWORD="your-app-password"
```

寄送最新營業日報告：

```bash
cd "/Users/wujohnson/workspace/New-project"
python3 send_report_email.py
```

寄送指定日期報告：

```bash
cd "/Users/wujohnson/workspace/New-project"
python3 send_report_email.py --date 2026/03/24
```

## 產生日快照

若要把某天報表先跑好並留存成快照，之後使用者查詢同一天時就不必重新抓期交所：

```bash
cd "/Users/wujohnson/workspace/New-project"
python3 generate_snapshot.py
```

指定日期：

```bash
cd "/Users/wujohnson/workspace/New-project"
python3 generate_snapshot.py --date 2026/03/24
```

快照會存到 `snapshots/`，包含：

- `YYYY-MM-DD.json`
- `YYYY-MM-DD.pdf`

## 每日推送

若要手動執行一次完整推送（先更新快照，再送 Telegram，再寄 Email + PDF）：

```bash
cd "/Users/wujohnson/workspace/New-project"
python3 send_daily_push.py
```

指定日期：

```bash
cd "/Users/wujohnson/workspace/New-project"
python3 send_daily_push.py --date 2026/03/24
```

此腳本完成後，若 `snapshots/` 有更新，還會自動 commit 並 push 到 GitHub，讓 Render 重新部署最新快照，之後網頁讀取會更快。

## GitHub Actions 雲端排程

若要讓網站在你的電腦關機時也能自動抓資料、送 Telegram、寄 Email + PDF，請在 GitHub repo 設定以下 Secrets：

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `GMAIL_USER`
- `GMAIL_TO`
- `GMAIL_APP_PASSWORD`

專案已包含：

- [.github/workflows/daily-push.yml](/Users/wujohnson/workspace/New-project/.github/workflows/daily-push.yml)

這個 workflow 會在平日台灣時間 15:00 自動開始：

1. 抓最新可用 TAIFEX 報告
2. 產生日快照
3. 發送 Telegram
4. 寄送 Email + PDF
5. 將快照 push 回 GitHub，讓 Render 重新部署

若 15:00 抓到的資料不完整，workflow 會自動等待 5 分鐘後重抓，最多再試 2 次；若最後仍失敗，會額外發一則 Telegram 告警，附上 GitHub Actions 執行連結。

## Docker 部署

```bash
docker build -t taifex-report .
docker run --rm -p 8000:8000 taifex-report
```

## Render 部署

專案已包含：

- [render.yaml](/Users/wujohnson/workspace/New-project/render.yaml)
- [Dockerfile](/Users/wujohnson/workspace/New-project/Dockerfile)
- [requirements.txt](/Users/wujohnson/workspace/New-project/requirements.txt)

Render 操作：

1. 把這個資料夾推到 GitHub。
2. 登入 Render。
3. 點 `New +` -> `Blueprint`。
4. 選你的 GitHub repo。
5. Render 會自動讀取 `render.yaml` 建立 `taifex-report-dashboard`。
6. 部署完成後，打開 Render 提供的網址即可跨網路使用。

Render 特性：

- 電腦關機時網站仍可使用
- 手機與外網可直接開啟
- `PORT` 由平台自動提供
- 不需要資料庫
- 上線後股東會紀念品工具可直接走同一個網址的 `/shareholder-gift-tracker`

## 其他雲端平台

此專案也可部署到 Railway、Fly.io、Cloud Run 等支援 Docker 的平台。
