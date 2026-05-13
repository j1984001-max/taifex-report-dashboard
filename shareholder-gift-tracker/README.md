# 股東會紀念品追蹤器

批次輸入股票代號後，整理今年度股東會紀念品、最後買進日、電子投票期間與可補到的領取資訊。

## 啟動方式

```bash
cd "/Users/wujohnson/Documents/New project/shareholder-gift-tracker"
python3 server.py
```

預設會開在：

```text
http://127.0.0.1:8765
```

如果只是想在這台電腦或同區網裝置開：

- 本機：`http://127.0.0.1:8765`
- 同網路其他裝置：`http://你的電腦區網IP:8765`

## 目前整合的來源

- 撿股讚：紀念品、開會日期、最後買進日、股代、零股寄單
- 股東禮簿：電子投票起訖、零股可否領取、股代名稱
- 宏遠股代：部分個股的電投領取期間與發放條件

## 這版可以做什麼

- 批次貼上多筆股票代號
- 儲存 watchlist 到瀏覽器 localStorage
- 每次查詢都抓最新頁面資料
- 和上次查詢結果比對，欄位有異動時標示 `NEW`
- 對於尚未公告的股票，保留在 watchlist 方便持續追蹤

## 限制

- 電投領取細節目前只有在整合來源有公開欄位時才能補出來，並不是每家股代網站都會直接提供。
- 資料來源都是公開網頁，若對方網站改版，可能需要微調解析邏輯。
- 實際領取規則仍應以公司通知書、公開資訊觀測站與股代公告為準。

## Render 上線

這個資料夾已包含：

- [Dockerfile](/Users/wujohnson/Documents/New project/shareholder-gift-tracker/Dockerfile)
- [render.yaml](/Users/wujohnson/Documents/New project/shareholder-gift-tracker/render.yaml)

如果要公開上線到 Render：

1. 把 `shareholder-gift-tracker/` 推到 GitHub repo。
2. 登入 Render。
3. 點 `New +` -> `Blueprint`。
4. 選你的 GitHub repo。
5. Render 會自動讀取 `render.yaml` 建立網站。
6. 部署完成後，直接用 Render 提供的網址開啟。
