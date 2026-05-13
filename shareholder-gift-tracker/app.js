const WATCHLIST_KEY = "shareholder-gift-tracker-watchlist-v1";
const SNAPSHOT_KEY = "shareholder-gift-tracker-snapshot-v1";
const AUTO_REFRESH_MS = 15 * 60 * 1000;
const API_BASE = "/api/shareholder-gifts";

const sampleCodes = ["2317", "2409", "3037", "9938", "2006", "1416"];

const codesInput = document.getElementById("codesInput");
const lookupBtn = document.getElementById("lookupBtn");
const saveWatchlistBtn = document.getElementById("saveWatchlistBtn");
const loadSampleBtn = document.getElementById("loadSampleBtn");
const clearBtn = document.getElementById("clearBtn");
const refreshBtn = document.getElementById("refreshBtn");
const updatedAtText = document.getElementById("updatedAtText");
const statusText = document.getElementById("statusText");
const resultsBody = document.getElementById("resultsBody");
const summaryStrip = document.getElementById("summaryStrip");
const summaryCardTemplate = document.getElementById("summaryCardTemplate");

let activeCodes = [];
let lastResponse = null;

function normalizeCodes(raw) {
  const matches = raw.match(/\d{3,6}/g) || [];
  return [...new Set(matches)];
}

function saveWatchlist(codes) {
  localStorage.setItem(WATCHLIST_KEY, codes.join("\n"));
}

function loadWatchlist() {
  return localStorage.getItem(WATCHLIST_KEY) || "";
}

function loadSnapshots() {
  try {
    return JSON.parse(localStorage.getItem(SNAPSHOT_KEY) || "{}");
  } catch {
    return {};
  }
}

function saveSnapshots(snapshot) {
  localStorage.setItem(SNAPSHOT_KEY, JSON.stringify(snapshot));
}

function formatDate(value) {
  if (!value) return "未提供";
  const date = new Date(`${value}T00:00:00`);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat("zh-TW", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(date);
}

function formatRange(start, end) {
  if (!start && !end) return "未提供";
  if (start && end) return `${formatDate(start)} - ${formatDate(end)}`;
  return formatDate(start || end);
}

function toSignature(item) {
  return [
    item.status,
    item.souvenirName,
    item.lastBuyDate,
    item.meetingDate,
    item.evoteStartDate,
    item.evoteEndDate,
    item.evotePickupStartDate,
    item.evotePickupEndDate,
    item.evotePickupRule,
  ].join("|");
}

function computeDiffs(results) {
  const previous = loadSnapshots();
  const next = {};

  const enriched = results.map((item) => {
    const signature = toSignature(item);
    const changed = previous[item.code] && previous[item.code] !== signature;
    next[item.code] = signature;
    return { ...item, changed };
  });

  saveSnapshots(next);
  return enriched;
}

function renderSummary(results) {
  summaryStrip.innerHTML = "";
  const published = results.filter((item) => item.status === "published").length;
  const unpublished = results.filter((item) => item.status === "unpublished").length;
  const changed = results.filter((item) => item.changed).length;
  const withVote = results.filter((item) => item.evoteStartDate || item.evoteEndDate).length;

  const cards = [
    ["追蹤代號", results.length, "這次查詢的股票代號數量"],
    ["已公告", published, "已抓到紀念品或會議資料"],
    ["未公告", unpublished, "目前仍建議留在 watchlist 持續追蹤"],
    ["有新變化", changed, "和你上次查詢相比有欄位變動"],
    ["含電子投票", withVote, "已補到電子投票起訖資訊"],
  ];

  cards.forEach(([label, value, note]) => {
    const node = summaryCardTemplate.content.firstElementChild.cloneNode(true);
    node.querySelector(".summary-label").textContent = label;
    node.querySelector(".summary-value").textContent = value;
    node.querySelector(".summary-note").textContent = note;
    summaryStrip.appendChild(node);
  });
}

function buildStatusBadge(item) {
  const badge = document.createElement("div");
  badge.className = `status-badge ${item.status}`;
  badge.textContent =
    item.status === "published"
      ? "已公告"
      : item.status === "partial"
        ? "部分資料"
        : "未公告";

  if (item.changed) {
    const flag = document.createElement("span");
    flag.className = "new-flag";
    flag.textContent = "NEW";
    badge.appendChild(flag);
  }

  return badge;
}

function renderSources(item) {
  if (!item.sources?.length) return "—";
  return item.sources
    .map((source) => `<a href="${source.url}" target="_blank" rel="noreferrer">${source.label}</a>`)
    .join("<br>");
}

function renderRows(results) {
  if (!results.length) {
    resultsBody.innerHTML = '<tr><td colspan="7" class="empty-cell">查無結果</td></tr>';
    return;
  }

  resultsBody.innerHTML = results
    .map((item) => {
      const souvenirText = item.souvenirName || "尚未公布";
      const dateBlock = `
        <div class="cell-stack">
          <span><strong>最後買進：</strong>${formatDate(item.lastBuyDate)}</span>
          <span><strong>股東會：</strong>${formatDate(item.meetingDate)}</span>
          <span><strong>地點：</strong>${item.meetingCity || "未提供"}</span>
        </div>
      `;
      const evoteBlock = `
        <div class="cell-stack">
          <span><strong>電子投票：</strong>${formatRange(item.evoteStartDate, item.evoteEndDate)}</span>
          <span><strong>電投領取期：</strong>${formatRange(item.evotePickupStartDate, item.evotePickupEndDate)}</span>
          <span><strong>領取資訊：</strong>${item.evotePickupRule || item.meetingDistributionRule || item.evotePickupPlace || "未補到更細資訊"}</span>
        </div>
      `;
      const agentBlock = `
        <div class="cell-stack">
          <span><strong>股代：</strong>${item.transferAgentName || item.transferAgentShort || "未提供"}</span>
          <span><strong>電話：</strong>${item.transferAgentPhone || "未提供"}</span>
          <span><strong>零股寄單：</strong>${item.oddLotMail || "未提供"}</span>
          <span><strong>備註：</strong>${item.notes || item.proxyPeriodText || "—"}</span>
        </div>
      `;

      return `
        <tr class="${item.changed ? "row-changed" : ""}">
          <td>
            <div class="stock-block">
              <strong>${item.code}</strong>
              <span>${item.companyName || "尚未比對到公司名稱"}</span>
            </div>
          </td>
          <td></td>
          <td>${souvenirText}</td>
          <td>${dateBlock}</td>
          <td>${evoteBlock}</td>
          <td>${agentBlock}</td>
          <td class="sources-cell">${renderSources(item)}</td>
        </tr>
      `;
    })
    .join("");

  [...resultsBody.querySelectorAll("tr")].forEach((row, index) => {
    const cell = row.children[1];
    if (cell) {
      cell.appendChild(buildStatusBadge(results[index]));
    }
  });
}

async function lookup(codes) {
  if (!codes.length) {
    statusText.textContent = "請先輸入至少一筆股票代號";
    return;
  }

  activeCodes = codes;
  lookupBtn.disabled = true;
  refreshBtn.disabled = true;
  statusText.textContent = "正在抓取最新資料...";

  try {
    const response = await fetch(`${API_BASE}/lookup?codes=${encodeURIComponent(codes.join(","))}`);
    const payload = await response.json();
    if (!response.ok || !payload.ok) {
      throw new Error(payload.error || "查詢失敗");
    }

    const enriched = computeDiffs(payload.results);
    lastResponse = { ...payload, results: enriched };
    updatedAtText.textContent = new Date().toLocaleString("zh-TW");
    statusText.textContent = `共查詢 ${enriched.length} 檔，資料來自 ${payload.sourceStats.wespai} 筆撿股讚、${payload.sourceStats.idealLabs} 筆股東禮簿、${payload.sourceStats.honsec} 筆宏遠股代資料。`;
    renderSummary(enriched);
    renderRows(enriched);
  } catch (error) {
    statusText.textContent = error.message || "查詢失敗";
    resultsBody.innerHTML = `<tr><td colspan="7" class="empty-cell">${statusText.textContent}</td></tr>`;
  } finally {
    lookupBtn.disabled = false;
    refreshBtn.disabled = false;
  }
}

lookupBtn.addEventListener("click", () => {
  const codes = normalizeCodes(codesInput.value);
  lookup(codes);
});

refreshBtn.addEventListener("click", () => {
  const codes = activeCodes.length ? activeCodes : normalizeCodes(codesInput.value);
  lookup(codes);
});

saveWatchlistBtn.addEventListener("click", () => {
  const codes = normalizeCodes(codesInput.value);
  saveWatchlist(codes);
  statusText.textContent = `已儲存 ${codes.length} 筆 watchlist。`;
});

loadSampleBtn.addEventListener("click", () => {
  codesInput.value = sampleCodes.join("\n");
});

clearBtn.addEventListener("click", () => {
  codesInput.value = "";
  activeCodes = [];
  statusText.textContent = "已清空輸入";
});

setInterval(() => {
  if (activeCodes.length) {
    lookup(activeCodes);
  }
}, AUTO_REFRESH_MS);

function bootstrap() {
  const saved = loadWatchlist();
  if (saved) {
    codesInput.value = saved;
    const codes = normalizeCodes(saved);
    if (codes.length) {
      lookup(codes);
    }
  }
}

bootstrap();
