        const reportRoot = document.getElementById("reportRoot");
        const reportDate = document.getElementById("reportDate");
        const errorBox = document.getElementById("errorBox");
        const errorText = document.getElementById("errorText");
        const refreshBtn = document.getElementById("refreshBtn");
        const datePicker = document.getElementById("datePicker");
        const shareLink = document.getElementById("shareLink");
        const shareBtn = document.getElementById("shareBtn");
        const prevDayBtn = document.getElementById("prevDayBtn");
        const nextDayBtn = document.getElementById("nextDayBtn");
        const mobileShareUrl = document.getElementById("mobileShareUrl");
        const reportGeneratedAt = document.getElementById("reportGeneratedAt");
        const reportSourceHint = document.getElementById("reportSourceHint");

        function escapeHtml(text) {
            return String(text ?? "")
                .replaceAll("&", "&amp;")
                .replaceAll("<", "&lt;")
                .replaceAll(">", "&gt;")
                .replaceAll('"', "&quot;")
                .replaceAll("'", "&#039;");
        }

        function formatNumber(value) {
            if (value === null || value === undefined) return "缺資料";
            if (typeof value === "string") return value;
            return new Intl.NumberFormat("zh-TW").format(value);
        }

        function formatSigned(value) {
            if (value === null || value === undefined) return "缺資料";
            const num = Number(value);
            if (!Number.isFinite(num)) return value;
            return `${num >= 0 ? "+" : ""}${formatNumber(num)}`;
        }

        function signedClass(value) {
            if (value === null || value === undefined) return "";
            return Number(value) >= 0 ? "num-pos" : "num-neg";
        }

        function sourceLinks(sources) {
            return sources.map((url) => `<a class="text-sky-600 dark:text-sky-300 underline break-all" href="${url}" target="_blank" rel="noreferrer">${url}</a>`).join("<br>");
        }

        function renderMeta(section) {
            return `
                <div class="section-meta-grid">
                    <div class="meta-pill"><span class="font-bold">日期：</span>${escapeHtml(section.date)}</div>
                    <div class="meta-pill"><span class="font-bold">單位：</span>${escapeHtml(section.unit)}</div>
                    <div class="meta-pill"><span class="font-bold">資料類型：</span>${section.title.startsWith("H.") || section.title.startsWith("I.") || section.title.startsWith("J.") ? "分析判讀" : "原始數字 / 摘要整理"}</div>
                </div>
            `;
        }

        function renderBullets(title, items) {
            return `
                <div class="mt-5">
                    <h4 class="font-black mb-2">${title}</h4>
                    <ul class="list-disc pl-6 space-y-1 text-sm leading-7">
                        ${items.map(item => `<li>${escapeHtml(item)}</li>`).join("")}
                    </ul>
                </div>
            `;
        }

        function sectionShell(title, inner) {
            return `
                <section class="section-card rounded-2xl bg-white dark:bg-slate-900 p-6 shadow-sm" data-section-title="${escapeHtml(title)}">
                    <h2 class="text-2xl font-black mb-5">${escapeHtml(title)}</h2>
                    ${inner}
                </section>
            `;
        }

        function normalizeErrorMessage(error) {
            const raw = String(error?.message || error || "").trim();
            if (!raw) return "資料暫時無法載入，請稍後再試。";
            if (raw.includes("HTTP 502")) return "上游資料來源暫時無法取得，請稍後再試。";
            if (raw.includes("Failed to fetch")) return "目前無法連線到本機報告服務，請確認 server 是否正常執行。";
            if (raw.includes("Expecting value") || raw.includes("Unexpected token")) return "伺服器回傳格式異常，請重新整理後再試。";
            return raw;
        }

        function applySectionAnchors() {
            const anchorMap = {
                "A. 三大法人總表詳細版": "section-a",
                "B. 三大法人期貨分契約詳細版": "section-b",
                "C. 大額交易人未沖銷詳細版": "section-c",
                "D. 三大法人選擇權分契約詳細版": "section-d",
                "E. 選擇權支撐壓力詳細版": "section-e",
                "F. OI 增減詳細版": "section-f",
                "G. Put/Call Ratio / 買賣權比": "section-g",
                "H. 綜合分析": "section-h",
                "I. Telegram 精簡版": "section-i",
                "J. Email 完整版": "section-j",
            };
            reportRoot.querySelectorAll("[data-section-title]").forEach((node) => {
                const key = node.getAttribute("data-section-title") || "";
                if (anchorMap[key]) node.id = anchorMap[key];
            });
        }

        function showLoadingState(message = "正在抓取最近一個營業日資料，請稍候。") {
            reportRoot.innerHTML = `
                <section class="loading-card">
                    <div class="flex items-center gap-2 mb-4">
                        <span class="loading-dot"></span>
                        <span class="loading-dot"></span>
                        <span class="loading-dot"></span>
                    </div>
                    <h2 class="text-xl font-black mb-3">資料載入中</h2>
                    <p class="text-sm leading-7 text-slate-600 dark:text-slate-300">${escapeHtml(message)}</p>
                </section>
            `;
        }

        function renderTableA(section) {
            const rows = section.rows.map((row) => `
                <tr>
                    <td>${escapeHtml(row.institution)}</td>
                    <td class="${signedClass(row.futuresTradeNetQty)}">${formatSigned(row.futuresTradeNetQty)}</td>
                    <td class="${signedClass(row.futuresOiNetQty)}">${formatSigned(row.futuresOiNetQty)}</td>
                    <td class="${signedClass(row.optionsTradeNetQty)}">${formatSigned(row.optionsTradeNetQty)}</td>
                    <td class="${signedClass(row.optionsOiNetQty)}">${formatSigned(row.optionsOiNetQty)}</td>
                    <td class="${signedClass(row.combinedOiNetQty)}">${formatSigned(row.combinedOiNetQty)}</td>
                </tr>
            `).join("");

            return sectionShell(section.title, `
                ${renderMeta(section)}
                <div class="table-wrap">
                    <table>
                        <thead>
                            <tr>
                                <th>法人</th>
                                <th>期貨交易淨額</th>
                                <th>期貨未平倉淨額</th>
                                <th>選擇權交易淨額</th>
                                <th>選擇權未平倉淨額</th>
                                <th>合計未平倉淨額</th>
                            </tr>
                        </thead>
                        <tbody>${rows}</tbody>
                    </table>
                </div>
                <div class="mt-5 text-sm leading-7">
                    <h4 class="font-black mb-2">表格解讀</h4>
                    <p>${escapeHtml(section.interpretation)}</p>
                </div>
                ${renderBullets("重點摘要", section.highlights)}
                <div class="mt-5 text-sm leading-7">
                    <h4 class="font-black mb-2">資料來源</h4>
                    ${sourceLinks(section.sources)}
                </div>
            `);
        }

        function renderContractTable(section, isOption = false) {
            const rows = section.rows.map((row) => `
                <tr>
                    <td>${escapeHtml(isOption ? row.institution : `${row.product} / ${row.institution}`)}</td>
                    ${isOption ? "" : `<td>${escapeHtml(row.product)}</td><td>${escapeHtml(row.institution)}</td>`}
                    <td>${formatNumber(row.tradeLongQty)}</td>
                    <td>${formatNumber(row.tradeLongAmount)}</td>
                    <td>${formatNumber(row.tradeShortQty)}</td>
                    <td>${formatNumber(row.tradeShortAmount)}</td>
                    <td class="${signedClass(row.tradeNetQty)}">${formatSigned(row.tradeNetQty)}</td>
                    <td>${formatNumber(row.oiLongQty)}</td>
                    <td>${formatNumber(row.oiLongAmount)}</td>
                    <td>${formatNumber(row.oiShortQty)}</td>
                    <td>${formatNumber(row.oiShortAmount)}</td>
                    <td class="${signedClass(row.oiNetQty)}">${formatSigned(row.oiNetQty)}</td>
                </tr>
            `).join("");

            const head = isOption
                ? `<tr><th>身份別</th><th>多方口數</th><th>多方金額</th><th>空方口數</th><th>空方金額</th><th>交易淨額</th><th>未平倉多方</th><th>未平倉多方金額</th><th>未平倉空方</th><th>未平倉空方金額</th><th>未平倉淨額</th></tr>`
                : `<tr><th>商品 / 身份別</th><th>商品</th><th>身份別</th><th>多方口數</th><th>多方金額</th><th>空方口數</th><th>空方金額</th><th>交易淨額</th><th>未平倉多方</th><th>未平倉多方金額</th><th>未平倉空方</th><th>未平倉空方金額</th><th>未平倉淨額</th></tr>`;

            return sectionShell(section.title, `
                ${renderMeta(section)}
                <div class="table-wrap">
                    <table>
                        <thead>${head}</thead>
                        <tbody>${rows}</tbody>
                    </table>
                </div>
                <div class="mt-5 text-sm leading-7">
                    <h4 class="font-black mb-2">表格解讀</h4>
                    <p>${escapeHtml(section.interpretation)}</p>
                </div>
                ${renderBullets("重點摘要", section.highlights)}
                <div class="mt-5 text-sm leading-7">
                    <h4 class="font-black mb-2">資料來源</h4>
                    ${sourceLinks(section.sources)}
                </div>
            `);
        }

        function renderTableC(section) {
            const row = section.row;
            return sectionShell(section.title, `
                ${renderMeta(section)}
                <div class="table-wrap">
                    <table>
                        <thead>
                            <tr>
                                <th>契約名稱</th>
                                <th>到期月份</th>
                                <th>前五大買方</th>
                                <th>前五大買方占比</th>
                                <th>前十大買方</th>
                                <th>前十大買方占比</th>
                                <th>前五大賣方</th>
                                <th>前五大賣方占比</th>
                                <th>前十大賣方</th>
                                <th>前十大賣方占比</th>
                                <th>全市場未沖銷</th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr>
                                <td>${escapeHtml(row.contractName)}</td>
                                <td>${escapeHtml(row.expiry)}</td>
                                <td>${formatNumber(row.longTop5Qty)}</td>
                                <td>${row.longTop5Pct.toFixed(1)}%</td>
                                <td>${formatNumber(row.longTop10Qty)}</td>
                                <td>${row.longTop10Pct.toFixed(1)}%</td>
                                <td>${formatNumber(row.shortTop5Qty)}</td>
                                <td>${row.shortTop5Pct.toFixed(1)}%</td>
                                <td>${formatNumber(row.shortTop10Qty)}</td>
                                <td>${row.shortTop10Pct.toFixed(1)}%</td>
                                <td>${formatNumber(row.marketOi)}</td>
                            </tr>
                        </tbody>
                    </table>
                </div>
                <div class="mt-5 text-sm leading-7">
                    <h4 class="font-black mb-2">表格解讀</h4>
                    <p>${escapeHtml(section.interpretation)}</p>
                </div>
                ${renderBullets("重點摘要", section.highlights)}
                <div class="mt-5 text-sm leading-7">
                    <h4 class="font-black mb-2">資料來源</h4>
                    ${sourceLinks(section.sources)}
                </div>
            `);
        }

        function renderTableE(section) {
            const sharedSupport = section.sharedSupport || { rangeLow: null, rangeHigh: null, clustered: false, items: [] };
            const sharedResistance = section.sharedResistance || { rangeLow: null, rangeHigh: null, clustered: false, items: [] };

            const supportItems = sharedSupport.items.length
                ? sharedSupport.items.map((item) =>
                `<li>${escapeHtml(item.label)} ${escapeHtml(item.series)}：${formatNumber(item.strike)} Put / ${formatNumber(item.oi)} 口</li>`
            ).join("")
                : "<li>缺資料</li>";
            const resistanceItems = sharedResistance.items.length
                ? sharedResistance.items.map((item) =>
                `<li>${escapeHtml(item.label)} ${escapeHtml(item.series)}：${formatNumber(item.strike)} Call / ${formatNumber(item.oi)} 口</li>`
            ).join("")
                : "<li>缺資料</li>";
            const renderChart = (chart, index) => {
                const maxCall = Math.max(...chart.rows.map((row) => row.callOi || 0), 1);
                const maxPut = Math.max(...chart.rows.map((row) => row.putOi || 0), 1);
                const rows = chart.rows.map((row) => {
                const callWidth = `${Math.max(((row.callOi || 0) / maxCall) * 100, row.callOi ? 3 : 0)}%`;
                const putWidth = `${Math.max(((row.putOi || 0) / maxPut) * 100, row.putOi ? 3 : 0)}%`;
                const callChangeClass = row.callChange >= 0 ? "sr-change-pos" : "sr-change-neg";
                const putChangeClass = row.putChange >= 0 ? "sr-change-pos" : "sr-change-neg";
                const strikeClass = row.strike === chart.atmStrike
                    ? "atm"
                    : row.strike === chart.floor.strike || row.strike === chart.defense.strike
                        ? "support"
                        : row.strike === chart.ceiling.strike
                            ? "resistance"
                            : "";
                const extraNote = row.strike === chart.defense.strike && row.strike !== chart.floor.strike
                    ? `<span class="sr-note">近防線</span>`
                    : "";
                return `
                    <div class="sr-row">
                        <div class="sr-side call">
                            <div class="sr-bar" style="width:${callWidth};"></div>
                            <div class="sr-label">
                                <span class="${callChangeClass}">(${formatSigned(row.callChange)})</span>
                                <span>${formatNumber(row.callOi)}</span>
                            </div>
                        </div>
                        <div class="sr-strike ${strikeClass}">
                            ${formatNumber(row.strike)}
                            ${extraNote}
                        </div>
                        <div class="sr-side put">
                            <div class="sr-bar" style="width:${putWidth};"></div>
                            <div class="sr-label">
                                <span>${formatNumber(row.putOi)}</span>
                                <span class="${putChangeClass}">(${formatSigned(row.putChange)})</span>
                            </div>
                        </div>
                    </div>
                `;
                }).join("");

                return `
                    <div class="sr-panel ${index === 0 ? "active" : ""}" data-sr-panel="${index}">
                    <div class="sr-chart-card">
                        <div class="mb-4">
                            <div class="flex flex-wrap items-center gap-3 mb-2">
                                <h4 class="text-lg font-black">${escapeHtml(chart.label)}</h4>
                                <span class="px-2 py-1 rounded-full bg-slate-100 dark:bg-slate-800 text-xs font-bold mono">${escapeHtml(chart.series)}</span>
                            </div>
                            <div class="text-sm leading-7">
                                <div><span class="font-bold">ATM：</span>${formatNumber(chart.atmStrike)}</div>
                                <div><span class="font-bold">主壓：</span>${formatNumber(chart.ceiling.strike)} Call / ${formatNumber(chart.ceiling.callOi)} 口</div>
                                <div><span class="font-bold">主撐：</span>${formatNumber(chart.floor.strike)} Put / ${formatNumber(chart.floor.putOi)} 口</div>
                                <div><span class="font-bold">近防線：</span>${formatNumber(chart.defense.strike)} Put / ${formatNumber(chart.defense.putOi)} 口</div>
                            </div>
                        </div>
                        <div class="mb-3 text-xs text-slate-500 dark:text-slate-400">
                            左側為 Call OI 與增減，右側為 Put OI 與增減；長條越長代表未平倉量越大。
                        </div>
                        <div class="sr-chart">${rows}</div>
                        ${renderBullets("本圖重點摘要", chart.highlights)}
                    </div>
                    </div>
                `;
            };
            return sectionShell(section.title, `
                ${renderMeta(section)}
                <div class="mb-4 text-sm leading-7">
                    <p><span class="font-bold">月選主契約：</span>${escapeHtml(section.monthlyContract)}</p>
                    <p><span class="font-bold">近月 TX 結算價：</span>${formatNumber(section.txSettlement)}</p>
                </div>
                <div class="sr-summary-grid">
                    ${section.charts.map((chart, index) => `
                        <button type="button" class="sr-summary-card ${index === 0 ? "active" : ""}" data-sr-target="${index}">
                            <div class="flex items-center gap-3 mb-3">
                                <h4>${escapeHtml(chart.label)}</h4>
                                <span class="px-2 py-1 rounded-full bg-slate-100 dark:bg-slate-800 text-xs font-bold mono">${escapeHtml(chart.series)}</span>
                            </div>
                            <div class="sr-summary-meta">
                                <div><span class="font-bold">ATM：</span>${formatNumber(chart.atmStrike)}</div>
                                <div><span class="font-bold">主壓：</span>${formatNumber(chart.ceiling.strike)} Call / ${formatNumber(chart.ceiling.callOi)} 口</div>
                                <div><span class="font-bold">主撐：</span>${formatNumber(chart.floor.strike)} Put / ${formatNumber(chart.floor.putOi)} 口</div>
                                <div><span class="font-bold">近防線：</span>${formatNumber(chart.defense.strike)} Put / ${formatNumber(chart.defense.putOi)} 口</div>
                            </div>
                        </button>
                    `).join("")}
                </div>
                <div class="sr-view">
                    <div class="sr-tab-bar">
                        ${section.charts.map((chart, index) => `
                            <button type="button" class="sr-tab-btn ${index === 0 ? "active" : ""}" data-sr-target="${index}">
                                ${escapeHtml(chart.label)} / ${escapeHtml(chart.series)}
                            </button>
                        `).join("")}
                    </div>
                    ${section.charts.map(renderChart).join("")}
                </div>
                <div class="grid grid-cols-1 lg:grid-cols-2 gap-5 mt-6">
                    <div class="shared-zone">
                        <h4 class="font-black mb-2">共同支撐</h4>
                        <p class="text-sm leading-7 mb-3">
                            觀察區：${formatNumber(sharedSupport.rangeLow)} 至 ${formatNumber(sharedSupport.rangeHigh)}
                            ，${sharedSupport.items.length ? (sharedSupport.clustered ? "三張圖支撐相對集中。" : "三張圖支撐分散，需分開看。") : "缺資料。"}
                        </p>
                        <ul class="list-disc pl-6 space-y-1 text-sm leading-7">
                            ${supportItems}
                        </ul>
                    </div>
                    <div class="shared-zone">
                        <h4 class="font-black mb-2">共同壓力</h4>
                        <p class="text-sm leading-7 mb-3">
                            觀察區：${formatNumber(sharedResistance.rangeLow)} 至 ${formatNumber(sharedResistance.rangeHigh)}
                            ，${sharedResistance.items.length ? (sharedResistance.clustered ? "三張圖壓力相對集中。" : "三張圖壓力分散，需分開看。") : "缺資料。"}
                        </p>
                        <ul class="list-disc pl-6 space-y-1 text-sm leading-7">
                            ${resistanceItems}
                        </ul>
                    </div>
                </div>
                <div class="mt-5 text-sm leading-7">
                    <h4 class="font-black mb-2">表格解讀</h4>
                    <p>${escapeHtml(section.interpretation)}</p>
                </div>
                ${renderBullets("重點摘要", section.highlights)}
                <div class="mt-5 text-sm leading-7">
                    <h4 class="font-black mb-2">資料來源</h4>
                    ${sourceLinks(section.sources)}
                </div>
            `);
        }

        function activateSrChart(root, targetIndex) {
            root.querySelectorAll("[data-sr-target]").forEach((node) => {
                node.classList.toggle("active", node.getAttribute("data-sr-target") === String(targetIndex));
            });
            root.querySelectorAll("[data-sr-panel]").forEach((node) => {
                node.classList.toggle("active", node.getAttribute("data-sr-panel") === String(targetIndex));
            });
        }

        function renderTableF(section) {
            const rows = section.rows.map((row) => `
                <tr>
                    <td>${formatNumber(row.strike)}</td>
                    <td>${formatNumber(row.callOi)}</td>
                    <td class="${signedClass(row.callChange)}">${formatSigned(row.callChange)}</td>
                    <td>${formatNumber(row.putOi)}</td>
                    <td class="${signedClass(row.putChange)}">${formatSigned(row.putChange)}</td>
                </tr>
            `).join("");
            return sectionShell(section.title, `
                ${renderMeta(section)}
                <div class="table-wrap mb-5">
                    <table>
                        <thead>
                            <tr>
                                <th>日期</th>
                                <th>當日未平倉量</th>
                                <th>前一日</th>
                                <th>前一日未平倉量</th>
                                <th>整體增減</th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr>
                                <td>${escapeHtml(section.overall.date)}</td>
                                <td>${formatNumber(section.overall.currentOi)}</td>
                                <td>${escapeHtml(section.overall.previousDate)}</td>
                                <td>${formatNumber(section.overall.previousOi)}</td>
                                <td class="${signedClass(section.overall.change)}">${formatSigned(section.overall.change)}</td>
                            </tr>
                        </tbody>
                    </table>
                </div>
                <div class="table-wrap">
                    <table>
                        <thead>
                            <tr>
                                <th>履約價</th>
                                <th>Call OI</th>
                                <th>Call 增減</th>
                                <th>Put OI</th>
                                <th>Put 增減</th>
                            </tr>
                        </thead>
                        <tbody>${rows}</tbody>
                    </table>
                </div>
                <div class="mt-5 text-sm leading-7">
                    <h4 class="font-black mb-2">表格解讀</h4>
                    <p>${escapeHtml(section.interpretation)}</p>
                </div>
                ${renderBullets("重點摘要", section.highlights)}
                <div class="mt-5 text-sm leading-7">
                    <h4 class="font-black mb-2">資料來源</h4>
                    ${sourceLinks(section.sources)}
                </div>
            `);
        }

        function renderTableG(section) {
            const trendText = section.highlights.find((item) => item.startsWith("五日趨勢："));
            const rows = section.rows.length
                ? section.rows.map((row) => `
                <tr>
                    <td>${escapeHtml(row.date)}</td>
                    <td>${formatNumber(row.putVolume)}</td>
                    <td>${formatNumber(row.callVolume)}</td>
                    <td>${row.volumeRatio.toFixed(2)}%</td>
                    <td>${formatNumber(row.putOi)}</td>
                    <td>${formatNumber(row.callOi)}</td>
                    <td>${row.oiRatio.toFixed(2)}%</td>
                </tr>
            `).join("")
                : `<tr><td colspan="7" class="text-left">缺資料</td></tr>`;
            return sectionShell(section.title, `
                ${renderMeta(section)}
                ${trendText ? `<div class="mb-4 text-sm leading-7"><span class="font-black">五日趨勢：</span>${escapeHtml(trendText.replace("五日趨勢：", ""))}</div>` : ""}
                <div class="table-wrap">
                    <table>
                        <thead>
                            <tr>
                                <th>日期</th>
                                <th>賣權成交量</th>
                                <th>買權成交量</th>
                                <th>成交量比率</th>
                                <th>賣權未平倉量</th>
                                <th>買權未平倉量</th>
                                <th>未平倉量比率</th>
                            </tr>
                        </thead>
                        <tbody>${rows}</tbody>
                    </table>
                </div>
                <div class="mt-5 text-sm leading-7">
                    <h4 class="font-black mb-2">表格解讀</h4>
                    <p>${escapeHtml(section.interpretation)}</p>
                </div>
                ${renderBullets("重點摘要", section.highlights)}
                <div class="mt-5 text-sm leading-7">
                    <h4 class="font-black mb-2">資料來源</h4>
                    ${sourceLinks(section.sources)}
                </div>
            `);
        }

        function renderAnalysis(report) {
            const a = report.analysis;
            const sections = a.sections.map((section, index) => `
                <div class="mb-5">
                    <h4 class="font-black mb-2">${index + 2}. ${escapeHtml(section.title)}</h4>
                    <p class="text-sm leading-7">${escapeHtml(section.body)}</p>
                </div>
            `).join("");
            return sectionShell("H. 綜合分析", `
                <div class="text-sm leading-7">
                    <h4 class="font-black mb-2">1. 今日重點摘要</h4>
                    <ul class="list-disc pl-6 space-y-1">
                        ${a.highlights.map(item => `<li>${escapeHtml(item)}</li>`).join("")}
                    </ul>
                </div>
                <div class="mt-5">${sections}</div>
                <div class="mt-5">
                    <h4 class="font-black mb-2">9. 策略建議（保守版 / 中性版 / 積極版）</h4>
                    <ul class="list-disc pl-6 space-y-1 text-sm leading-7">
                        <li>保守版：${escapeHtml(a.strategies.conservative)}</li>
                        <li>中性版：${escapeHtml(a.strategies.neutral)}</li>
                        <li>積極版：${escapeHtml(a.strategies.aggressive)}</li>
                    </ul>
                </div>
                <div class="mt-5">
                    <h4 class="font-black mb-2">10. 一句話結論</h4>
                    <p class="text-sm leading-7">${escapeHtml(a.conclusion)}</p>
                </div>
            `);
        }

        function renderCopySection(title, text) {
            return sectionShell(title, `
                <div class="text-sm mb-4 text-slate-500 dark:text-slate-400">可直接複製使用</div>
                <div class="copy-block text-sm leading-7 p-4 rounded-xl bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-800">${escapeHtml(text)}</div>
            `);
        }

        function renderReport(report) {
            const tables = report.tables;
            reportDate.textContent = `資料日期：${report.meta.date}`;
            reportGeneratedAt.textContent = `最後更新：${report.meta.generatedAt || "缺資料"}`;
            reportSourceHint.textContent = `來源：期交所官方頁面整理${report.meta.pcRatioMethod ? ` / P-C Ratio：${report.meta.pcRatioMethod}` : ""}`;
            shareLink.value = report.meta.reportUrl || window.location.href;
            mobileShareUrl.textContent = report.meta.reportUrl || window.location.href;
            if (window.history && window.history.replaceState) {
                const nextUrl = new URL(report.meta.reportUrl || window.location.href, window.location.origin);
                window.history.replaceState({}, "", `${nextUrl.pathname}${nextUrl.search}`);
            }
            reportRoot.innerHTML = [
                renderTableA(tables.A),
                renderContractTable(tables.B, false),
                renderTableC(tables.C),
                renderContractTable(tables.D, true),
                renderTableE(tables.E),
                renderTableF(tables.F),
                renderTableG(tables.G),
                renderAnalysis(report),
                renderCopySection("I. Telegram 精簡版", report.telegram),
                renderCopySection("J. Email 完整版", report.email),
            ].join("");

            applySectionAnchors();
            const eSection = document.getElementById("section-e");
            if (eSection) {
                eSection.querySelectorAll("[data-sr-target]").forEach((node) => {
                    node.addEventListener("click", () => activateSrChart(eSection, node.getAttribute("data-sr-target")));
                });
            }
        }

        function currentSelectedDate() {
            return datePicker.value || "";
        }

        function shiftDate(days) {
            const base = currentSelectedDate() || new Date().toISOString().slice(0, 10);
            const dt = new Date(`${base}T00:00:00`);
            dt.setDate(dt.getDate() + days);
            const y = dt.getFullYear();
            const m = String(dt.getMonth() + 1).padStart(2, "0");
            const d = String(dt.getDate()).padStart(2, "0");
            datePicker.value = `${y}-${m}-${d}`;
            loadReport();
        }

        function syncDatePicker(reportDateText) {
            if (!reportDateText) return;
            const [y, m, d] = reportDateText.split("/");
            datePicker.value = `${y}-${m}-${d}`;
        }

        function initializeDatePickerFromUrl() {
            const date = new URLSearchParams(window.location.search).get("date");
            if (!date) return;
            const normalized = date.replaceAll("/", "-");
            if (/^\d{4}-\d{2}-\d{2}$/.test(normalized)) {
                datePicker.value = normalized;
            }
        }

        async function loadReport() {
            refreshBtn.disabled = true;
            errorBox.classList.add("hidden");
            try {
                const date = currentSelectedDate();
                showLoadingState(date ? `正在抓取 ${date.replaceAll("-", "/")} 的歷史資料，請稍候。` : "正在抓取最近一個營業日資料，請稍候。");
                const query = date ? `?date=${encodeURIComponent(date.replaceAll("-", "/"))}` : "";
                const response = await fetch(`/api/report${query}`, {
                    cache: query.includes("refresh=1") ? "no-store" : "default",
                });
                const data = await response.json();
                if (!response.ok) throw new Error(data.error || `HTTP ${response.status}`);
                syncDatePicker(data.meta.date);
                renderReport(data);
            } catch (error) {
                errorBox.classList.remove("hidden");
                errorText.textContent = normalizeErrorMessage(error);
            } finally {
                refreshBtn.disabled = false;
            }
        }

        document.getElementById("themeBtn").addEventListener("click", () => {
            const nextDark = !document.documentElement.classList.contains("dark");
            document.documentElement.classList.toggle("dark", nextDark);
            document.documentElement.classList.toggle("light", !nextDark);
            localStorage.setItem("dashboard-theme", nextDark ? "dark" : "light");
        });
        refreshBtn.addEventListener("click", loadReport);
        datePicker.addEventListener("change", loadReport);
        prevDayBtn.addEventListener("click", () => shiftDate(-1));
        nextDayBtn.addEventListener("click", () => shiftDate(1));
        shareBtn.addEventListener("click", async () => {
            const url = shareLink.value || window.location.href;
            if (navigator.share) {
                try {
                    await navigator.share({ title: "台指期權籌碼報告", url });
                    return;
                } catch (_) {}
            }
            try {
                await navigator.clipboard.writeText(url);
                if (mobileShareUrl) mobileShareUrl.textContent = url;
                shareBtn.textContent = "已複製";
                setTimeout(() => { shareBtn.textContent = "分享連結"; }, 1200);
            } catch (_) {}
        });
        initializeDatePickerFromUrl();
        loadReport();
