(function () {
    "use strict";

    let analyticsWindowBars = [];
    let analyticsRootContainer = null;

    function ensureAnalyticsContainer() {
        let container = document.getElementById("analysisContainer");
        if (container) {
            analyticsRootContainer = container;
            return container;
        }

        container = document.createElement("div");
        container.id = "analysisContainer";
        container.style.marginBottom = "20px";
        container.style.display = "flex";
        container.style.flexDirection = "column";
        container.style.gap = "14px";

        const metadata = document.getElementById("metadataContainer");
        const overallStats = document.getElementById("overallStatsContainer");
        const timeSlider = document.getElementById("timeSlider");

        if (metadata && metadata.parentNode) {
            metadata.parentNode.insertBefore(container, metadata.nextSibling);
        } else if (overallStats && overallStats.parentNode) {
            overallStats.parentNode.insertBefore(container, overallStats);
        } else if (timeSlider && timeSlider.parentNode && timeSlider.parentNode.parentNode) {
            timeSlider.parentNode.parentNode.insertBefore(container, timeSlider.parentNode);
        } else {
            document.body.appendChild(container);
        }

        analyticsRootContainer = container;
        return container;
    }

    function createAnalyticsCard(title, accent) {
        const card = document.createElement("div");
        card.style.padding = "14px 16px";
        card.style.backgroundColor = "rgba(22, 27, 34, 0.55)";
        card.style.border = "1px solid #30363d";
        card.style.borderLeft = `4px solid ${accent || "#58a6ff"}`;
        card.style.borderRadius = "8px";
        card.style.boxShadow = "0 2px 8px rgba(0,0,0,0.25)";

        const header = document.createElement("div");
        header.textContent = title;
        header.style.fontSize = "0.75rem";
        header.style.color = "#8b949e";
        header.style.fontWeight = "bold";
        header.style.textTransform = "uppercase";
        header.style.letterSpacing = "1px";
        header.style.marginBottom = "10px";

        const body = document.createElement("div");

        card.appendChild(header);
        card.appendChild(body);

        return { card, body };
    }

    function formatBytes(bytes) {
        const n = Number(bytes) || 0;
        const abs = Math.abs(n);
        const units = ["B", "KB", "MB", "GB", "TB"];

        let value = abs;
        let idx = 0;
        while (value >= 1024 && idx < units.length - 1) {
            value /= 1024;
            idx++;
        }

        let digits = 0;
        if (idx > 0) {
            if (value < 10) digits = 2;
            else if (value < 100) digits = 1;
        }

        return `${n < 0 ? "-" : ""}${value.toFixed(digits)} ${units[idx]}`;
    }

    function formatTimeValue(seconds) {
        const s = Number(seconds) || 0;
        const abs = Math.abs(s);

        if (abs < 1e-9) return `${s.toFixed(3)} s`;
        if (abs < 1e-6) return `${(s * 1e9).toFixed(1)} ns`;
        if (abs < 1e-3) return `${(s * 1e6).toFixed(1)} µs`;
        if (abs < 1) return `${(s * 1e3).toFixed(2)} ms`;
        if (abs < 60) return `${s.toFixed(3)} s`;
        return `${(s / 60).toFixed(2)} min`;
    }

    function formatPercent(frac) {
        return `${((Number(frac) || 0) * 100).toFixed(1)}%`;
    }

    function severityColor(sev) {
        if (sev === "critical") return "#f85149";
        if (sev === "warning") return "#d29922";
        return "#58a6ff";
    }

    function createMetricGrid(items) {
        const grid = document.createElement("div");
        grid.style.display = "grid";
        grid.style.gridTemplateColumns = "repeat(auto-fit, minmax(180px, 1fr))";
        grid.style.gap = "8px";

        items.forEach(item => {
            const box = document.createElement("div");
            box.style.padding = "10px 12px";
            box.style.backgroundColor = "rgba(13, 17, 23, 0.75)";
            box.style.border = "1px solid #30363d";
            box.style.borderRadius = "6px";

            const k = document.createElement("div");
            k.textContent = item.label;
            k.style.fontSize = "0.72rem";
            k.style.color = "#8b949e";
            k.style.marginBottom = "6px";

            const v = document.createElement("div");
            v.textContent = item.value;
            v.style.fontSize = "0.92rem";
            v.style.color = "#c9d1d9";
            v.style.fontFamily = "'Fira Code', monospace";
            v.style.fontWeight = "600";

            box.appendChild(k);
            box.appendChild(v);
            grid.appendChild(box);
        });

        return grid;
    }

    function createClickablePill(text, color, onClick) {
        const pill = document.createElement("span");
        pill.textContent = text;
        pill.style.display = "inline-block";
        pill.style.padding = "3px 8px";
        pill.style.borderRadius = "999px";
        pill.style.border = `1px solid ${color || "#58a6ff"}`;
        pill.style.color = color || "#58a6ff";
        pill.style.fontSize = "0.75rem";
        pill.style.marginRight = "6px";
        pill.style.marginBottom = "6px";
        pill.style.fontFamily = "'Fira Code', monospace";

        if (onClick) {
            pill.style.cursor = "pointer";
            pill.style.pointerEvents = "auto";
            pill.addEventListener("click", onClick);
        }

        return pill;
    }

    function focusRankFromAnalytics(rankId) {
        if (typeof rankMap === "undefined") return;

        const sData = rankMap.get(rankId);
        if (!sData) return;

        const targetPosition = sData.localPos.clone().add(sData.nodeGroup.position);
        const mockObj = {
            userData: {
                rank: rankId,
                host: sData.nodeGroup.userData.host,
                isCore: true
            },
            worldPos: targetPosition,
            name: `Rank ${rankId}`
        };

        if (typeof setSelectedObject === "function") {
            setSelectedObject(mockObj);
        }
        if (typeof flyCameraTo === "function") {
            flyCameraTo(targetPosition);
        }
    }

    function focusLinkFromAnalytics(sender, receiver) {
        if (typeof rankMap === "undefined") return;

        const sData = rankMap.get(sender);
        const rData = rankMap.get(receiver);
        if (!sData || !rData) return;

        const sPos = sData.localPos.clone().add(sData.nodeGroup.position);
        const rPos = rData.localPos.clone().add(rData.nodeGroup.position);
        const mid = sPos.clone().lerp(rPos, 0.5);

        if (typeof setSelectedObject === "function") {
            setSelectedObject(null);
        }
        if (typeof flyCameraTo === "function") {
            flyCameraTo(mid);
        }
    }

    function createAnalyticsTable(headers) {
        const table = document.createElement("table");
        table.style.width = "100%";
        table.style.borderCollapse = "collapse";
        table.style.fontSize = "0.82rem";

        const thead = document.createElement("thead");
        const tr = document.createElement("tr");

        headers.forEach(h => {
            const th = document.createElement("th");
            th.textContent = h;
            th.style.textAlign = "left";
            th.style.padding = "6px 8px";
            th.style.color = "#8b949e";
            th.style.fontWeight = "600";
            th.style.borderBottom = "1px solid #30363d";
            tr.appendChild(th);
        });

        thead.appendChild(tr);
        table.appendChild(thead);

        const tbody = document.createElement("tbody");
        table.appendChild(tbody);

        return { table, tbody };
    }

    function renderAnalysisSummaryCard(container, analysis) {
        const summary = analysis.summary || {};
        const { card, body } = createAnalyticsCard("Trace Analysis Summary", "#58a6ff");

        body.appendChild(createMetricGrid([
            { label: "Total Events", value: String(summary.total_events || 0) },
            { label: "Transfers", value: String(summary.canonical_transfer_events || 0) },
            { label: "Transfer Bytes", value: formatBytes(summary.canonical_transfer_bytes || 0) },
            { label: "Completion Events", value: String(summary.completion_events || 0) },
            { label: "Barrier Events", value: String(summary.barrier_events || 0) },
            { label: "Estimated Runtime", value: formatTimeValue(summary.estimated_runtime || 0) },
            { label: "Pair Density", value: (summary.pair_density || 0).toFixed(3) },
            { label: "Avg Out Peers", value: (summary.avg_out_peers || 0).toFixed(2) },
            { label: "Avg In Peers", value: (summary.avg_in_peers || 0).toFixed(2) }
        ]));

        container.appendChild(card);
    }

    function renderAnalysisPatternsCard(container, analysis) {
        const patterns = analysis.patterns || [];
        const { card, body } = createAnalyticsCard("Detected Communication Patterns", "#3fb950");

        if (patterns.length === 0) {
            const empty = document.createElement("div");
            empty.textContent = "No strong communication pattern heuristics were detected.";
            empty.style.color = "#8b949e";
            body.appendChild(empty);
            container.appendChild(card);
            return;
        }

        patterns.forEach(pattern => {
            const row = document.createElement("div");
            row.style.padding = "10px 0";
            row.style.borderTop = "1px solid #21262d";

            const top = document.createElement("div");
            top.style.display = "flex";
            top.style.justifyContent = "space-between";
            top.style.alignItems = "center";
            top.style.gap = "10px";

            const title = document.createElement("div");
            title.style.color = "#c9d1d9";
            title.style.fontWeight = "600";
            title.textContent = String(pattern.type || "pattern").replace(/_/g, " ");

            const strength = document.createElement("div");
            strength.style.color = "#3fb950";
            strength.style.fontFamily = "'Fira Code', monospace";
            strength.textContent = `strength ${(pattern.strength || 0).toFixed(2)}`;

            const desc = document.createElement("div");
            desc.style.color = "#8b949e";
            desc.style.marginTop = "6px";
            desc.style.fontSize = "0.85rem";
            desc.textContent = pattern.description || "";

            row.appendChild(top);
            top.appendChild(title);
            top.appendChild(strength);
            row.appendChild(desc);

            if (Array.isArray(pattern.ranks) && pattern.ranks.length > 0) {
                const ranksRow = document.createElement("div");
                ranksRow.style.marginTop = "8px";
                pattern.ranks.forEach(rank => {
                    ranksRow.appendChild(
                        createClickablePill(`rank ${rank}`, "#58a6ff", () => focusRankFromAnalytics(rank))
                    );
                });
                row.appendChild(ranksRow);
            }

            if (Array.isArray(pattern.pairs) && pattern.pairs.length > 0) {
                const pairsRow = document.createElement("div");
                pairsRow.style.marginTop = "8px";
                pattern.pairs.slice(0, 5).forEach(pair => {
                    if (pair && Array.isArray(pair.ranks) && pair.ranks.length === 2) {
                        pairsRow.appendChild(
                            createClickablePill(
                                `${pair.ranks[0]} ↔ ${pair.ranks[1]}`,
                                "#a371f7",
                                () => focusLinkFromAnalytics(pair.ranks[0], pair.ranks[1])
                            )
                        );
                    }
                });
                row.appendChild(pairsRow);
            }

            body.appendChild(row);
        });

        container.appendChild(card);
    }

    function renderAnalysisIssuesCard(container, analysis) {
        const issues = analysis.issues || [];
        const { card, body } = createAnalyticsCard("Potential Performance Issues", "#f85149");

        if (issues.length === 0) {
            const empty = document.createElement("div");
            empty.textContent = "No obvious performance issues were flagged by the heuristic analysis.";
            empty.style.color = "#8b949e";
            body.appendChild(empty);
            container.appendChild(card);
            return;
        }

        issues.forEach(issue => {
            const color = severityColor(issue.severity);

            const box = document.createElement("div");
            box.style.padding = "10px 12px";
            box.style.marginBottom = "10px";
            box.style.backgroundColor = "rgba(13, 17, 23, 0.75)";
            box.style.border = `1px solid ${color}`;
            box.style.borderRadius = "6px";

            const top = document.createElement("div");
            top.style.display = "flex";
            top.style.justifyContent = "space-between";
            top.style.alignItems = "center";
            top.style.gap = "12px";

            const title = document.createElement("div");
            title.style.color = "#c9d1d9";
            title.style.fontWeight = "700";
            title.textContent = String(issue.type || "issue").replace(/_/g, " ");

            const sev = document.createElement("div");
            sev.textContent = `${String(issue.severity || "info").toUpperCase()} ${(issue.score || 0).toFixed(2)}`;
            sev.style.color = color;
            sev.style.fontFamily = "'Fira Code', monospace";
            sev.style.fontSize = "0.8rem";

            const desc = document.createElement("div");
            desc.style.color = "#8b949e";
            desc.style.marginTop = "6px";
            desc.style.fontSize = "0.85rem";
            desc.textContent = issue.description || "";

            box.appendChild(top);
            top.appendChild(title);
            top.appendChild(sev);
            box.appendChild(desc);

            const tags = document.createElement("div");
            tags.style.marginTop = "8px";

            if (Array.isArray(issue.ranks)) {
                issue.ranks.forEach(rank => {
                    tags.appendChild(
                        createClickablePill(`rank ${rank}`, color, () => focusRankFromAnalytics(rank))
                    );
                });
            }

            if (Array.isArray(issue.pairs)) {
                issue.pairs.forEach(pair => {
                    if (Array.isArray(pair) && pair.length === 2) {
                        tags.appendChild(
                            createClickablePill(`${pair[0]} → ${pair[1]}`, color, () => focusLinkFromAnalytics(pair[0], pair[1]))
                        );
                    }
                });
            }

            if (tags.childNodes.length > 0) {
                box.appendChild(tags);
            }

            body.appendChild(box);
        });

        container.appendChild(card);
    }

    function renderAnalysisTopRanksCard(container, analysis) {
        const ranks = analysis.top_ranks_by_touch_bytes || [];
        const { card, body } = createAnalyticsCard("Top Communicating Ranks", "#d29922");

        if (ranks.length === 0) {
            const empty = document.createElement("div");
            empty.textContent = "No rank-level communication summary available.";
            empty.style.color = "#8b949e";
            body.appendChild(empty);
            container.appendChild(card);
            return;
        }

        const { table, tbody } = createAnalyticsTable(["Rank", "Touch Bytes", "Action"]);

        ranks.slice(0, 10).forEach(item => {
            const tr = document.createElement("tr");
            tr.style.borderBottom = "1px solid #21262d";

            const tdRank = document.createElement("td");
            tdRank.style.padding = "8px";
            tdRank.style.color = "#c9d1d9";
            tdRank.style.fontFamily = "'Fira Code', monospace";
            tdRank.textContent = item.rank;

            const tdBytes = document.createElement("td");
            tdBytes.style.padding = "8px";
            tdBytes.style.color = "#c9d1d9";
            tdBytes.textContent = formatBytes(item.bytes || 0);

            const tdAction = document.createElement("td");
            tdAction.style.padding = "8px";

            const btn = document.createElement("button");
            btn.textContent = "Focus";
            btn.style.background = "#21262d";
            btn.style.color = "#c9d1d9";
            btn.style.border = "1px solid #30363d";
            btn.style.borderRadius = "4px";
            btn.style.padding = "4px 8px";
            btn.style.cursor = "pointer";
            btn.addEventListener("click", () => focusRankFromAnalytics(item.rank));

            tdAction.appendChild(btn);

            tr.appendChild(tdRank);
            tr.appendChild(tdBytes);
            tr.appendChild(tdAction);
            tbody.appendChild(tr);
        });

        body.appendChild(table);
        container.appendChild(card);
    }

    function renderAnalysisTopLinksCard(container, analysis) {
        const links = analysis.top_links || [];
        const { card, body } = createAnalyticsCard("Hottest Communication Links", "#a371f7");

        if (links.length === 0) {
            const empty = document.createElement("div");
            empty.textContent = "No hot-link analysis data available.";
            empty.style.color = "#8b949e";
            body.appendChild(empty);
            container.appendChild(card);
            return;
        }

        const { table, tbody } = createAnalyticsTable(["Link", "Bytes", "Messages", "Action"]);

        links.slice(0, 10).forEach(link => {
            const tr = document.createElement("tr");
            tr.style.borderBottom = "1px solid #21262d";

            const tdLink = document.createElement("td");
            tdLink.style.padding = "8px";
            tdLink.style.color = "#c9d1d9";
            tdLink.style.fontFamily = "'Fira Code', monospace";
            tdLink.textContent = `${link.sender} → ${link.receiver}`;

            const tdBytes = document.createElement("td");
            tdBytes.style.padding = "8px";
            tdBytes.style.color = "#c9d1d9";
            tdBytes.textContent = formatBytes(link.bytes || 0);

            const tdMsgs = document.createElement("td");
            tdMsgs.style.padding = "8px";
            tdMsgs.style.color = "#c9d1d9";
            tdMsgs.textContent = String(link.messages || 0);

            const tdAction = document.createElement("td");
            tdAction.style.padding = "8px";

            const btn = document.createElement("button");
            btn.textContent = "Focus";
            btn.style.background = "#21262d";
            btn.style.color = "#c9d1d9";
            btn.style.border = "1px solid #30363d";
            btn.style.borderRadius = "4px";
            btn.style.padding = "4px 8px";
            btn.style.cursor = "pointer";
            btn.addEventListener("click", () => focusLinkFromAnalytics(link.sender, link.receiver));

            tdAction.appendChild(btn);

            tr.appendChild(tdLink);
            tr.appendChild(tdBytes);
            tr.appendChild(tdMsgs);
            tr.appendChild(tdAction);
            tbody.appendChild(tr);
        });

        body.appendChild(table);
        container.appendChild(card);
    }

    function renderAnalysisCollectiveRootsCard(container, analysis) {
        const roots = analysis.collective_roots || [];
        if (roots.length === 0) return;

        const { card, body } = createAnalyticsCard("Collective Roots", "#d29922");
        const { table, tbody } = createAnalyticsTable(["Root", "Bytes", "Events", "Action"]);

        roots.slice(0, 8).forEach(root => {
            const tr = document.createElement("tr");
            tr.style.borderBottom = "1px solid #21262d";

            const tdRoot = document.createElement("td");
            tdRoot.style.padding = "8px";
            tdRoot.style.color = "#c9d1d9";
            tdRoot.style.fontFamily = "'Fira Code', monospace";
            tdRoot.textContent = root.root;

            const tdBytes = document.createElement("td");
            tdBytes.style.padding = "8px";
            tdBytes.style.color = "#c9d1d9";
            tdBytes.textContent = formatBytes(root.bytes || 0);

            const tdEvents = document.createElement("td");
            tdEvents.style.padding = "8px";
            tdEvents.style.color = "#c9d1d9";
            tdEvents.textContent = String(root.events || 0);

            const tdAction = document.createElement("td");
            tdAction.style.padding = "8px";

            const btn = document.createElement("button");
            btn.textContent = "Focus";
            btn.style.background = "#21262d";
            btn.style.color = "#c9d1d9";
            btn.style.border = "1px solid #30363d";
            btn.style.borderRadius = "4px";
            btn.style.padding = "4px 8px";
            btn.style.cursor = "pointer";
            btn.addEventListener("click", () => focusRankFromAnalytics(root.root));

            tdAction.appendChild(btn);

            tr.appendChild(tdRoot);
            tr.appendChild(tdBytes);
            tr.appendChild(tdEvents);
            tr.appendChild(tdAction);
            tbody.appendChild(tr);
        });

        body.appendChild(table);
        container.appendChild(card);
    }

    function renderAnalysisBarrierCard(container, analysis) {
        const spreads = analysis.barrier_spreads || [];
        if (spreads.length === 0) return;

        const { card, body } = createAnalyticsCard("Barrier Skew", "#f0883e");

        const worst = [...spreads]
            .sort((a, b) => (b.spread || 0) - (a.spread || 0))
            .slice(0, 8);

        const { table, tbody } = createAnalyticsTable(["Barrier", "Start", "End", "Spread"]);

        worst.forEach(item => {
            const tr = document.createElement("tr");
            tr.style.borderBottom = "1px solid #21262d";

            [
                item.barrier_index,
                formatTimeValue(item.t_min || 0),
                formatTimeValue(item.t_max || 0),
                formatTimeValue(item.spread || 0)
            ].forEach(val => {
                const td = document.createElement("td");
                td.style.padding = "8px";
                td.style.color = "#c9d1d9";
                td.style.fontFamily = "'Fira Code', monospace";
                td.textContent = String(val);
                tr.appendChild(td);
            });

            tbody.appendChild(tr);
        });

        body.appendChild(table);
        container.appendChild(card);
    }

    function renderAnalysisTimeWindowsCard(container, analysis) {
        const windows = analysis.time_windows || [];
        const { card, body } = createAnalyticsCard("Communication Phases", "#58a6ff");

        if (windows.length === 0) {
            const empty = document.createElement("div");
            empty.textContent = "No time-window analysis available.";
            empty.style.color = "#8b949e";
            body.appendChild(empty);
            container.appendChild(card);
            return;
        }

        const intro = document.createElement("div");
        intro.textContent = "Click a bar to seek to that phase of execution.";
        intro.style.fontSize = "0.8rem";
        intro.style.color = "#8b949e";
        intro.style.marginBottom = "10px";
        body.appendChild(intro);

        const strip = document.createElement("div");
        strip.style.display = "flex";
        strip.style.alignItems = "flex-end";
        strip.style.gap = "3px";
        strip.style.height = "90px";
        strip.style.padding = "8px";
        strip.style.background = "rgba(13, 17, 23, 0.65)";
        strip.style.border = "1px solid #30363d";
        strip.style.borderRadius = "6px";

        const maxBytes = Math.max(...windows.map(w => w.canonical_transfer_bytes || 0), 1);
        analyticsWindowBars = [];

        windows.forEach((win, idx) => {
            const bar = document.createElement("div");
            const frac = (win.canonical_transfer_bytes || 0) / maxBytes;
            const h = Math.max(8, Math.round(70 * frac));

            bar.style.flex = "1 1 0";
            bar.style.height = `${h}px`;
            bar.style.background = "rgba(88, 166, 255, 0.45)";
            bar.style.border = "1px solid rgba(88, 166, 255, 0.35)";
            bar.style.borderRadius = "4px 4px 0 0";
            bar.style.cursor = "pointer";
            bar.style.transition = "all 0.12s ease-out";

            bar.title =
                `Window ${idx}\n` +
                `Start: ${formatTimeValue(win.t_start || 0)}\n` +
                `End: ${formatTimeValue(win.t_end || 0)}\n` +
                `Transfer Bytes: ${formatBytes(win.canonical_transfer_bytes || 0)}\n` +
                `Transfers: ${win.canonical_transfer_events || 0}\n` +
                `Completions: ${win.completion_events || 0}\n` +
                `Barriers: ${win.barrier_events || 0}`;

            bar.addEventListener("click", async () => {
                if (typeof pausePlayback === "function") {
                    pausePlayback();
                }
                const mid = ((win.t_start || 0) + (win.t_end || 0)) / 2;
                if (typeof seekToTime === "function") {
                    await seekToTime(mid);
                }
            });

            strip.appendChild(bar);
            analyticsWindowBars.push({
                el: bar,
                start: win.t_start || 0,
                end: win.t_end || 0
            });
        });

        body.appendChild(strip);
        container.appendChild(card);
    }

    function renderAnalytics() {
        const container = ensureAnalyticsContainer();
        container.innerHTML = "";
        analyticsWindowBars = [];

        if (typeof parsedData === "undefined" || !parsedData || !parsedData.analysis) {
            const { card, body } = createAnalyticsCard("Analysis", "#8b949e");
            body.textContent = "No analytics data available in this profile.";
            body.style.color = "#8b949e";
            container.appendChild(card);
            return;
        }

        const analysis = parsedData.analysis;

        renderAnalysisSummaryCard(container, analysis);
        renderAnalysisPatternsCard(container, analysis);
        renderAnalysisIssuesCard(container, analysis);
        renderAnalysisTopRanksCard(container, analysis);
        renderAnalysisTopLinksCard(container, analysis);
        renderAnalysisCollectiveRootsCard(container, analysis);
        renderAnalysisBarrierCard(container, analysis);
        renderAnalysisTimeWindowsCard(container, analysis);

        const now = (typeof currentTime !== "undefined") ? currentTime : 0;
        updateAnalyticsTimeWindowIndicator(now);
    }

    function updateAnalyticsTimeWindowIndicator(time) {
        if (!analyticsWindowBars || analyticsWindowBars.length === 0) return;

        analyticsWindowBars.forEach(win => {
            const active = time >= win.start && time <= win.end;
            win.el.style.background = active
                ? "rgba(63, 185, 80, 0.9)"
                : "rgba(88, 166, 255, 0.45)";
            win.el.style.border = active
                ? "1px solid rgba(63, 185, 80, 1)"
                : "1px solid rgba(88, 166, 255, 0.35)";
            win.el.style.boxShadow = active
                ? "0 0 12px rgba(63, 185, 80, 0.65)"
                : "none";
        });
    }

    window.AnalyticsUI = {
        renderAnalytics,
        updateAnalyticsTimeWindowIndicator
    };
})();

