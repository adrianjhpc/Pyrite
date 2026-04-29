(function () {
    "use strict";

    const DEFAULT_CONFIG = {
        enabled: true,
        maxTopRanks: 5,
        maxTopLinks: 6,
        maxCollectiveRoots: 3,
        showIssueRanks: true,
        showPatternRanks: true,
        showCollectiveRoots: true,
        showTopRanks: true,
        showIssueLinks: true,
        showTopLinks: true,
        showPingPongPairs: true
    };

    let ui = {
        container: null,
        inputs: {},
        status: null
    };

    function getCurrentConfig() {
        if (window.Analytics3D && typeof window.Analytics3D.getConfig === "function") {
            return window.Analytics3D.getConfig();
        }
        return Object.assign({}, DEFAULT_CONFIG);
    }

    function ensureContainer() {
        let container = document.getElementById("analysis3dControlsContainer");
        if (container) {
            ui.container = container;
            return container;
        }

        container = document.createElement("div");
        container.id = "analysis3dControlsContainer";
        container.style.marginTop = "20px";
        container.style.display = "flex";
        container.style.flexDirection = "column";
        container.style.gap = "14px";

        const analysisContainer = document.getElementById("analysisContainer");
        const overallStatsContainer = document.getElementById("overallStatsContainer");
        const sidebar = document.getElementById("sidebar");

        if (analysisContainer && analysisContainer.parentNode) {
            analysisContainer.parentNode.insertBefore(container, analysisContainer.nextSibling);
        } else if (overallStatsContainer && overallStatsContainer.parentNode) {
            overallStatsContainer.parentNode.insertBefore(container, overallStatsContainer.parentNode);
        } else if (sidebar) {
            sidebar.appendChild(container);
        } else {
            document.body.appendChild(container);
        }

        ui.container = container;
        return container;
    }

    function createCard(title, accent) {
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

    function createSectionTitle(text) {
        const el = document.createElement("div");
        el.textContent = text;
        el.style.fontSize = "0.73rem";
        el.style.color = "#8b949e";
        el.style.fontWeight = "700";
        el.style.letterSpacing = "0.5px";
        el.style.margin = "10px 0 8px 0";
        el.style.textTransform = "uppercase";
        return el;
    }

    function createCheckboxRow(label, key, checked) {
        const row = document.createElement("label");
        row.style.display = "flex";
        row.style.alignItems = "center";
        row.style.gap = "8px";
        row.style.color = "#c9d1d9";
        row.style.fontSize = "0.85rem";
        row.style.marginBottom = "8px";
        row.style.cursor = "pointer";

        const input = document.createElement("input");
        input.type = "checkbox";
        input.checked = !!checked;
        input.dataset.key = key;

        const text = document.createElement("span");
        text.textContent = label;

        row.appendChild(input);
        row.appendChild(text);

        ui.inputs[key] = input;
        return row;
    }

    function createNumberRow(label, key, value, min, max, step) {
        const row = document.createElement("div");
        row.style.display = "grid";
        row.style.gridTemplateColumns = "1fr 90px";
        row.style.gap = "10px";
        row.style.alignItems = "center";
        row.style.marginBottom = "8px";

        const text = document.createElement("label");
        text.textContent = label;
        text.style.color = "#c9d1d9";
        text.style.fontSize = "0.85rem";

        const input = document.createElement("input");
        input.type = "number";
        input.value = String(value);
        input.min = String(min);
        input.max = String(max);
        input.step = String(step);
        input.dataset.key = key;
        input.style.backgroundColor = "#0d1117";
        input.style.color = "#c9d1d9";
        input.style.border = "1px solid #30363d";
        input.style.borderRadius = "4px";
        input.style.padding = "6px 8px";
        input.style.width = "100%";

        row.appendChild(text);
        row.appendChild(input);

        ui.inputs[key] = input;
        return row;
    }

    function createLegendRow(label, color, kind) {
        const row = document.createElement("div");
        row.style.display = "flex";
        row.style.alignItems = "center";
        row.style.gap = "10px";
        row.style.marginBottom = "8px";

        const swatchWrap = document.createElement("div");
        swatchWrap.style.width = "22px";
        swatchWrap.style.display = "flex";
        swatchWrap.style.justifyContent = "center";
        swatchWrap.style.alignItems = "center";

        if (kind === "line") {
            const line = document.createElement("div");
            line.style.width = "20px";
            line.style.height = "4px";
            line.style.borderRadius = "999px";
            line.style.background = color;
            line.style.boxShadow = `0 0 8px ${color}`;
            swatchWrap.appendChild(line);
        } else {
            const dot = document.createElement("div");
            dot.style.width = "12px";
            dot.style.height = "12px";
            dot.style.borderRadius = "999px";
            dot.style.background = color;
            dot.style.boxShadow = `0 0 10px ${color}`;
            swatchWrap.appendChild(dot);
        }

        const text = document.createElement("div");
        text.style.color = "#c9d1d9";
        text.style.fontSize = "0.84rem";
        text.textContent = label;

        row.appendChild(swatchWrap);
        row.appendChild(text);
        return row;
    }

    function toBool(key) {
        return !!(ui.inputs[key] && ui.inputs[key].checked);
    }

    function toInt(key, fallback) {
        if (!ui.inputs[key]) return fallback;
        const v = parseInt(ui.inputs[key].value, 10);
        return Number.isFinite(v) ? v : fallback;
    }

    function collectConfigFromUI() {
        return {
            enabled: toBool("enabled"),
            showIssueRanks: toBool("showIssueRanks"),
            showPatternRanks: toBool("showPatternRanks"),
            showCollectiveRoots: toBool("showCollectiveRoots"),
            showTopRanks: toBool("showTopRanks"),
            showIssueLinks: toBool("showIssueLinks"),
            showTopLinks: toBool("showTopLinks"),
            showPingPongPairs: toBool("showPingPongPairs"),
            maxTopRanks: Math.max(1, toInt("maxTopRanks", DEFAULT_CONFIG.maxTopRanks)),
            maxTopLinks: Math.max(1, toInt("maxTopLinks", DEFAULT_CONFIG.maxTopLinks)),
            maxCollectiveRoots: Math.max(1, toInt("maxCollectiveRoots", DEFAULT_CONFIG.maxCollectiveRoots))
        };
    }

    function setStatus(text, color) {
        if (!ui.status) return;
        ui.status.textContent = text || "";
        ui.status.style.color = color || "#8b949e";
    }

    function updateDisabledState() {
        const enabled = toBool("enabled");
        Object.keys(ui.inputs).forEach(key => {
            if (key === "enabled") return;
            ui.inputs[key].disabled = !enabled;
            ui.inputs[key].style.opacity = enabled ? "1" : "0.55";
        });

        if (!enabled) {
            setStatus("3D analytics overlay disabled", "#8b949e");
        } else {
            setStatus("3D analytics overlay active", "#3fb950");
        }
    }

    function applyConfig() {
        updateDisabledState();

        const cfg = collectConfigFromUI();

        if (!window.Analytics3D) {
            setStatus("Analytics3D is not loaded", "#f85149");
            return;
        }

        window.Analytics3D.configure(cfg);
        setStatus(
            cfg.enabled ? "3D analytics overlay active" : "3D analytics overlay disabled",
            cfg.enabled ? "#3fb950" : "#8b949e"
        );
    }

    function resetDefaults() {
        const cfg = Object.assign({}, DEFAULT_CONFIG);
        Object.keys(cfg).forEach(key => {
            if (!ui.inputs[key]) return;
            if (ui.inputs[key].type === "checkbox") {
                ui.inputs[key].checked = !!cfg[key];
            } else {
                ui.inputs[key].value = String(cfg[key]);
            }
        });
        applyConfig();
    }

    function buildControlsCard() {
        const cfg = getCurrentConfig();
        const { card, body } = createCard("3D Analytics Overlay", "#58a6ff");

        body.appendChild(createCheckboxRow("Enable analytics overlays", "enabled", cfg.enabled));

        body.appendChild(createSectionTitle("Rank highlights"));
        body.appendChild(createCheckboxRow("Issue-related ranks", "showIssueRanks", cfg.showIssueRanks));
        body.appendChild(createCheckboxRow("Pattern-related ranks", "showPatternRanks", cfg.showPatternRanks));
        body.appendChild(createCheckboxRow("Collective roots", "showCollectiveRoots", cfg.showCollectiveRoots));
        body.appendChild(createCheckboxRow("Top communicating ranks", "showTopRanks", cfg.showTopRanks));

        body.appendChild(createSectionTitle("Link highlights"));
        body.appendChild(createCheckboxRow("Issue-related links", "showIssueLinks", cfg.showIssueLinks));
        body.appendChild(createCheckboxRow("Top/hot links", "showTopLinks", cfg.showTopLinks));
        body.appendChild(createCheckboxRow("Ping-pong pairs", "showPingPongPairs", cfg.showPingPongPairs));

        body.appendChild(createSectionTitle("Limits"));
        body.appendChild(createNumberRow("Top ranks shown", "maxTopRanks", cfg.maxTopRanks, 1, 20, 1));
        body.appendChild(createNumberRow("Top links shown", "maxTopLinks", cfg.maxTopLinks, 1, 20, 1));
        body.appendChild(createNumberRow("Collective roots shown", "maxCollectiveRoots", cfg.maxCollectiveRoots, 1, 10, 1));

        const actions = document.createElement("div");
        actions.style.display = "flex";
        actions.style.gap = "8px";
        actions.style.marginTop = "12px";
        actions.style.flexWrap = "wrap";

        const btnRefresh = document.createElement("button");
        btnRefresh.textContent = "Refresh";
        btnRefresh.style.background = "#21262d";
        btnRefresh.style.color = "#c9d1d9";
        btnRefresh.style.border = "1px solid #30363d";
        btnRefresh.style.borderRadius = "4px";
        btnRefresh.style.padding = "6px 10px";
        btnRefresh.style.cursor = "pointer";
        btnRefresh.addEventListener("click", applyConfig);

        const btnReset = document.createElement("button");
        btnReset.textContent = "Reset defaults";
        btnReset.style.background = "#21262d";
        btnReset.style.color = "#c9d1d9";
        btnReset.style.border = "1px solid #30363d";
        btnReset.style.borderRadius = "4px";
        btnReset.style.padding = "6px 10px";
        btnReset.style.cursor = "pointer";
        btnReset.addEventListener("click", resetDefaults);

        actions.appendChild(btnRefresh);
        actions.appendChild(btnReset);
        body.appendChild(actions);

        const status = document.createElement("div");
        status.style.marginTop = "10px";
        status.style.fontSize = "0.8rem";
        status.style.color = "#8b949e";
        body.appendChild(status);
        ui.status = status;

        Object.keys(ui.inputs).forEach(key => {
            const el = ui.inputs[key];
            el.addEventListener("change", applyConfig);
            el.addEventListener("input", applyConfig);
        });

        updateDisabledState();
        return card;
    }

    function buildLegendCard() {
        const { card, body } = createCard("Analytics Highlight Legend", "#a371f7");

        body.appendChild(createLegendRow("Critical issue ranks / links", "#f85149", "dot"));
        body.appendChild(createLegendRow("Warning issue ranks / links", "#d29922", "dot"));
        body.appendChild(createLegendRow("Info / top-rank highlights", "#58a6ff", "dot"));
        body.appendChild(createLegendRow("Pattern ranks / ping-pong pairs", "#3fb950", "dot"));
        body.appendChild(createLegendRow("Collective roots", "#f0883e", "dot"));
        body.appendChild(createLegendRow("Hotspot / top links", "#a371f7", "line"));

        const note = document.createElement("div");
        note.style.marginTop = "10px";
        note.style.fontSize = "0.78rem";
        note.style.color = "#8b949e";
        note.textContent = "These overlays are persistent analytics cues and remain visible independently of playback-time message lines.";
        body.appendChild(note);

        return card;
    }

    function rebuildUI() {
        const container = ensureContainer();
        container.innerHTML = "";

        container.appendChild(buildControlsCard());
        container.appendChild(buildLegendCard());

        if (!window.Analytics3D) {
            setStatus("Analytics3D is not loaded", "#f85149");
        } else {
            applyConfig();
        }
    }

    function init() {
        ensureContainer();
        rebuildUI();
    }

    function refreshUIFromEngine() {
        const cfg = getCurrentConfig();
        Object.keys(cfg).forEach(key => {
            if (!ui.inputs[key]) return;
            if (ui.inputs[key].type === "checkbox") {
                ui.inputs[key].checked = !!cfg[key];
            } else {
                ui.inputs[key].value = String(cfg[key]);
            }
        });
        updateDisabledState();
    }

    window.Analytics3DControls = {
        init,
        rebuildUI,
        refreshUIFromEngine,
        apply: applyConfig,
        resetDefaults
    };

    document.addEventListener("DOMContentLoaded", init);
})();

