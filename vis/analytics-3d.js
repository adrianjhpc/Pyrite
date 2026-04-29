(function () {
    "use strict";

    const DEFAULTS = {
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

    const COLORS = {
        critical: 0xf85149,
        warning: 0xd29922,
        info: 0x58a6ff,
        root: 0xf0883e,
        pattern: 0x3fb950,
        hotspot: 0xa371f7
    };

    let options = Object.assign({}, DEFAULTS);

    let rootGroup = null;
    let attachedObjects = [];
    let animatedHalos = [];
    let animatedLinks = [];

    function hasCoreGlobals() {
        return (
            typeof THREE !== "undefined" &&
            typeof scene !== "undefined" &&
            typeof rankMap !== "undefined" &&
            typeof parsedData !== "undefined"
        );
    }

    function ensureRootGroup() {
        if (!hasCoreGlobals()) return null;

        if (rootGroup && rootGroup.parent) return rootGroup;

        rootGroup = new THREE.Group();
        rootGroup.name = "analytics3dRoot";
        scene.add(rootGroup);
        return rootGroup;
    }

    function disposeMaterial(mat) {
        if (!mat) return;
        if (Array.isArray(mat)) {
            mat.forEach(m => { if (m && typeof m.dispose === "function") m.dispose(); });
        } else if (typeof mat.dispose === "function") {
            mat.dispose();
        }
    }

    function disposeObjectDeep(obj) {
        if (!obj) return;
        obj.traverse(child => {
            if (child.geometry && typeof child.geometry.dispose === "function") {
                child.geometry.dispose();
            }
            disposeMaterial(child.material);
        });
    }

    function attachToParent(parent, obj) {
        if (!parent || !obj) return;
        parent.add(obj);
        attachedObjects.push({ parent, obj });
    }

    function safeRemove(parent, obj) {
        if (!parent || !obj) return;
        try {
            parent.remove(obj);
        } catch (e) {
            // ignore
        }
    }

    function clear() {
        animatedHalos = [];
        animatedLinks = [];

        attachedObjects.forEach(entry => {
            safeRemove(entry.parent, entry.obj);
            disposeObjectDeep(entry.obj);
        });
        attachedObjects = [];

        if (rootGroup && rootGroup.parent) {
            safeRemove(rootGroup.parent, rootGroup);
        }
        if (rootGroup) {
            disposeObjectDeep(rootGroup);
        }
        rootGroup = null;
    }

    function setEnabled(enabled) {
        options.enabled = !!enabled;
        if (!options.enabled) {
            clear();
        } else {
            refreshHighlights();
        }
    }

    function mergeOptions(newOptions) {
        options = Object.assign({}, options, newOptions || {});

        if (!options.enabled) {
            clear();
            return;
        }

        refreshHighlights();
    }

    function getConfig() {
        return Object.assign({}, options);
    }

    function priorityForRankKind(kind, severity) {
        if (kind === "issue") {
            if (severity === "critical") return 400;
            if (severity === "warning") return 300;
            return 200;
        }
        if (kind === "root") return 120;
        if (kind === "pattern") return 100;
        if (kind === "top") return 50;
        return 1;
    }

    function priorityForLinkKind(kind, severity) {
        if (kind === "issue") {
            if (severity === "critical") return 400;
            if (severity === "warning") return 300;
            return 200;
        }
        if (kind === "pingpong") return 130;
        if (kind === "top") return 100;
        return 1;
    }

    function pickColorForRank(kind, severity) {
        if (kind === "issue") {
            if (severity === "critical") return COLORS.critical;
            if (severity === "warning") return COLORS.warning;
            return COLORS.info;
        }
        if (kind === "root") return COLORS.root;
        if (kind === "pattern") return COLORS.pattern;
        return COLORS.info;
    }

    function pickColorForLink(kind, severity) {
        if (kind === "issue") {
            if (severity === "critical") return COLORS.critical;
            if (severity === "warning") return COLORS.warning;
            return COLORS.info;
        }
        if (kind === "pingpong") return COLORS.pattern;
        return COLORS.hotspot;
    }

    function collectRankHighlights(analysis) {
        const rankHighlights = new Map();

        function addRank(rank, kind, color, priority, reason) {
            if (rank == null) return;
            const current = rankHighlights.get(rank);
            if (!current || priority > current.priority) {
                rankHighlights.set(rank, {
                    rank,
                    kind,
                    color,
                    priority,
                    reasons: [reason]
                });
            } else if (current) {
                current.reasons.push(reason);
            }
        }

        if (options.showIssueRanks && Array.isArray(analysis.issues)) {
            analysis.issues.forEach(issue => {
                const sev = issue.severity || "info";
                const priority = priorityForRankKind("issue", sev);
                const color = pickColorForRank("issue", sev);
                const ranks = Array.isArray(issue.ranks) ? issue.ranks : [];
                ranks.forEach(rank => {
                    addRank(rank, "issue", color, priority, issue.type || "issue");
                });
            });
        }

        if (options.showCollectiveRoots && Array.isArray(analysis.collective_roots)) {
            analysis.collective_roots.slice(0, options.maxCollectiveRoots).forEach(rootInfo => {
                const rank = rootInfo.root;
                addRank(
                    rank,
                    "root",
                    pickColorForRank("root"),
                    priorityForRankKind("root"),
                    "collective_root"
                );
            });
        }

        if (options.showPatternRanks && Array.isArray(analysis.patterns)) {
            analysis.patterns.forEach(pattern => {
                const ranks = Array.isArray(pattern.ranks) ? pattern.ranks : [];
                ranks.forEach(rank => {
                    addRank(
                        rank,
                        "pattern",
                        pickColorForRank("pattern"),
                        priorityForRankKind("pattern"),
                        pattern.type || "pattern"
                    );
                });
            });
        }

        if (options.showTopRanks && Array.isArray(analysis.top_ranks_by_touch_bytes)) {
            analysis.top_ranks_by_touch_bytes.slice(0, options.maxTopRanks).forEach(item => {
                addRank(
                    item.rank,
                    "top",
                    pickColorForRank("top"),
                    priorityForRankKind("top"),
                    "top_rank"
                );
            });
        }

        return Array.from(rankHighlights.values());
    }

    function collectLinkHighlights(analysis) {
        const linkHighlights = new Map();

        function addLink(sender, receiver, kind, color, priority, reason, bytesValue) {
            if (sender == null || receiver == null || sender === receiver) return;
            const key = `${sender}->${receiver}`;
            const current = linkHighlights.get(key);
            if (!current || priority > current.priority) {
                linkHighlights.set(key, {
                    sender,
                    receiver,
                    kind,
                    color,
                    priority,
                    reason,
                    bytes: bytesValue || 0
                });
            } else if (current) {
                current.bytes = Math.max(current.bytes || 0, bytesValue || 0);
            }
        }

        if (options.showIssueLinks && Array.isArray(analysis.issues)) {
            analysis.issues.forEach(issue => {
                const sev = issue.severity || "info";
                const priority = priorityForLinkKind("issue", sev);
                const color = pickColorForLink("issue", sev);

                const pairs = Array.isArray(issue.pairs) ? issue.pairs : [];
                pairs.forEach(pair => {
                    if (Array.isArray(pair) && pair.length === 2) {
                        addLink(pair[0], pair[1], "issue", color, priority, issue.type || "issue", 0);
                    }
                });
            });
        }

        if (options.showTopLinks && Array.isArray(analysis.top_links)) {
            analysis.top_links.slice(0, options.maxTopLinks).forEach(link => {
                addLink(
                    link.sender,
                    link.receiver,
                    "top",
                    pickColorForLink("top"),
                    priorityForLinkKind("top"),
                    "top_link",
                    link.bytes || 0
                );
            });
        }

        if (options.showPingPongPairs && Array.isArray(analysis.patterns)) {
            analysis.patterns.forEach(pattern => {
                if (pattern.type !== "ping_pong" || !Array.isArray(pattern.pairs)) return;
                pattern.pairs.slice(0, 4).forEach(pair => {
                    if (pair && Array.isArray(pair.ranks) && pair.ranks.length === 2) {
                        addLink(
                            pair.ranks[0],
                            pair.ranks[1],
                            "pingpong",
                            pickColorForLink("pingpong"),
                            priorityForLinkKind("pingpong"),
                            "ping_pong",
                            pair.bytes_total || 0
                        );
                    }
                });
            });
        }

        return Array.from(linkHighlights.values())
            .sort((a, b) => {
                if ((b.priority || 0) !== (a.priority || 0)) {
                    return (b.priority || 0) - (a.priority || 0);
                }
                return (b.bytes || 0) - (a.bytes || 0);
            })
            .slice(0, options.maxTopLinks + 4);
    }

    function getRankWorldPosition(rankId) {
        const r = rankMap.get(rankId);
        if (!r || !r.nodeGroup) return null;

        r.nodeGroup.updateMatrixWorld(true);

        const pos = r.localPos.clone();
        pos.applyMatrix4(r.nodeGroup.matrixWorld);
        return pos;
    }

    function createRankHalo(highlight) {
        const r = rankMap.get(highlight.rank);
        if (!r || !r.nodeGroup) return;

        const color = new THREE.Color(highlight.color);
        const radius = Math.max(0.22, ((r.depth || 0.2) * 0.9) + 0.16);

        const outerMat = new THREE.MeshBasicMaterial({
            color: color,
            transparent: true,
            opacity: 0.16,
            depthWrite: false,
            depthTest: false,
            blending: THREE.AdditiveBlending
        });

        const wireMat = new THREE.MeshBasicMaterial({
            color: color,
            wireframe: true,
            transparent: true,
            opacity: 0.85,
            depthWrite: false,
            depthTest: false
        });

        const outerGeo = new THREE.SphereGeometry(radius, 14, 12);
        const wireGeo = new THREE.SphereGeometry(radius * 1.22, 12, 10);

        const group = new THREE.Group();
        group.name = "analytics3dRankHalo";
        group.position.copy(r.localPos);
        group.userData = {
            rank: highlight.rank,
            reasons: highlight.reasons || [],
            kind: highlight.kind
        };

        const outer = new THREE.Mesh(outerGeo, outerMat);
        const wire = new THREE.Mesh(wireGeo, wireMat);

        group.add(outer);
        group.add(wire);

        attachToParent(r.nodeGroup, group);

        animatedHalos.push({
            group,
            outer,
            wire,
            baseScale: 1.0,
            color,
            phase: Math.random() * Math.PI * 2,
            speed: 0.0012 + Math.random() * 0.0008
        });
    }

    function makeArcMidpoint(start, end, liftFactor, lateralOffset) {
        const mid = start.clone().lerp(end, 0.5);
        const dir = end.clone().sub(start);
        const dist = dir.length();

        let sideways = new THREE.Vector3(-dir.y, dir.x, 0);
        if (sideways.lengthSq() < 1e-6) {
            sideways = new THREE.Vector3(0, 1, 0);
        }
        sideways.normalize().multiplyScalar(lateralOffset);

        mid.add(sideways);
        mid.z += Math.max(3.0, dist * liftFactor);
        return mid;
    }

    function createLinkHighlight(link, maxBytes) {
        const start = getRankWorldPosition(link.sender);
        const end = getRankWorldPosition(link.receiver);
        if (!start || !end) return;

        const color = new THREE.Color(link.color);
        const liftFactor = (link.kind === "pingpong") ? 0.22 : 0.16;
        const lateral = (link.sender > link.receiver) ? 2.5 : -2.5;
        const mid = makeArcMidpoint(start, end, liftFactor, lateral);

        const curve = new THREE.QuadraticBezierCurve3(start, mid, end);

        const relative = maxBytes > 0 ? (link.bytes || 0) / maxBytes : 0;
        const radius = 0.10 + (0.22 * Math.min(1, relative));

        const tubeGeo = new THREE.TubeGeometry(curve, 24, radius, 6, false);
        const tubeMat = new THREE.MeshBasicMaterial({
            color: color,
            transparent: true,
            opacity: 0.30,
            depthWrite: false,
            depthTest: false,
            blending: THREE.AdditiveBlending
        });

        const linePoints = curve.getPoints(32);
        const lineGeo = new THREE.BufferGeometry().setFromPoints(linePoints);
        const lineMat = new THREE.LineBasicMaterial({
            color: color,
            transparent: true,
            opacity: 0.85,
            depthWrite: false,
            depthTest: false
        });

        const group = new THREE.Group();
        group.name = "analytics3dLink";
        group.userData = {
            sender: link.sender,
            receiver: link.receiver,
            reason: link.reason,
            kind: link.kind
        };

        const tube = new THREE.Mesh(tubeGeo, tubeMat);
        const line = new THREE.Line(lineGeo, lineMat);

        const endpointGeo = new THREE.SphereGeometry(radius * 1.4, 10, 8);
        const endpointMat = new THREE.MeshBasicMaterial({
            color: color,
            transparent: true,
            opacity: 0.65,
            depthWrite: false,
            depthTest: false
        });

        const p1 = new THREE.Mesh(endpointGeo, endpointMat.clone());
        const p2 = new THREE.Mesh(endpointGeo.clone(), endpointMat.clone());
        p1.position.copy(start);
        p2.position.copy(end);

        group.add(tube);
        group.add(line);
        group.add(p1);
        group.add(p2);

        const root = ensureRootGroup();
        attachToParent(root, group);

        animatedLinks.push({
            group,
            tube,
            line,
            endpoints: [p1, p2],
            phase: Math.random() * Math.PI * 2,
            speed: 0.001 + Math.random() * 0.0006,
            baseTubeOpacity: tubeMat.opacity,
            baseLineOpacity: lineMat.opacity
        });
    }

    function refreshHighlights(customOptions) {
        if (customOptions) {
            options = Object.assign({}, options, customOptions);
        }

        clear();

        if (!options.enabled || !hasCoreGlobals() || !parsedData || !parsedData.analysis) {
            return;
        }

        const analysis = parsedData.analysis;

        const rankHighlights = collectRankHighlights(analysis);
        rankHighlights.forEach(createRankHalo);

        const linkHighlights = collectLinkHighlights(analysis);
        const maxLinkBytes = Math.max(...linkHighlights.map(l => l.bytes || 0), 1);
        linkHighlights.forEach(link => createLinkHighlight(link, maxLinkBytes));
    }

    function update(timeMs) {
        if (!options.enabled) return;

        const t = Number(timeMs) || 0;

        animatedHalos.forEach(entry => {
            const s = 1.0 + 0.08 * Math.sin(t * entry.speed + entry.phase);
            const s2 = 1.0 + 0.14 * Math.sin(t * entry.speed * 0.8 + entry.phase + 0.8);

            entry.outer.scale.setScalar(s);
            entry.wire.scale.setScalar(s2);

            if (entry.outer.material) {
                entry.outer.material.opacity = 0.13 + 0.06 * (0.5 + 0.5 * Math.sin(t * entry.speed + entry.phase));
            }
            if (entry.wire.material) {
                entry.wire.material.opacity = 0.55 + 0.25 * (0.5 + 0.5 * Math.sin(t * entry.speed * 1.1 + entry.phase));
            }
        });

        animatedLinks.forEach(entry => {
            const pulse = 0.5 + 0.5 * Math.sin(t * entry.speed + entry.phase);

            if (entry.tube.material) {
                entry.tube.material.opacity = entry.baseTubeOpacity * (0.75 + 0.55 * pulse);
            }
            if (entry.line.material) {
                entry.line.material.opacity = entry.baseLineOpacity * (0.75 + 0.45 * pulse);
            }

            entry.endpoints.forEach((pt, idx) => {
                const scale = 1.0 + 0.15 * Math.sin(t * entry.speed * 1.15 + entry.phase + idx);
                pt.scale.setScalar(scale);
                if (pt.material) {
                    pt.material.opacity = 0.50 + 0.25 * pulse;
                }
            });
        });
    }

    window.Analytics3D = {
        refreshHighlights,
        update,
        clear,
        setEnabled,
        configure: mergeOptions,
        getConfig
    };

})();

