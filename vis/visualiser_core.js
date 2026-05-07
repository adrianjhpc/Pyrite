// ==========================================
// visualiser_core.js
// PURE 3D ENGINE & UI CONTROLLER
// ==========================================

const MPI_CATEGORIES = {
    "MPI_SEND": { type: "p2p_block", color: 0x58a6ff },
    "MPI_RECV": { type: "p2p_block", color: 0x58a6ff },
    "MPI_BSEND": { type: "p2p_block", color: 0x58a6ff },
    "MPI_SSEND": { type: "p2p_block", color: 0x58a6ff },
    "MPI_RSEND": { type: "p2p_block", color: 0x58a6ff },
    "MPI_SENDRECV": { type: "p2p_block", color: 0x58a6ff },
    "MPI_ISEND": { type: "p2p_nonblock", color: 0x3fb950 },
    "MPI_IRECV": { type: "p2p_nonblock", color: 0x3fb950 },
    "MPI_IBSEND": { type: "p2p_nonblock", color: 0x3fb950 },
    "MPI_ISSEND": { type: "p2p_nonblock", color: 0x3fb950 },
    "MPI_IRSEND": { type: "p2p_nonblock", color: 0x3fb950 },
    "MPI_WAIT": { type: "state", color: 0x238636 },
    "MPI_WAITALL": { type: "state", color: 0x238636 },
    "MPI_WAITANY": { type: "state", color: 0x238636 },
    "MPI_WAITSOME": { type: "state", color: 0x238636 },
    "MPI_TEST": { type: "state", color: 0x238636 },
    "MPI_TESTANY": { type: "state", color: 0x238636 },
    "MPI_TESTALL": { type: "state", color: 0x238636 },
    "MPI_TESTSOME": { type: "state", color: 0x238636 },
    "MPI_BCAST": { type: "collective", color: 0xd29922 },
    "MPI_REDUCE": { type: "collective", color: 0xd29922 },
    "MPI_ALLREDUCE": { type: "collective", color: 0xd29922 },
    "MPI_GATHER": { type: "collective", color: 0xd29922 },
    "MPI_SCATTER": { type: "collective", color: 0xd29922 },
    "MPI_ALLGATHER": { type: "collective", color: 0xd29922 },
    "MPI_BARRIER": { type: "collective", color: 0xd29922 }
};
const DEFAULT_CATEGORY = { type: "unknown", color: 0x8b949e };

window.VisualiserCore = {
    scene: null, camera: null, renderer: null, controls: null,
    nodeMap: new Map(), rankMap: new Map(), rankToNodeGroup: new Map(),
    activeLines: [], junctionPoints: [], dynamicCells: {},
    activelyGlowingRanks: new Map(), 
    defaultRankColor: new THREE.Color(0x4b5563),
    sharedMaterials: {},
    tooltipEl: null,
    
    // UI/Camera State
    uiMediaRecorder: null, uiRecordedChunks: [], uiStream: null,
    defaultCameraPose: null, selectedObject: null, isFollowEnabled: false,
    followOffset: new THREE.Vector3(0, 15, 40), desiredCam: new THREE.Vector3(),
    nodeOriginalPos: new Map(), currentLayoutMode: "blueprint",

    init: function(containerId) {
        const container = document.getElementById(containerId);
        this.scene = new THREE.Scene();
        this.scene.fog = new THREE.FogExp2(0x0d1117, 0.001);

        this.camera = new THREE.PerspectiveCamera(60, container.clientWidth / container.clientHeight, 0.1, 2000);    
        this.camera.position.set(0, 100, 300);

        this.renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true, logarithmicDepthBuffer: true });
        this.renderer.setSize(container.clientWidth, container.clientHeight);
        this.renderer.setClearColor(0x0d1117, 1);
        this.renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 2));
        container.appendChild(this.renderer.domElement);

        this.controls = new THREE.OrbitControls(this.camera, this.renderer.domElement);
        this.controls.enableDamping = true;
        this.controls.dampingFactor = 0.05;
        this.controls.keyPanSpeed = 20.0; 

        // Shared Geometries
        this.sharedSphereGeo = new THREE.SphereGeometry(0.04, 8, 8);
        this.sharedArrowGeo = new THREE.ConeGeometry(0.08, 0.25, 8);
        this.sharedArrowGeo.translate(0, -0.125, 0); 
        this.sharedArrowGeo.rotateX(Math.PI / 2);

        const grid = new THREE.GridHelper(1000, 50, 0x30363d, 0x21262d);
        grid.position.y = -10;
        grid.name = "floorGrid";
        this.scene.add(grid);

        this.scene.add(new THREE.AmbientLight(0xffffff, 0.8));
        const dirLight = new THREE.DirectionalLight(0xffffff, 1.0); 
        dirLight.position.set(200, 500, 300);
        this.scene.add(dirLight);

        this.initSharedMaterials();
        this.initTooltip();
        this.bindEvents();
        this.initSavedViewsUI();
        this.initLegend();

        this.animate();
    },

    bindEvents: function() {
        window.addEventListener("resize", () => {
            const container = this.renderer.domElement.parentElement;
            this.camera.aspect = container.clientWidth / container.clientHeight;
            this.camera.updateProjectionMatrix();
            this.renderer.setSize(container.clientWidth, container.clientHeight);
        });

        this.renderer.domElement.addEventListener('click', (e) => this.onCanvasClick(e), false);
        this.renderer.domElement.addEventListener('mousemove', (e) => this.onMouseMove(e), false);

        document.getElementById("btn-rec-ui-start")?.addEventListener("click", () => this.startUIRecording());
        document.getElementById("btn-rec-ui-stop")?.addEventListener("click", () => this.stopUIRecording());
        document.getElementById("btn-view-iso")?.addEventListener("click", () => this.viewPreset("iso"));
        document.getElementById("btn-view-top")?.addEventListener("click", () => this.viewPreset("top"));
        document.getElementById("btn-view-front")?.addEventListener("click", () => this.viewPreset("front"));
        document.getElementById("btn-view-side")?.addEventListener("click", () => this.viewPreset("side"));
        document.getElementById("btn-view-reset")?.addEventListener("click", () => this.resetView());
        document.getElementById("chk-follow")?.addEventListener("change", (e) => this.setFollowEnabled(e.target.checked));
        document.getElementById("layoutMode")?.addEventListener("change", (e) => this.applyLayout(e.target.value));
        document.getElementById("btn-view-save")?.addEventListener("click", () => this.saveCurrentView());
        document.getElementById("btn-view-load")?.addEventListener("click", () => this.loadSelectedView());
        document.getElementById("btn-view-del")?.addEventListener("click", () => this.deleteSelectedView());
    },

    initSharedMaterials: function() {
        Object.keys(MPI_CATEGORIES).forEach(call => {
            const cat = MPI_CATEGORIES[call];
            this.sharedMaterials[call + "_line"] = new THREE.LineBasicMaterial({ color: cat.color, transparent: true, opacity: 0.5, blending: THREE.AdditiveBlending, depthWrite: false, depthTest: false });
            this.sharedMaterials[call + "_tube"] = new THREE.MeshLambertMaterial({ color: cat.color });
            this.sharedMaterials[call + "_junction"] = new THREE.MeshLambertMaterial({ color: cat.color });
        });
        this.sharedMaterials["default_tube"] = new THREE.MeshLambertMaterial({ color: DEFAULT_CATEGORY.color });
        this.sharedMaterials["default_junction"] = new THREE.MeshLambertMaterial({ color: DEFAULT_CATEGORY.color });
    },

    animate: function() {
        requestAnimationFrame(() => this.animate());

        if (this.isFollowEnabled) this.updateFollowCamera();
        else this.controls.update();

        if (this.activelyGlowingRanks.size > 0) {
            this.activelyGlowingRanks.forEach((state, rankId) => {
                state.intensity -= 0.02;
                if (state.intensity <= 0) {
                    state.mesh.setColorAt(state.instanceId, this.defaultRankColor);
                    this.activelyGlowingRanks.delete(rankId);
                } else {
                    const c = this.defaultRankColor.clone().lerp(state.color, state.intensity);
                    state.mesh.setColorAt(state.instanceId, c);
                }
                state.mesh.instanceColor.needsUpdate = true;
            });
        }

        if (window.Analytics3D) Analytics3D.update(performance.now());
        this.renderer.render(this.scene, this.camera);
    },

    clearTopology: function() {
        const geoms = new Set(), mats = new Set();
        const collect = (root) => {
            root.traverse(obj => {
                if (obj.geometry) geoms.add(obj.geometry);
                if (obj.material) {
                    if (Array.isArray(obj.material)) obj.material.forEach(m => mats.add(m));
                    else mats.add(obj.material);
                }
            });
        };

        this.nodeMap.forEach(group => { collect(group); this.scene.remove(group); });
        this.nodeMap.clear();
        this.rankMap.clear();
        this.activelyGlowingRanks.clear();

        const toRemove = [];
        this.scene.traverse(obj => { if (obj.name === "cabinetBox" || obj.name === "groupBox") toRemove.push(obj); });
        toRemove.forEach(obj => { collect(obj); this.scene.remove(obj); });

        this.clearLines();
        geoms.forEach(g => g.dispose());
        mats.forEach(m => m.dispose());

        this.nodeOriginalPos.clear();
        this.setSelectedObject(null);
        this.setFollowEnabled(false);
    },

    // ---------------------------------------------------------
    // TOPOLOGY BUILDER (Accepts decoupled data)
    // ---------------------------------------------------------
    buildTopology: function(hardwareBlueprint, topologyData, metadata) {
        this.clearTopology();
        const nodesMap = {};
        const groupBoxes = [];
        const boxW = 7, boxH = 11, boxD = 9;

        if (hardwareBlueprint) {
            const bp = hardwareBlueprint;
            let currentCabX = 0;
            if (bp.cabinets && Array.isArray(bp.cabinets)) {
                bp.cabinets.forEach(cabinet => {
                    let currentRackX = currentCabX;
                    if (cabinet.racks && Array.isArray(cabinet.racks)) {
                        cabinet.racks.forEach(rack => {
                            let rMinX = Infinity, rMaxX = -Infinity, rMinY = Infinity, rMaxY = -Infinity;
                            if (rack.blades && Array.isArray(rack.blades)) {
                                rack.blades.forEach((blade, bIdx) => {
                                    let bMinX = Infinity, bMaxX = -Infinity, bMinY = Infinity, bMaxY = -Infinity;
                                    if (blade.nodes && Array.isArray(blade.nodes)) {
                                        blade.nodes.forEach((node, nIdx) => {
                                            const absoluteX = currentRackX + (nIdx * 8); 
                                            const absoluteY = bIdx * 12; 
                                            nodesMap[node.hostname] = { x: absoluteX, y: absoluteY, z: 0, ranks: [], cpus: node.cpus || 1, coresPerCpu: node.cores_per_cpu || 1 };
                                            if (absoluteX < bMinX) bMinX = absoluteX; if (absoluteX > bMaxX) bMaxX = absoluteX;
                                            if (absoluteY < bMinY) bMinY = absoluteY; if (absoluteY > bMaxY) bMaxY = absoluteY;
                                            if (absoluteX < rMinX) rMinX = absoluteX; if (absoluteX > rMaxX) rMaxX = absoluteX;
                                            if (absoluteY < rMinY) rMinY = absoluteY; if (absoluteY > rMaxY) rMaxY = absoluteY;
                                        });
                                    }
                                    if (bMinX !== Infinity) groupBoxes.push({ type: 'blade', x: bMinX + (bMaxX - bMinX) / 2, y: bMinY, z: 0, w: (bMaxX - bMinX) + boxW + 1.5, h: boxH + 1.5, d: boxD + 1.5 });
                                });
                            }
                            if (rMinX !== Infinity) {
                                groupBoxes.push({ type: 'chassis', x: rMinX + (rMaxX - rMinX) / 2, y: rMinY + (rMaxY - rMinY) / 2, z: 0, w: (rMaxX - rMinX) + boxW + 5, h: (rMaxY - rMinY) + boxH + 5, d: boxD + 5 });
                                currentRackX += (rMaxX - rMinX) + 25; 
                            }
                        });
                    }
                    currentCabX = currentRackX + 40; 
                });
            } else {
                Object.keys(bp).forEach(host => {
                    if (host !== "cabinets" && host !== "metadata") nodesMap[host] = { ranks: [], x: bp[host].x || 0, y: bp[host].y || 0, z: bp[host].z || 0, cpus: bp[host].cpus || 1, coresPerCpu: bp[host].cores_per_cpu || 1 };
                });
            }
        }

        groupBoxes.forEach(g => {
            const geo = new THREE.BoxGeometry(g.w, g.h, g.d);
            const mat = new THREE.LineBasicMaterial({ color: g.type === 'blade' ? 0x4b5563 : 0x8b949e, transparent: true, opacity: g.type === 'blade' ? 0.2 : 0.4 });
            const mesh = new THREE.LineSegments(new THREE.EdgesGeometry(geo), mat);
            mesh.name = "groupBox"; 
            mesh.userData = { isGrouping: true, type: g.type };
            mesh.position.set(g.x, g.y, g.z);
            this.scene.add(mesh);
        }); 

        if (topologyData && Array.isArray(topologyData)) {
            topologyData.forEach(t => {
                const host = t.hostname || "unknown";
                if (!nodesMap[host]) nodesMap[host] = { x: t.x || 0, y: t.y || 0, z: t.z || 0, ranks: [], cpus: 1, coresPerCpu: 1 };
                if (t.rank !== undefined && t.rank !== null && !nodesMap[host].ranks.find(r => r.id === t.rank)) {
                    nodesMap[host].ranks.push({ id: t.rank, chip: t.chip !== undefined ? t.chip : 0, core: t.core !== undefined ? t.core : nodesMap[host].ranks.length });
                }
            });
        }

        let maxY = 0, minX = Infinity, maxX = -Infinity, totalNodes = 0;
        const sharedNodeGeo = new THREE.BoxGeometry(boxW, boxH, boxD);
        const sharedNodeEdges = new THREE.EdgesGeometry(sharedNodeGeo);
        const sharedActiveNodeMat = new THREE.LineBasicMaterial({ color: 0x58a6ff, transparent: true, opacity: 0.8 }); 
        const sharedIdleNodeMat = new THREE.LineBasicMaterial({ color: 0x4b5563, transparent: true, opacity: 0.2 });   
        const sharedFillGeo = new THREE.BoxGeometry(boxW - 0.2, boxH - 0.2, boxD - 0.2);
        const sharedFillMat = new THREE.MeshBasicMaterial({ color: 0x161b22, transparent: true, opacity: 0.9, polygonOffset: true, polygonOffsetFactor: 1, polygonOffsetUnits: 1 });
        const sharedChipMat = new THREE.MeshBasicMaterial({ color: 0x21262d, transparent: true, opacity: 0.8 });
        const sharedIdleCoreMat = new THREE.MeshBasicMaterial({ color: 0x6e7681, transparent: true, opacity: 0.6 });
        const sharedActiveRankMat = new THREE.MeshBasicMaterial({ color: 0xffffff }); 
        const dummy = new THREE.Object3D();

        Object.keys(nodesMap).forEach(host => {
            const nData = nodesMap[host];
            if (nData.y > maxY) maxY = nData.y;
            if (nData.x < minX) minX = nData.x;
            if (nData.x > maxX) maxX = nData.x;
            totalNodes++;

            const nodeGroup = new THREE.Group();
            nodeGroup.position.set(nData.x, nData.y, nData.z);
            this.scene.add(nodeGroup);
            nData.group = nodeGroup;
            this.rankToNodeGroup.set(host, nodeGroup);
            this.nodeMap.set(host, nodeGroup);

            nodeGroup.userData = { host: host };  
            this.nodeOriginalPos.set(host, nodeGroup.position.clone());

            const shellMesh = new THREE.LineSegments(sharedNodeEdges, nData.ranks.length > 0 ? sharedActiveNodeMat : sharedIdleNodeMat);
            shellMesh.name = "mpiNode";
            shellMesh.userData = { hostname: host, isNode: true };
            nodeGroup.add(shellMesh);

            const fillMesh = new THREE.Mesh(sharedFillGeo, sharedFillMat);
            fillMesh.userData = { hostname: host, isNode: true };
            nodeGroup.add(fillMesh);

            const numChips = nData.cpus || 1, numCores = nData.coresPerCpu || 1;
            const ranks = nData.ranks.sort((a, b) => a.id - b.id);
            const activeCoreCount = ranks.length, idleCoreCount = (numChips * numCores) - activeCoreCount;

            const chipCols = Math.ceil(Math.sqrt(numChips)), chipRows = Math.ceil(numChips / chipCols);
            const chipSpaceX = (boxW - 0.5) / chipCols, chipSpaceY = (boxH - 0.5) / chipRows;
            const coreCols = Math.ceil(Math.sqrt(numCores)), coreRows = Math.ceil(numCores / coreCols);
            const coreSpacing = Math.min((chipSpaceX * 0.90) / coreCols, (chipSpaceY * 0.90) / coreRows);
            const coreSize = coreSpacing * 0.80; 

            const chipGeo = new THREE.BoxGeometry(chipSpaceX * 0.9, chipSpaceY * 0.9, 0.2);
            const coreGeo = new THREE.BoxGeometry(coreSize, coreSize, coreSize);

            let instActiveCores = null, instIdleCores = null;
            let instChips = new THREE.InstancedMesh(chipGeo, sharedChipMat, numChips);
            nodeGroup.add(instChips);

            const activeRankData = {}; 
            if (activeCoreCount > 0) {
                instActiveCores = new THREE.InstancedMesh(coreGeo, sharedActiveRankMat.clone(), activeCoreCount);
                instActiveCores.instanceMatrix.setUsage(THREE.DynamicDrawUsage);
                instActiveCores.instanceColor = new THREE.InstancedBufferAttribute(new Float32Array(activeCoreCount * 3), 3);
                for(let i=0; i<activeCoreCount; i++) instActiveCores.setColorAt(i, this.defaultRankColor);
                instActiveCores.userData = { isInstancedCore: true, host: host, rankData: activeRankData };
                nodeGroup.add(instActiveCores);
            }
            if (idleCoreCount > 0) {
                instIdleCores = new THREE.InstancedMesh(coreGeo, sharedIdleCoreMat, idleCoreCount);
                nodeGroup.add(instIdleCores);
            }

            let chipIdx = 0, activeIdx = 0, idleIdx = 0;
            for (let c = 0; c < numChips; c++) {
                const chipOffsetX = ((c % chipCols) * chipSpaceX) - ((chipCols - 1) * chipSpaceX) / 2;
                const chipOffsetY = -(Math.floor(c / chipCols) * chipSpaceY) + ((chipRows - 1) * chipSpaceY) / 2;
                const chipOffsetZ = (boxD / 2) - 0.3; 

                dummy.position.set(chipOffsetX, chipOffsetY, chipOffsetZ);
                dummy.updateMatrix();
                instChips.setMatrixAt(chipIdx++, dummy.matrix);

                const startX = chipOffsetX - ((coreCols * coreSpacing) / 2) + (coreSpacing / 2);
                const startY = chipOffsetY + ((coreRows * coreSpacing) / 2) - (coreSpacing / 2); 

                for (let i = 0; i < numCores; i++) {
                    const activeRank = ranks[(c * numCores) + i];
                    dummy.position.set(startX + ((i % coreCols) * coreSpacing), startY - (Math.floor(i / coreCols) * coreSpacing), chipOffsetZ + 0.15);
                    dummy.updateMatrix();

                    if (activeRank && instActiveCores) {
                        instActiveCores.setMatrixAt(activeIdx, dummy.matrix);
                        activeRankData[activeIdx] = { rank: activeRank.id, host: host, chip: c, core: i, isCore: true, localPos: dummy.position.clone() };
                        this.rankMap.set(activeRank.id, { mesh: instActiveCores, instanceId: activeIdx, nodeGroup: nodeGroup, localPos: dummy.position.clone(), depth: coreSize }); 
                        activeIdx++;
                    } else if (instIdleCores) {
                        instIdleCores.setMatrixAt(idleIdx++, dummy.matrix);
                    }
                }
            }
        });

        if (totalNodes > 0) {
            const cabWidth = (maxX - minX), cabCenterX = minX + cabWidth / 2, centerY = maxY / 2;
            const floorGrid = this.scene.getObjectByName("floorGrid");
            if (floorGrid) floorGrid.position.set(cabCenterX, -10, 0);       
            this.camera.position.set(cabCenterX, centerY, Math.max(300, cabWidth * 1.5, maxY * 1.5));
            this.controls.target.set(cabCenterX, centerY, 0);
            this.controls.update();
            this.defaultCameraPose = this.getCameraPose();
        }

        if (metadata) this.renderMetadata(metadata, topologyData?.length || 0, Object.keys(nodesMap).length);
    },

    // ---------------------------------------------------------
    // FRAME RENDERER (Accepts Array of Active Events)
    // ---------------------------------------------------------
    renderFrame: function(activeEvents) {
        this.clearLines();
        const aggregatedConnections = new Map();

        activeEvents.forEach(event => {
            const cat = MPI_CATEGORIES[event.call] || MPI_CATEGORIES[event.message_type] || DEFAULT_CATEGORY; // Support old .call and new .message_type
            const callType = event.call || event.message_type;
            const sData = this.rankMap.get(event.sender);
            const rData = this.rankMap.get(event.receiver);

            if (callType !== "MPI_WAIT" && callType !== "MPI_WAITALL") {
                const color = new THREE.Color(cat.color);
                if (sData) {
                    this.activelyGlowingRanks.set(event.sender, { mesh: sData.mesh, instanceId: sData.instanceId, color: color, intensity: 1.0 });
                    sData.mesh.setColorAt(sData.instanceId, color);
                    sData.mesh.instanceColor.needsUpdate = true;
                }
                if (rData) {
                    this.activelyGlowingRanks.set(event.receiver, { mesh: rData.mesh, instanceId: rData.instanceId, color: color, intensity: 1.0 });
                    rData.mesh.setColorAt(rData.instanceId, color);
                    rData.mesh.instanceColor.needsUpdate = true;
                }
            }

            if (event.sender === event.receiver) return;

            let connectionKey = (cat.type === "collective" && sData && rData) 
                ? `coll-${sData.nodeGroup.uuid}-${rData.nodeGroup.uuid}-${callType}`
                : `${event.sender}-${event.receiver}-${callType}`;

            if (!aggregatedConnections.has(connectionKey)) {
                aggregatedConnections.set(connectionKey, { sender: event.sender, receiver: event.receiver, call: callType, count: 1, sData: sData, rData: rData });
            } else {
                aggregatedConnections.get(connectionKey).count++;
            }
        });

        aggregatedConnections.forEach(conn => {
            const { sender, receiver, call, count, sData, rData } = conn;
            const cat = MPI_CATEGORIES[call] || DEFAULT_CATEGORY;

            if (sData && rData) {
                if (sData.nodeGroup === rData.nodeGroup) {
                    const startWorld = sData.localPos.clone().applyMatrix4(sData.nodeGroup.matrixWorld);
                    const endWorld = rData.localPos.clone().applyMatrix4(rData.nodeGroup.matrixWorld);
                    startWorld.z += (sData.depth / 2) + 0.5; endWorld.z += (rData.depth / 2) + 0.5;
                    this.drawIntraNodeLine(startWorld, endWorld, call, count);
                } else {
                    const startWorld = (cat.type === "collective") ? sData.nodeGroup.position.clone() : sData.localPos.clone().applyMatrix4(sData.nodeGroup.matrixWorld);
                    const endWorld = (cat.type === "collective") ? rData.nodeGroup.position.clone() : rData.localPos.clone().applyMatrix4(rData.nodeGroup.matrixWorld);
                    startWorld.z += (cat.type === "collective") ? 5.5 : (sData.depth / 2) + 0.5;
                    endWorld.z += (cat.type === "collective") ? 5.5 : (rData.depth / 2) + 0.5;
                    this.drawInterNodeLine(startWorld, endWorld, call, sender, receiver, count);
                }
            }
        });
    },

    drawIntraNodeLine: function(startPos, endPos, callName, msgCount) {
        const midPoint = startPos.clone().lerp(endPos, 0.5);
        midPoint.z += Math.max(startPos.distanceTo(endPos) * 0.4, 1.0); 
        const thick = 1 + Math.log10(msgCount);
        const curve = new THREE.QuadraticBezierCurve3(startPos, midPoint, endPos);
        const tube = new THREE.Mesh(new THREE.TubeGeometry(curve, 10, 0.04 * thick, 4, false), this.sharedMaterials[callName + "_tube"] || this.sharedMaterials["default_tube"]);
        this.scene.add(tube);
        this.activeLines.push(tube);
        this.createJunction(startPos, callName, thick);
        this.createArrow(endPos, curve.getTangent(1.0).normalize(), callName, thick);
    },

    drawInterNodeLine: function(startPos, endPos, callName, sender, receiver, msgCount) {
        const cat = MPI_CATEGORIES[callName] || DEFAULT_CATEGORY;
        const midPoint = startPos.clone().lerp(endPos, 0.5);
        const bow = Math.max(startPos.distanceTo(endPos) * 0.3, 8.0); 
        const lane = (sender > receiver) ? 3.0 : -3.0;

        if (cat.type === "collective") { midPoint.z += bow * 1.5; } 
        else if (cat.type === "p2p_nonblock") { midPoint.z += bow; midPoint.y += lane; } 
        else { midPoint.z += bow; midPoint.y -= lane; }

        const thick = 1 + Math.log10(msgCount);
        const curve = new THREE.QuadraticBezierCurve3(startPos, midPoint, endPos);
        const tube = new THREE.Mesh(new THREE.TubeGeometry(curve, 12, 0.12 * thick, 4, false), this.sharedMaterials[callName + "_tube"] || this.sharedMaterials["default_tube"]);
        this.scene.add(tube);
        this.activeLines.push(tube);
        this.createJunction(startPos, callName, thick);
        this.createArrow(endPos, curve.getTangent(1.0).normalize(), callName, thick);
    },

    createJunction: function(pos, callName, scale) {
        const mesh = new THREE.Mesh(this.sharedSphereGeo, this.sharedMaterials[callName + "_junction"] || this.sharedMaterials["default_junction"]);
        mesh.position.copy(pos); mesh.scale.set(scale, scale, scale);
        this.scene.add(mesh); this.junctionPoints.push(mesh);
    },

    createArrow: function(pos, dir, callName, scale) {
        const mesh = new THREE.Mesh(this.sharedArrowGeo, this.sharedMaterials[callName + "_junction"] || this.sharedMaterials["default_junction"]);
        mesh.position.copy(pos); mesh.lookAt(pos.clone().add(dir)); mesh.scale.set(scale, scale, scale);
        this.scene.add(mesh); this.junctionPoints.push(mesh);
    },

    clearLines: function() {
        this.activeLines.forEach(l => { if (l.geometry) l.geometry.dispose(); this.scene.remove(l); });
        this.activeLines = [];
        this.junctionPoints.forEach(pt => this.scene.remove(pt));
        this.junctionPoints = [];
    },

    // ---------------------------------------------------------
    // UI, HTML OVERLAYS & CAMERA 
    // ---------------------------------------------------------
    initTooltip: function() {
        if (!document.getElementById("mpiTooltip")) {
            this.tooltipEl = document.createElement('div');
            this.tooltipEl.id = "mpiTooltip";
            this.tooltipEl.style.cssText = "position:absolute;display:none;pointer-events:none;background:rgba(22,27,34,0.95);border:1px solid #58a6ff;border-radius:6px;padding:10px 15px;color:#c9d1d9;font-family:'Fira Code', monospace;font-size:0.85rem;z-index:2000;box-shadow:0 4px 12px rgba(0,0,0,0.8);";
            document.body.appendChild(this.tooltipEl);
        } else this.tooltipEl = document.getElementById("mpiTooltip");
    },

    onMouseMove: function(event) {
        if (!this.tooltipEl) return;
        const rect = this.renderer.domElement.getBoundingClientRect();
        const raycaster = new THREE.Raycaster();
        raycaster.setFromCamera({ x: ((event.clientX - rect.left) / rect.width) * 2 - 1, y: -((event.clientY - rect.top) / rect.height) * 2 + 1 }, this.camera);
        const intersects = raycaster.intersectObjects(this.scene.children, true);

        let hoveredRank = null, hoveredNode = null;
        for (let i = 0; i < intersects.length; i++) {
            const obj = intersects[i].object;
            if (obj.userData?.isInstancedCore) { hoveredRank = obj.userData.rankData[intersects[i].instanceId]; break; } 
            else if (obj.userData?.isNode && !hoveredNode) { hoveredNode = obj.userData; }
        }

        const data = hoveredRank || hoveredNode;
        if (data) {
            this.tooltipEl.innerHTML = data.isCore 
                ? `<strong style="color:#58a6ff;font-size:1rem;">Rank ${data.rank}</strong><hr style="border:0;border-top:1px solid #30363d;margin:6px 0;"><span style="color:#8b949e;">Host:</span> ${data.host}<br/><span style="color:#8b949e;">Chip:</span> ${data.chip} | <span style="color:#8b949e;">Core:</span> ${data.core}`
                : `<strong style="color:#8b949e;font-size:1rem;">Node Chassis</strong><hr style="border:0;border-top:1px solid #30363d;margin:6px 0;"><span style="color:#8b949e;">Host:</span> <span style="color:#c9d1d9;">${data.hostname}</span>`;
            this.tooltipEl.style.display = 'block';
            this.tooltipEl.style.left = (event.clientX + 15) + 'px'; 
            this.tooltipEl.style.top = (event.clientY + 15) + 'px';
            document.body.style.cursor = 'pointer';
        } else {
            this.tooltipEl.style.display = 'none';
            document.body.style.cursor = 'default';
        }
    },

    onCanvasClick: function(event) {
        const rect = this.renderer.domElement.getBoundingClientRect();
        const raycaster = new THREE.Raycaster();
        raycaster.setFromCamera({ x: ((event.clientX - rect.left) / rect.width) * 2 - 1, y: -((event.clientY - rect.top) / rect.height) * 2 + 1 }, this.camera);
        const intersects = raycaster.intersectObjects(this.scene.children, true);

        for (let i = 0; i < intersects.length; i++) {
            const object = intersects[i].object;
            if (object.userData?.isInstancedCore) {
                const data = object.userData.rankData[intersects[i].instanceId];
                const targetPos = data.localPos.clone().add(object.parent.position);
                this.setSelectedObject({ userData: data, worldPos: targetPos, name: `Rank ${data.rank}` });
                this.flyCameraTo(targetPos); break;
            } else if (object.name === "mpiNode") {
                this.setSelectedObject(object.parent);
                const targetPos = new THREE.Vector3(); object.parent.getWorldPosition(targetPos);
                this.flyCameraTo(targetPos); break;
            }
        }
    },

    flyCameraTo: function(targetPos) {
        this.controls.enabled = false;
        const finalCam = targetPos.clone().add(new THREE.Vector3(0, 15, 40));
        const startTarget = this.controls.target.clone(), startCam = this.camera.position.clone();
        const start = performance.now();
        const step = (time) => {
            let t = Math.min(1, (time - start) / 800);
            const easeT = 1 - Math.pow(1 - t, 3);
            this.camera.position.lerpVectors(startCam, finalCam, easeT);
            this.controls.target.lerpVectors(startTarget, targetPos, easeT);
            if (t < 1) requestAnimationFrame(step);
            else { this.controls.enabled = !this.isFollowEnabled; this.controls.update(); }
        };
        requestAnimationFrame(step);
    },

    // [Keep the rest of your original Camera/Layout/SavedViews logic exactly the same, just prepend `this.`]
    getCameraPose: function() { return { position: this.camera.position.clone(), target: this.controls.target.clone(), up: this.camera.up.clone() }; },
    setCameraPose: function(pose) { if(pose.up) this.camera.up.copy(pose.up); this.camera.position.copy(pose.position); this.controls.target.copy(pose.target); this.controls.update(); },
    animateCameraPose: function(toPose, dur=650) {
        const from = this.getCameraPose(), start = performance.now();
        const step = (t) => {
            const u = Math.min(1, (t-start)/dur), ease = 1 - Math.pow(1-u, 3);
            this.camera.position.lerpVectors(from.position, toPose.position, ease);
            this.controls.target.lerpVectors(from.target, toPose.target, ease);
            if (toPose.up) this.camera.up.copy(from.up.clone().lerp(toPose.up, ease).normalize());
            this.controls.update();
            if (u < 1) requestAnimationFrame(step);
        };
        requestAnimationFrame(step);
    },
    viewPreset: function(name) {
        const box = new THREE.Box3();
        this.nodeMap.forEach(group => box.expandByObject(group));
        const center = new THREE.Vector3(), size = new THREE.Vector3(); box.getCenter(center); box.getSize(size);
        const dist = Math.max(size.length()*0.5, 10) * 2.2;
        const pose = { target: center, position: new THREE.Vector3(), up: new THREE.Vector3(0,1,0) };
        if (name==="iso") { pose.position.copy(center).add(new THREE.Vector3(1,0.7,1).normalize().multiplyScalar(dist)); }
        else if (name==="top") { pose.position.copy(center).add(new THREE.Vector3(0,1,0).multiplyScalar(dist)); pose.up.set(0,0,-1); }
        else if (name==="front") { pose.position.copy(center).add(new THREE.Vector3(0,0.2,1).normalize().multiplyScalar(dist)); }
        else if (name==="side") { pose.position.copy(center).add(new THREE.Vector3(1,0.2,0).normalize().multiplyScalar(dist)); }
        this.animateCameraPose(pose, 650);
    },
    resetView: function() { if (this.defaultCameraPose) this.animateCameraPose(this.defaultCameraPose, 650); },
    setSelectedObject: function(obj) {
        this.selectedObject = obj || null;
        const status = document.getElementById("camStatus");
        if (!status) return;
        if (!obj) { status.textContent = ""; return; }
        if (obj.userData?.rank !== undefined) status.textContent = `Selected rank ${obj.userData.rank} @ ${obj.userData.host}`;
        else if (obj.userData?.hostname) status.textContent = `Selected node ${obj.userData.hostname}`;
        else status.textContent = `Selected: ${obj.name || "object"}`;
    },
    setFollowEnabled: function(enabled) {
        this.isFollowEnabled = !!enabled; this.controls.enabled = !this.isFollowEnabled;
        if (this.isFollowEnabled) this.followOffset.copy(this.camera.position).sub(this.controls.target);
    },
    updateFollowCamera: function() {
        if (!this.isFollowEnabled || !this.selectedObject) return;
        const targetPos = new THREE.Vector3();
        if (this.selectedObject.userData?.isCore) targetPos.copy(this.selectedObject.worldPos);
        else this.selectedObject.getWorldPosition(targetPos);
        this.controls.target.lerp(targetPos, 0.18);
        this.desiredCam.copy(this.controls.target).add(this.followOffset);
        this.camera.position.lerp(this.desiredCam, 0.18);
        this.controls.update();
    },
    applyLayout: function(mode) {
        this.currentLayoutMode = mode;
        const targets = new Map(), hostnames = Array.from(this.nodeMap.keys());
        hostnames.forEach((hostname, idx) => {
            const orig = this.nodeOriginalPos.get(hostname) || new THREE.Vector3();
            if (mode === "blueprint") targets.set(hostname, orig.clone());
            else if (mode === "topdown") targets.set(hostname, new THREE.Vector3(orig.x, 0, orig.z));
            else if (mode === "rackfront") targets.set(hostname, new THREE.Vector3(orig.x, orig.y, 0));
            else if (mode === "line") targets.set(hostname, new THREE.Vector3((idx - hostnames.length / 2) * 18, 0, 0));
        });
        this.scene.traverse(obj => { if (obj.name === "cabinetBox" || obj.name === "groupBox") obj.visible = (mode === "blueprint"); });
        
        const start = performance.now(), startPos = new Map();
        this.nodeMap.forEach((g, h) => startPos.set(h, g.position.clone()));
        const step = (t) => {
            const u = Math.min(1, (t - start) / 650), ease = 1 - Math.pow(1 - u, 3);
            this.nodeMap.forEach((g, h) => g.position.lerpVectors(startPos.get(h), targets.get(h), ease));
            if (u < 1) requestAnimationFrame(step);
        };
        requestAnimationFrame(step);
        setTimeout(() => {
            if (mode === "topdown") this.viewPreset("top"); else if (mode === "rackfront" || mode === "line") this.viewPreset("front"); else this.resetView();
            if (window.Analytics3D) Analytics3D.refreshHighlights();
        }, 680);
    },
    initSavedViewsUI: function() {
        const sel = document.getElementById("savedViews"); if (!sel) return;
        sel.innerHTML = "";
        const views = JSON.parse(localStorage.getItem("mpiVis.savedViews.v1") || "{}");
        const names = Object.keys(views).sort();
        if (names.length === 0) { sel.appendChild(new Option("(no saved views)", "")); sel.disabled = true; return; }
        sel.disabled = false; names.forEach(n => sel.appendChild(new Option(n, n)));
    },
    saveCurrentView: function() {
        const name = (document.getElementById("viewName")?.value || "").trim();
        if (!name) return alert("Enter a view name.");
        const views = JSON.parse(localStorage.getItem("mpiVis.savedViews.v1") || "{}");
        const p = this.getCameraPose();
        views[name] = { position: p.position.toArray(), target: p.target.toArray(), up: p.up.toArray(), layoutMode: this.currentLayoutMode };
        localStorage.setItem("mpiVis.savedViews.v1", JSON.stringify(views));
        this.initSavedViewsUI();
    },
    loadSelectedView: function() {
        const name = document.getElementById("savedViews")?.value; if (!name) return;
        const v = JSON.parse(localStorage.getItem("mpiVis.savedViews.v1") || "{}")[name]; if (!v) return;
        if (v.layoutMode) this.applyLayout(v.layoutMode);
        setTimeout(() => this.animateCameraPose({ position: new THREE.Vector3().fromArray(v.position), target: new THREE.Vector3().fromArray(v.target), up: new THREE.Vector3().fromArray(v.up) }, 650), 680);
    },
    deleteSelectedView: function() {
        const name = document.getElementById("savedViews")?.value; if (!name) return;
        const views = JSON.parse(localStorage.getItem("mpiVis.savedViews.v1") || "{}"); delete views[name];
        localStorage.setItem("mpiVis.savedViews.v1", JSON.stringify(views)); this.initSavedViewsUI();
    },

    // HTML Generators
    renderMetadata: function(meta, ranks, nodes) {
        const c = document.getElementById("metadataContainer");
        if (c) c.innerHTML = `<div style="font-size:0.75rem;color:#8b949e;font-weight:bold;text-transform:uppercase;letter-spacing:1px;margin-bottom:10px;">Run Metadata</div><div style="color:#c9d1d9;font-family:'Fira Code', monospace;font-size:0.85rem;line-height:1.6;"><div><span style="color:#58a6ff;">Program:</span> ${meta.program||"Unknown"}</div><div><span style="color:#58a6ff;">Date:</span> ${meta.date||"Unknown"}</div><div><span style="color:#58a6ff;">System:</span> ${meta.system_name||"Unknown"}</div><div><span style="color:#58a6ff;">Scale:</span> ${ranks} Ranks across ${nodes} Nodes</div></div>`;
    },
    initLegend: function() {
        if (document.getElementById("mpiLegend") || !document.getElementById("visCanvas")) return;
        const leg = document.createElement("div"); leg.id = "mpiLegend";
        leg.style.cssText = "position:absolute;bottom:20px;right:20px;background:rgba(22,27,34,0.85);border:1px solid #30363d;border-radius:8px;padding:12px 18px;color:#c9d1d9;font-family:'Fira Code', monospace;font-size:0.8rem;z-index:1000;pointer-events:none;box-shadow:0 4px 12px rgba(0,0,0,0.5);backdrop-filter:blur(4px);";
        leg.innerHTML = `<div style="margin-bottom:10px;border-bottom:1px solid #30363d;padding-bottom:6px;color:#8b949e;"><strong>COMMUNICATION TYPES</strong></div>`;
        const unique = {}; Object.values(MPI_CATEGORIES).forEach(c => unique[c.type] = c.color);
        const formatName = (str) => ({ "p2p_block": "P2P Blocking", "p2p_nonblock": "P2P Non-Blocking", "state": "Wait / Sync States", "collective": "Collectives" }[str] || str);
        Object.entries(unique).forEach(([t, hex]) => {
            const hexStr = '#' + hex.toString(16).padStart(6, '0');
            leg.innerHTML += `<div style="display:flex;align-items:center;margin-bottom:6px;"><div style="width:12px;height:12px;margin-right:10px;border-radius:2px;background-color:${hexStr};box-shadow:0 0 5px ${hexStr}"></div><span>${formatName(t)}</span></div>`;
        });
        document.getElementById("visCanvas").style.position = "relative";
        document.getElementById("visCanvas").appendChild(leg);
    },
    initSpectrograms: function(stats) {
        if (!stats) return;
        const renderTbl = (id, dynamic) => {
            const c = document.getElementById(id); if (!c) return; c.innerHTML = "";
            const t = document.createElement('table'); t.style.cssText = 'border-collapse:separate;border-spacing:3px;';
            const calls = Object.keys(stats); if (!calls.length) return;
            const bins = Object.keys(stats[calls[0]]);
            let max = 0; calls.forEach(call => Object.values(stats[call]).forEach(v => { if (v > max) max = v; }));
            calls.forEach(call => {
                const tr = document.createElement('tr');
                tr.innerHTML = `<td style="color:#8b949e;text-align:right;padding-right:8px;">${call.replace('MPI_', '')}</td>`;
                if (dynamic) this.dynamicCells[call] = {};
                bins.forEach(bin => {
                    const td = document.createElement('td');
                    td.style.cssText = 'border:none;border-radius:3px;height:20px;width:35px;background-color:#161b22;';
                    if (dynamic) {
                        td.style.transition = 'background-color 0.15s ease-out';
                        td.title = `${call} (${bin}): 0 active`;
                        this.dynamicCells[call][bin] = td;
                    } else {
                        const val = stats[call][bin];
                        td.title = `${call} (${bin}): ${val} total`;
                        if (val > 0) td.style.backgroundColor = `rgba(88,166,255,${Math.max(0.15, val/max)})`;
                    }
                    tr.appendChild(td);
                });
                t.appendChild(tr);
            });
            c.appendChild(t);
        };
        renderTbl("overallStatsContainer", false);
        renderTbl("activeStatsContainer", true);
    },
    updateDynamicSpectrogram: function(activeEvents, globalStats) {
        if (!globalStats || !activeEvents || !Object.keys(this.dynamicCells).length) return;
        const calls = Object.keys(globalStats), bins = Object.keys(globalStats[calls[0]]);
        let counts = {}; calls.forEach(c => { counts[c] = {}; bins.forEach(b => counts[c][b] = 0); });
        activeEvents.forEach(e => {
            const c = e.call || e.message_type;
            if (counts[c] !== undefined) {
                const b = e.bytes || 0;
                if (b<128) counts[c]["< 128B"]++; else if (b<1024) counts[c]["128B - 1KB"]++; else if (b<65536) counts[c]["1KB - 64KB"]++;
                else if (b<1048576) counts[c]["64KB - 1MB"]++; else if (b<16777216) counts[c]["1MB - 16MB"]++; else counts[c]["> 16MB"]++;
            }
        });
        let max = 0; calls.forEach(c => bins.forEach(b => { if (globalStats[c][b] > max) max = globalStats[c][b]; }));
        calls.forEach(c => bins.forEach(b => {
            const cnt = counts[c][b], td = this.dynamicCells[c][b];
            if (td.dataset.lastCount == cnt) return; td.dataset.lastCount = cnt;
            td.title = `${c} (${b}): ${cnt} active`;
            td.style.backgroundColor = (cnt === 0) ? '#161b22' : `rgba(46,160,67,${Math.max(0.15, cnt/(max||1))})`;
        }));
    }
};
