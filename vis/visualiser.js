// ==========================================
// GLOBALS & STATE
// ==========================================
let scene, camera, renderer, controls;
let parsedData = null;

// Chunking & Async IO State
let uploadedFilePointer = null;
let currentLoadedChunkIndex = -1;
let headerLengthOffset = 0;
let chunkLoadPromise = null;        // Promise for the currently in-flight chunk load
let chunkLoadIndexInFlight = -1;    // Which chunk index that promise is loading

// Playback State
let currentTime = 0;
let minTime = 0;
let maxTime = 0;
let timeMultiplier = 1;
let isPlaying = false;
let animationFrameId;
let lastFrameTime = 0;
let lastDynUpdate = 0;
let isProcessingFrame = false;

// Raycasting State for click-zoom functionality
const raycaster = new THREE.Raycaster();
const mouse = new THREE.Vector2();

// 3D Maps & Caches
const nodeMap = new Map();  // Maps hostname -> Node Group
const rankMap = new Map();  // Maps rank ID -> Process Mesh
let activeLines = [];       // Currently rendered bezier curves
let junctionPoints = [];    // Active receive/send ports
let dynamicCells = {};      // HTML Table cell references

const rankToNodeGroup = new Map();

// Memory Caches
const sharedMaterials = {};
const sharedSphereGeo = new THREE.SphereGeometry(0.2, 8, 8);

// 3D HOVER TOOLTIPS
let tooltipEl;

// ==========================================
// CONFIGURATION & CATEGORIES
// ==========================================
const MPI_CATEGORIES = {
    //P2P Blocking (Blue)
    "MPI_SEND": { type: "p2p_block", color: 0x58a6ff },
    "MPI_RECV": { type: "p2p_block", color: 0x58a6ff },
    "MPI_BSEND": { type: "p2p_block", color: 0x58a6ff },
    "MPI_SSEND": { type: "p2p_block", color: 0x58a6ff },
    "MPI_RSEND": { type: "p2p_block", color: 0x58a6ff },
    "MPI_SENDRECV": { type: "p2p_block", color: 0x58a6ff },

    // P2P Non-Blocking (Teal)
    "MPI_ISEND": { type: "p2p_nonblock", color: 0x3fb950 },
    "MPI_IRECV": { type: "p2p_nonblock", color: 0x3fb950 },
    "MPI_IBSEND": { type: "p2p_nonblock", color: 0x3fb950 },
    "MPI_ISSEND": { type: "p2p_nonblock", color: 0x3fb950 },
    "MPI_IRSEND": { type: "p2p_nonblock", color: 0x3fb950 },
    
    // States (Dimmer Teal)
    "MPI_WAIT": { type: "state", color: 0x238636 },
    "MPI_WAITALL": { type: "state", color: 0x238636 },

    // Collectives (Orange)
    "MPI_BCAST": { type: "collective", color: 0xd29922 },
    "MPI_REDUCE": { type: "collective", color: 0xd29922 },
    "MPI_ALLREDUCE": { type: "collective", color: 0xd29922 },
    "MPI_GATHER": { type: "collective", color: 0xd29922 },
    "MPI_SCATTER": { type: "collective", color: 0xd29922 },
    "MPI_ALLGATHER": { type: "collective", color: 0xd29922 },
    "MPI_BARRIER": { type: "collective", color: 0xd29922 }
};
const DEFAULT_CATEGORY = { type: "unknown", color: 0x8b949e };

// ==========================================
// INITIALIZATION
// ==========================================
document.addEventListener("DOMContentLoaded", () => {
    initThreeJS();
    
    document.getElementById("profileLoader").addEventListener("change", handleFileUpload);
    
    // Sliders
    document.getElementById("timeSlider").addEventListener("input", (e) => {
        document.getElementById("currentTimeLabel").textContent = parseFloat(e.target.value).toFixed(3);
    });
    document.getElementById("timeSlider").addEventListener("change", handleManualSeek);
    
    document.getElementById("speedSlider").addEventListener("input", (e) => {
        const rawValue = parseFloat(e.target.value);
        const actualSpeed = Math.pow(10, rawValue);
        document.getElementById("speedLabel").textContent = actualSpeed.toFixed(3) + "x";
    });

    document.getElementById("btn-play").addEventListener("click", togglePlayback);
});

window.addEventListener("resize", () => {
  const container = document.getElementById("visCanvas");
  if (!container || !camera || !renderer) return;

  const w = container.clientWidth;
  const h = container.clientHeight;
  if (w <= 0 || h <= 0) return;

  camera.aspect = w / h;
  camera.updateProjectionMatrix();
  renderer.setSize(w, h);
});

function initThreeJS() {
    const container = document.getElementById("visCanvas");
    scene = new THREE.Scene();
    scene.fog = new THREE.FogExp2(0x0d1117, 0.001);

    camera = new THREE.PerspectiveCamera(60, container.clientWidth / container.clientHeight, 0.1, 2000);    
    camera.position.set(0, 100, 300);

    renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true });
    renderer.setSize(container.clientWidth, container.clientHeight);
    renderer.setClearColor(0x0d1117, 1);
    container.appendChild(renderer.domElement);

    controls = new THREE.OrbitControls(camera, renderer.domElement);
    controls.enableDamping = true;
    controls.dampingFactor = 0.05;

    controls.listenToKeyEvents(window);
    controls.keyPanSpeed = 20.0; 

    // Listen for clicks on the 3D canvas
    renderer.domElement.addEventListener('click', onCanvasClick, false);
    
    // Setup and listen for 3D hover tooltips
    initTooltip();
    renderer.domElement.addEventListener('mousemove', onMouseMove, false);
    
    renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 2));

    const grid = new THREE.GridHelper(1000, 50, 0x30363d, 0x21262d);
    grid.position.y = -10;
    scene.add(grid);

    const ambientLight = new THREE.AmbientLight(0xffffff, 0.8); 
    scene.add(ambientLight);
    
    const dirLight = new THREE.DirectionalLight(0xffffff, 1.0); 
    dirLight.position.set(200, 500, 300);
    scene.add(dirLight);

    initSharedMaterials();

    animateThreeJS();
}

function animateThreeJS() {
    requestAnimationFrame(animateThreeJS);
    controls.update();
    
    // Only decay the emissive glow if the timeline is actively playing!
    if (isPlaying) {
        rankMap.forEach(mesh => {
            if (mesh.material.emissiveIntensity > 0) {
                mesh.material.emissiveIntensity -= 0.02; // Fade out over ~50 frames
                if (mesh.material.emissiveIntensity < 0) mesh.material.emissiveIntensity = 0;
            }
        });
    }

    renderer.render(scene, camera);
}

function initSharedMaterials() {
    // Build specific materials for each MPI call
    Object.keys(MPI_CATEGORIES).forEach(call => {
        const cat = MPI_CATEGORIES[call];
        
        sharedMaterials[call + "_line"] = new THREE.LineBasicMaterial({
            color: cat.color,
            transparent: true,
            opacity: 0.5,
            blending: THREE.AdditiveBlending,
            depthWrite: false 
        });
        
        sharedMaterials[call + "_junction"] = new THREE.MeshBasicMaterial({
            color: cat.color
        });
    });

    // Build the fallback defaults
    sharedMaterials["default_line"] = new THREE.LineBasicMaterial({
        color: DEFAULT_CATEGORY.color,
        transparent: true, opacity: 0.5, blending: THREE.AdditiveBlending, depthWrite: false
    });
    sharedMaterials["default_junction"] = new THREE.MeshBasicMaterial({ color: DEFAULT_CATEGORY.color });
}

// ==========================================
// CHUNK LOADING (.mpix)
// ==========================================
async function decompressBlob(blob) {
    const ds = new DecompressionStream('deflate');
    const decompressedStream = blob.stream().pipeThrough(ds);
    return await new Response(decompressedStream).text();
}

async function handleFileUpload(event) {
    uploadedFilePointer = event.target.files[0];
    if (!uploadedFilePointer) return;

    try {
        if (uploadedFilePointer.name.endsWith('.mpix')) {
            const sizeBuf = await uploadedFilePointer.slice(0, 4).arrayBuffer();
            const headerSize = new DataView(sizeBuf).getUint32(0, true);
            headerLengthOffset = 4 + headerSize;

            const headerBlob = uploadedFilePointer.slice(4, headerLengthOffset);
            const headerText = await decompressBlob(headerBlob);
            
            parsedData = JSON.parse(headerText);
            parsedData.timeline = [];
        } else {
            alert("Please upload a packaged .mpix file for large traces.");
            return;
        }
        currentLoadedChunkIndex = -1;
        chunkLoadPromise = null;
        chunkLoadIndexInFlight = -1;
        initDashboard();
    } catch (error) {
        console.error("Failed to unpack:", error);
    }
}

async function ensureChunkLoadedForTime(time) {
    if (!parsedData?.chunks || !uploadedFilePointer) return;

    const chunks = parsedData.chunks;
    if (!Array.isArray(chunks) || chunks.length === 0) return;

    const findTargetIndex = (t) => {
        let idx = chunks.findIndex(c => t <= c.t_end);
        if (idx === -1) idx = chunks.length - 1;
        return idx;
    };

    // Loop handles the case where a different chunk is already loading:
    // we wait for it, then re-check what we need.
    while (true) {
        const targetIndex = findTargetIndex(time);

        // Already have the right chunk in memory
        if (targetIndex === currentLoadedChunkIndex) return;

        // If something is already loading, either wait for the same chunk,
        // or wait for it to finish then try again.
        if (chunkLoadPromise) {
            if (chunkLoadIndexInFlight === targetIndex) {
                await chunkLoadPromise;
                return;
            }
            await chunkLoadPromise;
            continue; // re-evaluate after the other load completes
        }

        // Start loading the required chunk
        const overlay = document.getElementById("loadingOverlay");
        const loadingText = document.getElementById("loadingText");

        chunkLoadIndexInFlight = targetIndex;

        chunkLoadPromise = (async () => {
            try {
                if (overlay) overlay.style.display = "block";
                if (loadingText) {
                    loadingText.textContent = `Unpacking Chunk ${targetIndex + 1} of ${chunks.length}...`;
                }

                // Yield so the browser can paint the overlay/text before heavy work
                await new Promise(resolve => setTimeout(resolve, 0));

                const chunk = chunks[targetIndex];

                const absoluteStart = headerLengthOffset + chunk.offset;
                const absoluteEnd = absoluteStart + chunk.size;

                const chunkBlob = uploadedFilePointer.slice(absoluteStart, absoluteEnd);
                const chunkText = await decompressBlob(chunkBlob);

                const timeline = JSON.parse(chunkText);

                if (Array.isArray(timeline) && timeline.length > 1) timeline.sort((a,b) => a.time - b.time);

                parsedData.timeline = timeline;
                currentLoadedChunkIndex = targetIndex;

                console.log(`Loaded Chunk ${targetIndex + 1}/${chunks.length}`);
            } catch (error) {
                console.error("Error loading chunk:", error);
                 parsedData.timeline = [];
                throw error; 
            } finally {
                if (overlay) overlay.style.display = "none";
            }
        })();

        try {
            await chunkLoadPromise;
        } finally {
            // Clear in-flight state (even if the load failed)
            chunkLoadPromise = null;
            chunkLoadIndexInFlight = -1;
        }

        return;
    }
}

// ==========================================
// TOPOLOGY & HARDWARE
// ==========================================
function collectDisposableResources(root, geoms, mats) {
  root.traverse(obj => {
    if (obj.geometry) geoms.add(obj.geometry);
    if (obj.material) {
      if (Array.isArray(obj.material)) obj.material.forEach(m => mats.add(m));
      else mats.add(obj.material);
    }
  });
}

function clearTopologyScene() {
  const geoms = new Set();
  const mats = new Set();

  nodeMap.forEach(group => {
    collectDisposableResources(group, geoms, mats);
    scene.remove(group);
  });
  nodeMap.clear();
  rankMap.clear();

  // Remove cabinetBox
  const toRemove = [];
  scene.traverse(obj => { if (obj.name === "cabinetBox") toRemove.push(obj); });
  toRemove.forEach(obj => {
    collectDisposableResources(obj, geoms, mats);
    scene.remove(obj);
  });

  clearLines();

  geoms.forEach(g => g.dispose());
  mats.forEach(m => m.dispose());
}

function initDashboard() {
    pausePlayback();
    clearTopologyScene();
    rankToNodeGroup.clear();   
 
    const topology = parsedData.topology;
    const chunks = parsedData.chunks;
    
    minTime = (chunks && chunks.length > 0) ? chunks[0].t_start : 0;
    maxTime = (chunks && chunks.length > 0) ? chunks[chunks.length - 1].t_end : 0;

    const duration = maxTime - minTime;
    timeMultiplier = (duration > 0) ? duration / 10.0 : 1;

    const slider = document.getElementById("timeSlider");
    slider.step = "any"; 
    slider.min = minTime; 
    slider.max = maxTime;
    slider.disabled = false;
    document.getElementById("btn-play").disabled = false;

    buildHardwareTopology(topology);
    buildRankIndex(parsedData.topology);
    renderMetadata();
    renderSpectrogram();
    initDynamicSpectrogram();
   
    initLegend();
 
    void seekToTime(minTime).catch(err => {
      console.error(err);
      pausePlayback();
    });
}

function buildRankIndex(topology) {
  rankToNodeGroup.clear();
  topology.forEach(t => {
    if (t.rank !== undefined && t.rank !== null) {
      rankToNodeGroup.set(t.rank, nodeMap.get(t.hostname));
    }
  });
}

function buildHardwareTopology(topology) {
    const nodesMap = {};

    // Read topology blueprint (Handles both flat lists and nested Cabinet structures)
    if (parsedData.hardware_blueprint) {
        const bp = parsedData.hardware_blueprint;

        // Check if we are using the advanced Cabinet -> Rack -> Node structure
        if (bp.cabinets && Array.isArray(bp.cabinets)) {
            bp.cabinets.forEach(cabinet => {
                const cabX = cabinet.x || 0;
                const cabZ = cabinet.z || 0;

                if (cabinet.racks && Array.isArray(cabinet.racks)) {
                    cabinet.racks.forEach(rack => {
                        // Calculate absolute X/Z based on cabinet + rack offsets
                        const absoluteX = cabX + (rack.x_offset || 0);
                        const absoluteZ = cabZ + (rack.z_offset || 0);

                        if (rack.nodes && Array.isArray(rack.nodes)) {
                            rack.nodes.forEach(node => {
                                const host = node.hostname;
                                // Convert the hardware 'slot' into a physical vertical Y coordinate
                                const absoluteY = (node.slot !== undefined ? node.slot : 0) * 15;

                                nodesMap[host] = { 
                                    ranks: [], 
                                    x: absoluteX, 
                                    y: absoluteY, 
                                    z: absoluteZ,
                                    cpus: node.cpus || 1,             
                                    coresPerCpu: node.cores_per_cpu || 1 
                                };
                            });
                        }
                    });
                }
            });
        } else {
            // Fallback for flat dictionary blueprints
            Object.keys(bp).forEach(host => {
                const node = bp[host];
                // Ignore the "cabinets" key if it accidentally bled into a flat map
                if (host !== "cabinets") { 
                    nodesMap[host] = { 
                        ranks: [], 
                        x: node.x || 0, 
                        y: node.y || 0, 
                        z: node.z || 0,
                        cpus: node.cpus || 1,             
                        coresPerCpu: node.cores_per_cpu || 1 
                    };
                }
            });
        }
    }

    // Add active MPI ranks and their physical locations
    topology.forEach(proc => {
        const host = proc.hostname || "unknown";
        if (!nodesMap[host]) {
            nodesMap[host] = { ranks: [], x: proc.x || 0, y: proc.y || 0, z: proc.z || 0, cpus: 1, coresPerCpu: 1 };
        }
        if (proc.rank !== undefined && proc.rank !== null) {
            // Check if this rank is already in the array
            if (!nodesMap[host].ranks.find(r => r.id === proc.rank)) {
                nodesMap[host].ranks.push({
                    id: proc.rank,
                    chip: proc.chip !== undefined ? proc.chip : 0,
                    core: proc.core !== undefined ? proc.core : nodesMap[host].ranks.length // Fallback
                });
            }
        }
    });

    let nodeIndex = 0;
    const totalNodes = Object.keys(nodesMap).length;
    let maxY = 0;

    const sharedNodeGeo = new THREE.BoxGeometry(10, 10, 10);
    const sharedNodeEdges = new THREE.EdgesGeometry(sharedNodeGeo);
    
    // Split Node Materials (Active = Bright Blue, Idle = Ghostly Gray)
    const sharedActiveNodeMat = new THREE.LineBasicMaterial({ color: 0x58a6ff, transparent: true, opacity: 0.8 }); 
    const sharedIdleNodeMat = new THREE.LineBasicMaterial({ color: 0x4b5563, transparent: true, opacity: 0.2 });   

    const sharedFillGeo = new THREE.BoxGeometry(9.8, 9.8, 9.8);
    const sharedFillMat = new THREE.MeshBasicMaterial({ color: 0x161b22, transparent: true, opacity: 0.9 });

    const sharedChipMat = new THREE.MeshBasicMaterial({ color: 0x21262d, transparent: true, opacity: 0.8 });
    
    const sharedActiveRankMat = new THREE.MeshLambertMaterial({ color: 0x4b5563, emissive: 0x58a6ff, emissiveIntensity: 0.15 });
    
    // Make Idle Cores a solid, brighter gray (wireframes vanish on high-res screens)
    const sharedIdleCoreMat = new THREE.MeshBasicMaterial({ color: 0x6e7681, transparent: true, opacity: 0.6 });
    
    // Caches for dynamically sized geometries (since core sizes depend on hardware specs)
    const geometryCache = {};

    // Build the scene
    Object.keys(nodesMap).forEach(hostname => {
        const data = nodesMap[hostname];
        const nodeGroup = new THREE.Group();

        let posX = data.x;
        let posY = data.y;
        let posZ = data.z;

        // Vertical Tower Override
        if (posY === 0 && posZ === 0 && posX === 0) {
            posX = 0;
            posY = nodeIndex * 15;
            posZ = 0;
        }

        if (posY > maxY) maxY = posY;
        nodeGroup.position.set(posX, posY, posZ);

        // Check if the node is actively used in the trace
        const isActiveNode = data.ranks.length > 0;
        const currentNodeMat = isActiveNode ? sharedActiveNodeMat : sharedIdleNodeMat;

        // Apply the correct colour based on activity
        const shellMesh = new THREE.LineSegments(sharedNodeEdges, currentNodeMat);
        shellMesh.name = "mpiNode";
        nodeGroup.add(shellMesh);

        const fillMesh = new THREE.Mesh(sharedFillGeo, sharedFillMat);
        fillMesh.name = "mpiNodeFill";
        nodeGroup.add(fillMesh);

        // Chip and core layout (strict hardware packing)
        const numChips = data.cpus;
        const numCores = data.coresPerCpu;

        // Sort all ranks on this node sequentially by ID
        const sortedRanks = data.ranks.sort((a, b) => a.id - b.id);

        const chipCols = Math.ceil(Math.sqrt(numChips));
        const chipRows = Math.ceil(numChips / chipCols);
        
        const chipSpacingX = 9.2 / chipCols; 
        const chipSpacingY = 9.2 / chipRows;

        const coreCols = Math.ceil(Math.sqrt(numCores));
        const coreRows = Math.ceil(numCores / coreCols);
        
        const maxCoreSpacingX = (chipSpacingX * 0.90) / coreCols; 
        const maxCoreSpacingY = (chipSpacingY * 0.90) / coreRows;
        
        const coreSpacing = Math.min(maxCoreSpacingX, maxCoreSpacingY);
        const coreSize = coreSpacing * 0.90; 

        const actualGridWidth = coreCols * coreSpacing;
        const actualGridHeight = coreRows * coreSpacing;

        const cacheKey = `${numChips}_${numCores}`;
        if (!geometryCache[cacheKey]) {
            geometryCache[cacheKey] = {
                chip: new THREE.BoxGeometry(chipSpacingX * 0.9, chipSpacingY * 0.9, 0.5),
                core: new THREE.BoxGeometry(coreSize, coreSize, coreSize)
            };
        }

        for (let c = 0; c < numChips; c++) {
            const cRow = Math.floor(c / chipCols);
            const cCol = c % chipCols;
            const chipOffsetX = (cCol * chipSpacingX) - 4.6 + (chipSpacingX / 2);
            const chipOffsetY = (cRow * chipSpacingY) - 4.6 + (chipSpacingY / 2);

            // Draw Chip Backplate 
            const chipMesh = new THREE.Mesh(geometryCache[cacheKey].chip, sharedChipMat);
            chipMesh.position.set(chipOffsetX, chipOffsetY, 5.01); 
            nodeGroup.add(chipMesh);

            // Center the square grid perfectly inside the chip
            const startX = chipOffsetX - (actualGridWidth / 2) + (coreSpacing / 2);
            const startY = chipOffsetY - (actualGridHeight / 2) + (coreSpacing / 2);

            for (let i = 0; i < numCores; i++) {
                const iRow = Math.floor(i / coreCols);
                const iCol = i % coreCols;
                
                const coreOffsetX = startX + (iCol * coreSpacing);
                const coreOffsetY = startY + (iRow * coreSpacing);

                const globalSlotIndex = (c * numCores) + i;
                const activeRank = sortedRanks[globalSlotIndex];

                if (activeRank) {
                    const uniqueRankMat = sharedActiveRankMat.clone();
                    const rankMesh = new THREE.Mesh(geometryCache[cacheKey].core, uniqueRankMat);
                    rankMesh.name = "mpiRank";
                    
                    // Attach the exact hardware payload to the 3D object for the tooltip
                    rankMesh.userData = { 
                        rank: activeRank.id, 
                        host: hostname, 
                        chip: c, 
                        core: i 
                    };

                    rankMesh.position.set(coreOffsetX, coreOffsetY, 5.2); 
                    nodeGroup.add(rankMesh);
                    rankMap.set(activeRank.id, rankMesh); 
                } else {
                    const idleMesh = new THREE.Mesh(geometryCache[cacheKey].core, sharedIdleCoreMat);
                    idleMesh.name = "idleCore";
                    idleMesh.position.set(coreOffsetX, coreOffsetY, 5.2); 
                    nodeGroup.add(idleMesh);
                }
            }
        }

        scene.add(nodeGroup);
        nodeMap.set(hostname, nodeGroup);
        nodeIndex++;
    });

    // CABINET & AUTO-FRAME
    if (totalNodes > 0) {
        const cabinetGeo = new THREE.BoxGeometry(20, maxY + 20, 20);
        const cabEdges = new THREE.EdgesGeometry(cabinetGeo);
        const cabinetMat = new THREE.LineBasicMaterial({ color: 0x8b949e, transparent: true, opacity: 0.5 });
        const cabinetMesh = new THREE.LineSegments(cabEdges, cabinetMat);
        cabinetMesh.name = "cabinetBox";
        cabinetMesh.position.set(0, maxY / 2, 0);
        scene.add(cabinetMesh);
    }

    const centerY = maxY / 2;
    const distanceToPullBack = Math.max(300, totalNodes * 20);
    camera.position.set(0, centerY, distanceToPullBack);
    controls.target.set(0, centerY, 0);
    controls.update(); 
}

// ==========================================
// 3D HOVER TOOLTIPS
// ==========================================
function initTooltip() {
    if (document.getElementById("mpiTooltip")) return;
    
    tooltipEl = document.createElement('div');
    tooltipEl.id = "mpiTooltip";
    tooltipEl.style.position = 'absolute';
    tooltipEl.style.display = 'none';
    tooltipEl.style.pointerEvents = 'none'; // Critical so it doesn't block the mouse
    tooltipEl.style.backgroundColor = 'rgba(22, 27, 34, 0.95)';
    tooltipEl.style.border = '1px solid #58a6ff';
    tooltipEl.style.borderRadius = '6px';
    tooltipEl.style.padding = '10px 15px';
    tooltipEl.style.color = '#c9d1d9';
    tooltipEl.style.fontFamily = "'Fira Code', monospace";
    tooltipEl.style.fontSize = '0.85rem';
    tooltipEl.style.zIndex = '2000';
    tooltipEl.style.boxShadow = '0 4px 12px rgba(0,0,0,0.8)';
    document.body.appendChild(tooltipEl);
}

function onMouseMove(event) {
    if (!tooltipEl) return;

    const rect = renderer.domElement.getBoundingClientRect();
    mouse.x = ((event.clientX - rect.left) / rect.width) * 2 - 1;
    mouse.y = -((event.clientY - rect.top) / rect.height) * 2 + 1;

    raycaster.setFromCamera(mouse, camera);
    const intersects = raycaster.intersectObjects(scene.children, true);

    let hoveredRank = null;
    for (let i = 0; i < intersects.length; i++) {
        if (intersects[i].object.name === "mpiRank") {
            hoveredRank = intersects[i].object;
            break;
        }
    }

    if (hoveredRank) {
        const data = hoveredRank.userData;
        tooltipEl.innerHTML = `
            <strong style="color: #58a6ff; font-size: 1.0rem;">Rank ${data.rank}</strong><br/>
            <hr style="border: 0; border-top: 1px solid #30363d; margin: 6px 0;">
            <span style="color: #8b949e;">Host:</span> ${data.host}<br/>
            <span style="color: #8b949e;">Chip:</span> ${data.chip} | <span style="color: #8b949e;">Core:</span> ${data.core}
        `;
        tooltipEl.style.display = 'block';
        tooltipEl.style.left = (event.clientX + 15) + 'px'; 
        tooltipEl.style.top = (event.clientY + 15) + 'px';
        document.body.style.cursor = 'pointer';
    } else {
        tooltipEl.style.display = 'none';
        document.body.style.cursor = 'default';
    }
}

// ===============
// CAMERA MOVEMENT
// ===============
function onCanvasClick(event) {
    const rect = renderer.domElement.getBoundingClientRect();
    mouse.x = ((event.clientX - rect.left) / rect.width) * 2 - 1;
    mouse.y = -((event.clientY - rect.top) / rect.height) * 2 + 1;

    raycaster.setFromCamera(mouse, camera);
    const intersects = raycaster.intersectObjects(scene.children, true);

    for (let i = 0; i < intersects.length; i++) {
        const object = intersects[i].object;
        
        if (object.name === "mpiNode" || object.name === "mpiRank") {
            const targetPosition = new THREE.Vector3();
            object.getWorldPosition(targetPosition);
            
            flyCameraTo(targetPosition);
            break; 
        }
    }
}

function flyCameraTo(targetPos) {
    controls.enabled = false;

    const cameraOffset = new THREE.Vector3(0, 15, 40);
    const finalCameraPos = targetPos.clone().add(cameraOffset);

    const startTarget = controls.target.clone();
    const startCamera = camera.position.clone();
    
    const duration = 800; 
    const startTime = performance.now();

    function animateTransition(time) {
        let elapsed = time - startTime;
        let t = elapsed / duration;
        if (t > 1) t = 1;

        const easeT = 1 - Math.pow(1 - t, 3);

        camera.position.lerpVectors(startCamera, finalCameraPos, easeT);
        controls.target.lerpVectors(startTarget, targetPos, easeT);
        
        if (t < 1) {
            requestAnimationFrame(animateTransition);
        } else {
            controls.enabled = true;
            controls.update();
        }
    }
    
    requestAnimationFrame(animateTransition);
}


// ==========================================
// PLAYBACK & RENDERING
// ==========================================
async function handleManualSeek(event) {
    pausePlayback();
    await seekToTime(parseFloat(event.target.value));
}

function pausePlayback() {
    isPlaying = false;
    const btn = document.getElementById("btn-play");
    if (btn) btn.innerHTML = "<b>▶ Play</b>";
    
    if (animationFrameId) {
        cancelAnimationFrame(animationFrameId);
        animationFrameId = null;
    }
}

function togglePlayback() {
    if (isPlaying) {
        pausePlayback();
    } else {
        isPlaying = true;
        const btn = document.getElementById("btn-play");
        if (btn) btn.innerHTML = "<b>|| Pause</b>";
        
        lastFrameTime = performance.now();
        playLoop(performance.now());
    }
}

async function seekToTime(time, isPlayingLoop = false) {
    currentTime = time;
    document.getElementById("timeSlider").value = currentTime;
    document.getElementById("currentTimeLabel").textContent = currentTime.toFixed(3);
    
    try {
      await ensureChunkLoadedForTime(currentTime);
    } catch (e) {
      pausePlayback();
      return [];
    }   
 
    const activeEvents = renderActiveCommunications();
    
    if (!isPlayingLoop) {
        updateDynamicSpectrogram(activeEvents);
    }
    
    return activeEvents;
}

async function playLoop(timestamp) {
    if (!isPlaying || isProcessingFrame) return; 
    isProcessingFrame = true;

    const deltaTime = (timestamp - lastFrameTime) / 1000; 
    lastFrameTime = timestamp;
    
    const cappedDelta = Math.min(deltaTime, 0.05);
    const speed = Math.pow(10, parseFloat(document.getElementById("speedSlider").value));
    let nextTime = currentTime + (cappedDelta * timeMultiplier * speed);

    if (nextTime >= maxTime) {
        await seekToTime(maxTime, false);
        pausePlayback();
        isProcessingFrame = false;
        return;
    }

    const activeEvents = await seekToTime(nextTime, true);

    if (timestamp - lastDynUpdate > 100) {
        updateDynamicSpectrogram(activeEvents);
        lastDynUpdate = timestamp;
    }

    isProcessingFrame = false;
    
    if (isPlaying) {
        animationFrameId = requestAnimationFrame(playLoop);
    }
}

function renderActiveCommunications() {
    clearLines();

    const raw = parseFloat(document.getElementById("speedSlider").value);
    const windowSize = Math.min(0.2, 0.05 * Math.pow(10, raw));

    const minTimeWindow = currentTime - windowSize;
    const timeline = parsedData.timeline;
    const activeEvents = [];

    // Binary Search
    if (timeline && timeline.length > 0) {
        let left = 0;
        let right = timeline.length - 1;
        let mid = 0;
        
        while (left <= right) {
            mid = Math.floor((left + right) / 2);
            if (timeline[mid].time <= currentTime) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
        
        const MAX_VISIBLE_MESSAGES = 400;
        let eventsCaptured = 0;
 
        // 'right' is now the index of the last event occurring before/at currentTime.
        // Step backwards to grab only the events within our time window!
        for (let i = right; i >= 0; i--) {
            if (timeline[i].time >= minTimeWindow) {
                activeEvents.push(timeline[i]);
                eventsCaptured++;
                
                // Abort immediately if the speed multiplier swallows too many events
                if (eventsCaptured >= MAX_VISIBLE_MESSAGES) break; 
            } else {
                break; // Stop immediately once we fall out of the time window
            }
        } 
    }

    activeEvents.forEach(event => {
        const cat = MPI_CATEGORIES[event.call] || DEFAULT_CATEGORY;

        // Illumination Logic
        const senderMesh = rankMap.get(event.sender);
        const recvMesh = rankMap.get(event.receiver);

        if (event.call === "MPI_WAIT" || event.call === "MPI_WAITALL") {
            return; 
        } else {
            // Ignite the ranks
            if (senderMesh) {
                senderMesh.material.emissive.setHex(cat.color);
                senderMesh.material.emissiveIntensity = 1.0;
            }
            if (recvMesh) {
                recvMesh.material.emissive.setHex(cat.color);
                recvMesh.material.emissiveIntensity = 1.0;
            }
        }

        if (event.sender === event.receiver) return;

        // Line Routing Logic
        const sNode = rankToNodeGroup.get(event.sender);
        const rNode = rankToNodeGroup.get(event.receiver);

        if (sNode && rNode) {
            // If the communications are inside a single node.
            if (sNode === rNode) {
                // Draw intra-node rank-to-rank.
                const sRankMesh = rankMap.get(event.sender);
                const rRankMesh = rankMap.get(event.receiver);

                if (sRankMesh && rRankMesh) {
                    const startWorld = new THREE.Vector3();
                    const endWorld = new THREE.Vector3();
                    
                    sRankMesh.getWorldPosition(startWorld);
                    rRankMesh.getWorldPosition(endWorld);

                    // Pass event.time to generate an animated packet
                    drawIntraNodeLine(startWorld, endWorld, event.call, event.time);
                }
            } else {
                // They are on different nodes.
                if (cat.type === "collective") {
                    // Collectives stay node-to-node
                    drawInterNodeLine(sNode.position, rNode.position, event.call, event.sender, event.receiver, event.time);
                } else {
                    // Point-to-Point routes directly core-to-core
                    const sRankMesh = rankMap.get(event.sender);
                    const rRankMesh = rankMap.get(event.receiver);

                    if (sRankMesh && rRankMesh) {
                        const startWorld = new THREE.Vector3();
                        const endWorld = new THREE.Vector3();
                        
                        sRankMesh.getWorldPosition(startWorld);
                        rRankMesh.getWorldPosition(endWorld);

                        // Pass event.time to generate an animated packet
                        drawInterNodeLine(startWorld, endWorld, event.call, event.sender, event.receiver, event.time);
                    }
                }
            }
        }
    });

    return activeEvents;
}

function drawIntraNodeLine(startPos, endPos, callName) {
    const cat = MPI_CATEGORIES[callName] || DEFAULT_CATEGORY;

    const midPoint = startPos.clone().lerp(endPos, 0.5);
    const distance = startPos.distanceTo(endPos);

    // Bow straight out towards the user so the line doesn't cut through the cores
    const bowDistance = Math.max(distance * 0.4, 1.0); 
    midPoint.z += bowDistance;

    const curve = new THREE.QuadraticBezierCurve3(startPos, midPoint, endPos);
    
    const geometry = new THREE.TubeGeometry(curve, 12, 0.08, 6, false);

    // Use the opaque Mesh material
    const material = sharedMaterials[callName + "_junction"] || sharedMaterials["default_junction"];

    const tube = new THREE.Mesh(geometry, material);
    tube.name = "mpiLine";
    scene.add(tube);
    activeLines.push(tube);

    // Add tiny junction points at the core boundaries
    const points = curve.getPoints(10); 
    createJunctionPoint(points[0], callName);
    createJunctionPoint(points[points.length - 1], callName);
}

function drawInterNodeLine(startPos, endPos, callName, sender, receiver) {
    const cat = MPI_CATEGORIES[callName] || DEFAULT_CATEGORY;

    const midPoint = startPos.clone().lerp(endPos, 0.5);
    const distance = startPos.distanceTo(endPos);
    const bowDistance = Math.max(distance * 0.3, 2.0); 
    const laneOffset = (sender > receiver) ? 2.0 : -2.0;

    // Track Separation Math
    if (cat.type === "collective") {
        midPoint.z += bowDistance; 
    } else if (cat.type === "p2p_nonblock") {
        midPoint.x -= bowDistance; 
        midPoint.z += laneOffset; 
    } else {
        midPoint.x += bowDistance; 
        midPoint.z += laneOffset; 
    }

    const curve = new THREE.QuadraticBezierCurve3(startPos, midPoint, endPos);
    
    // We use 20 segments here to keep the long arcs smooth, with the same 0.08 radius
    const geometry = new THREE.TubeGeometry(curve, 20, 0.08, 6, false);

    // Because we are using a Tube (which is a Mesh), we must use the junction material 
    const material = sharedMaterials[callName + "_junction"] || sharedMaterials["default_junction"];

    const tube = new THREE.Mesh(geometry, material);
    tube.name = "mpiLine";
    scene.add(tube);
    activeLines.push(tube);

    // Anchor strictly to the start and end of the curve
    const points = curve.getPoints(20); 
    createJunctionPoint(points[0], callName);
    createJunctionPoint(points[points.length - 1], callName);
}

function createJunctionPoint(pos, callName) {
    const mat = sharedMaterials[callName + "_junction"] || sharedMaterials["default_junction"];
    
    // We reuse the single sharedSphereGeo for every single junction
    const mesh = new THREE.Mesh(sharedSphereGeo, mat);
    mesh.position.copy(pos);
    scene.add(mesh);
    junctionPoints.push(mesh);
}

function clearLines() {
    activeLines.forEach(line => {
        // We only dispose the geometry now, because the curve is unique.
        // We do not dispose the material, because it is shared.
        if (line.geometry) line.geometry.dispose();
        scene.remove(line);
    });
    activeLines = [];

    junctionPoints.forEach(pt => {
        // We do not dispose geometry or material. Both are shared.
        scene.remove(pt);
    });
    junctionPoints = [];
}

function renderMetadata() {
    let container = document.getElementById("metadataContainer");
    
    // Dynamic injection fallback if the user hasn't added the div to their HTML yet
    if (!container) {
        const timeSlider = document.getElementById("timeSlider");
        if (timeSlider) {
            const timelinePanel = timeSlider.parentElement; 
            container = document.createElement("div");
            container.id = "metadataContainer";
            container.style.marginBottom = "20px";
            container.style.padding = "15px";
            container.style.backgroundColor = "rgba(22, 27, 34, 0.5)";
            container.style.border = "1px solid #30363d";
            container.style.borderRadius = "8px";
            timelinePanel.parentNode.insertBefore(container, timelinePanel);
        } else {
            return; // Safety exit if UI elements aren't found
        }
    }
    
    container.innerHTML = "";
    
    // Extract metadata from the parsed .mpix file header
    const meta = parsedData.metadata || parsedData.info || {};
    const programName = meta.program || meta.executable || meta.name || "Unknown Program";
    const runDate = meta.date || meta.timestamp || "Unknown Date";
   
    // Extract the topology metadata from the parsed .mpix file
    const systemName = meta.system_name || "Unknown System";
 
    // Calculate scale from topology
    const totalRanks = parsedData.topology ? parsedData.topology.length : 0;
    
    const activeNodes = new Set();
    if (parsedData.topology) {
        parsedData.topology.forEach(t => {
            if (t.hostname) activeNodes.add(t.hostname);
        });
    }
    const totalActiveNodes = activeNodes.size;

    container.innerHTML = `
        <div style="font-size: 0.75rem; color: #8b949e; font-weight: bold; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 10px;">
            Run Metadata
        </div>
        <div style="color: #c9d1d9; font-family: 'Fira Code', monospace; font-size: 0.85rem; line-height: 1.6;">
            <div><span style="color: #58a6ff;">Program:</span> ${programName}</div>
            <div><span style="color: #58a6ff;">Date:</span> ${runDate}</div>
            <div><span style="color: #58a6ff;">System:</span> ${systemName}</div>
            <div><span style="color: #58a6ff;">Scale:</span> ${totalRanks} Ranks across ${totalActiveNodes} Nodes</div>
        </div>
    `;
}


// ======
// LEGEND
// ======
function initLegend() {
    // Prevent duplicate legends if the user loads multiple profiles
    if (document.getElementById("mpiLegend")) return;

    // Create the floating panel
    const legendDiv = document.createElement("div");
    legendDiv.id = "mpiLegend";
    legendDiv.style.position = "absolute";
    legendDiv.style.bottom = "20px";
    legendDiv.style.right = "20px";
    legendDiv.style.backgroundColor = "rgba(22, 27, 34, 0.85)";
    legendDiv.style.border = "1px solid #30363d";
    legendDiv.style.borderRadius = "8px";
    legendDiv.style.padding = "12px 18px";
    legendDiv.style.color = "#c9d1d9";
    legendDiv.style.fontFamily = "'Fira Code', monospace, sans-serif";
    legendDiv.style.fontSize = "0.8rem";
    legendDiv.style.zIndex = "1000";
    legendDiv.style.pointerEvents = "none"; // Lets user click/drag the 3D camera *through* the legend
    legendDiv.style.boxShadow = "0 4px 12px rgba(0,0,0,0.5)";
    legendDiv.style.backdropFilter = "blur(4px)";

    const title = document.createElement("div");
    title.innerHTML = "<strong>COMMUNICATION TYPES</strong>";
    title.style.marginBottom = "10px";
    title.style.borderBottom = "1px solid #30363d";
    title.style.paddingBottom = "6px";
    title.style.color = "#8b949e";
    legendDiv.appendChild(title);

    // Group the dictionary by category type to avoid drawing 20 duplicate blue boxes
    const uniqueTypes = {};
    Object.values(MPI_CATEGORIES).forEach(cat => {
        if (!uniqueTypes[cat.type]) uniqueTypes[cat.type] = cat.color;
    });

    // Clean up the internal code names for the UI
    const formatName = (str) => {
        if (str === "p2p_block") return "P2P Blocking";
        if (str === "p2p_nonblock") return "P2P Non-Blocking";
        if (str === "state") return "Wait / Sync States";
        if (str === "collective") return "Collectives";
        return str;
    };

    // Draw the color swatches
    Object.entries(uniqueTypes).forEach(([type, hexColor]) => {
        const row = document.createElement("div");
        row.style.display = "flex";
        row.style.alignItems = "center";
        row.style.marginBottom = "6px";

        const colorBox = document.createElement("div");
        colorBox.style.width = "12px";
        colorBox.style.height = "12px";
        colorBox.style.marginRight = "10px";
        colorBox.style.borderRadius = "2px";
        // Convert the Three.js hex number back into a standard CSS #hex string
        colorBox.style.backgroundColor = '#' + hexColor.toString(16).padStart(6, '0');
        colorBox.style.boxShadow = `0 0 5px ${colorBox.style.backgroundColor}`; // Subtle glow

        const label = document.createElement("span");
        label.textContent = formatName(type);

        row.appendChild(colorBox);
        row.appendChild(label);
        legendDiv.appendChild(row);
    });

    // Mount it directly over the 3D Canvas
    const container = document.getElementById("visCanvas");
    if (container) {
        container.style.position = "relative"; // Required to anchor the absolute legend
        container.appendChild(legendDiv);
    }
}

// ==========================================
// SPECTROGRAM DASHBOARDS
// ==========================================
function renderSpectrogram() {
    const container = document.getElementById("overallStatsContainer");
    container.innerHTML = "";
    if (!parsedData || !parsedData.statistics) return;
    
    const table = document.createElement('table');
    table.style.borderCollapse = 'separate';
    table.style.borderSpacing = '3px';
    
    const stats = parsedData.statistics;
    const calls = Object.keys(stats);
    if (calls.length === 0) return;
    
    let maxVal = 0;
    calls.forEach(c => Object.values(stats[c]).forEach(v => { if (v > maxVal) maxVal = v; }));

    calls.forEach(call => {
        const tr = document.createElement('tr');
        // Label
        const tdLabel = document.createElement('td');
        tdLabel.textContent = call.replace('MPI_', '');
        tdLabel.style.color = '#8b949e';
        tdLabel.style.textAlign = 'right';
        tdLabel.style.paddingRight = '8px';
        tr.appendChild(tdLabel);

        // Blocks
        // Using Object.entries instead of Object.values so we can grab the 'bin' name for the tooltip
        Object.entries(stats[call]).forEach(([bin, val]) => {
            const td = document.createElement('td');
            td.style.border = 'none';
            td.style.borderRadius = '3px';
            td.style.height = '20px';
            td.style.width = '35px';
            
            // Add the native browser tooltip
            td.title = `${call} (${bin}): ${val} total messages`;
            
            if (val === 0) {
                td.style.backgroundColor = '#161b22';
            } else {
                const intensity = Math.max(0.15, val / maxVal);
                td.style.backgroundColor = `rgba(88, 166, 255, ${intensity})`; // Blue Overall
            }
            tr.appendChild(td);
        });
        table.appendChild(tr);
    });
    container.appendChild(table);
}

function initDynamicSpectrogram() {
    const container = document.getElementById("activeStatsContainer");
    container.innerHTML = "";
    dynamicCells = {};
    if (!parsedData || !parsedData.statistics) return;

    const table = document.createElement('table');
    table.style.borderCollapse = 'separate';
    table.style.borderSpacing = '3px';
    
    const stats = parsedData.statistics;
    const calls = Object.keys(stats);
    if (calls.length === 0) return;
    const binsTemplate = Object.keys(stats[calls[0]]);

    calls.forEach(call => {
        const tr = document.createElement('tr');
        dynamicCells[call] = {}; 

        const tdLabel = document.createElement('td');
        tdLabel.textContent = call.replace('MPI_', '');
        tdLabel.style.color = '#8b949e';
        tdLabel.style.textAlign = 'right';
        tdLabel.style.paddingRight = '8px';
        tr.appendChild(tdLabel);

        binsTemplate.forEach(bin => {
            const td = document.createElement('td');
            td.style.border = 'none';
            td.style.borderRadius = '3px';
            td.style.height = '20px';
            td.style.width = '35px';
            td.style.backgroundColor = '#161b22';
            td.style.transition = 'background-color 0.15s ease-out'; 
            
            // Add the initial blank tooltip
            td.title = `${call} (${bin}): 0 active messages`;
            
            dynamicCells[call][bin] = td;
            tr.appendChild(td);
        });
        table.appendChild(tr);
    });
    container.appendChild(table);
}

function updateDynamicSpectrogram(activeEvents) {
    if (!parsedData || !parsedData.statistics || !dynamicCells || !activeEvents) return;

    const stats = parsedData.statistics;
    const calls = Object.keys(stats);
    if (calls.length === 0) return;

    const first = stats[calls[0]];
    if (!first) return;  
    const binsTemplate = Object.keys(stats[calls[0]]);
    let currentCounts = {};
    calls.forEach(c => {
        currentCounts[c] = {};
        binsTemplate.forEach(b => currentCounts[c][b] = 0);
    });

    // Only tally the specific events active in the 3D scene right now
    for (let i = 0; i < activeEvents.length; i++) {
        const event = activeEvents[i];
        const call = event.call;
        if (currentCounts[call] !== undefined) {
            const b = event.bytes || 0;
            if (b < 128) currentCounts[call]["< 128B"]++;
            else if (b < 1024) currentCounts[call]["128B - 1KB"]++;
            else if (b < 65536) currentCounts[call]["1KB - 64KB"]++;
            else if (b < 1048576) currentCounts[call]["64KB - 1MB"]++;
            else if (b < 16777216) currentCounts[call]["1MB - 16MB"]++;
            else currentCounts[call]["> 16MB"]++;
        }
    }
    let globalMax = 0;
    calls.forEach(c => binsTemplate.forEach(b => { if (stats[c][b] > globalMax) globalMax = stats[c][b]; }));

    // Update Floating Blocks
    calls.forEach(call => {
        binsTemplate.forEach(bin => {
            const count = currentCounts[call][bin];
            const td = dynamicCells[call][bin];
            
            // Dynamically update the tooltip with the live count
            td.title = `${call} (${bin}): ${count} active messages`;
            
            if (count === 0) {
                td.style.backgroundColor = '#161b22';
            } else {
                const denom = globalMax > 0 ? globalMax : 1;
                const intensity = Math.max(0.15, count / denom);
                td.style.backgroundColor = `rgba(46, 160, 67, ${intensity})`; // Green Active
            }
        });
    });
}
