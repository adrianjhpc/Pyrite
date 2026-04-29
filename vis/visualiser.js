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
const rankMap = new Map();  // Maps rank ID -> Instance Data
let activeLines = [];       // Currently rendered bezier curves
let junctionPoints = [];    // Active receive/send ports
let dynamicCells = {};      // HTML Table cell references

const rankToNodeGroup = new Map();

// Lighting State
const activelyGlowingRanks = new Map(); // rankId -> { mesh, instanceId, color, intensity }
const defaultRankColor = new THREE.Color(0x4b5563);

// Memory Caches
const sharedMaterials = {};
const sharedSphereGeo = new THREE.SphereGeometry(0.04, 8, 8);

const sharedArrowGeo = new THREE.ConeGeometry(0.08, 0.25, 8);
sharedArrowGeo.translate(0, -0.125, 0); // Center the pivot at the tip
sharedArrowGeo.rotateX(Math.PI / 2);

// 3d hover tooltips
let tooltipEl;

// Recording functionality
let uiMediaRecorder = null;
let uiRecordedChunks = [];
let uiStream = null;

// Camera and layout controls
let defaultCameraPose = null;   // set after buildHardwareTopology() auto-frames
let selectedObject = null;      // last clicked mpiNode/mpiRank
let isFollowEnabled = false;
let followOffset = new THREE.Vector3(0, 15, 40);
let desiredCam = new THREE.Vector3();

const nodeOriginalPos = new Map();
let currentLayoutMode = "blueprint";

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
    
    // States / completion calls (Dimmer Teal)
    "MPI_WAIT": { type: "state", color: 0x238636 },
    "MPI_WAITALL": { type: "state", color: 0x238636 },
    "MPI_WAITANY": { type: "state", color: 0x238636 },
    "MPI_WAITSOME": { type: "state", color: 0x238636 },
    "MPI_TEST": { type: "state", color: 0x238636 },
    "MPI_TESTANY": { type: "state", color: 0x238636 },
    "MPI_TESTALL": { type: "state", color: 0x238636 },
    "MPI_TESTSOME": { type: "state", color: 0x238636 },

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
    
    document.getElementById("btn-rec-ui-start")?.addEventListener("click", () => {
	startUIRecording({ fps: 60, bitsPerSecond: 8_000_000 }).catch(err => {
	    console.error(err);
            alert("Failed to start UI recording. See console for details.");
	});
    });

    document.getElementById("btn-rec-ui-stop")?.addEventListener("click", stopUIRecording);

    // Camera presets
    document.getElementById("btn-view-iso")?.addEventListener("click", () => viewPreset("iso"));
    document.getElementById("btn-view-top")?.addEventListener("click", () => viewPreset("top"));
    document.getElementById("btn-view-front")?.addEventListener("click", () => viewPreset("front"));
    document.getElementById("btn-view-side")?.addEventListener("click", () => viewPreset("side"));
    document.getElementById("btn-view-reset")?.addEventListener("click", resetView);

    // Follow selection
    document.getElementById("chk-follow")?.addEventListener("change", (e) => {
	setFollowEnabled(e.target.checked);
    });

    // Layout switch
    document.getElementById("layoutMode")?.addEventListener("change", (e) => {
	applyLayout(e.target.value);
    });

    // Saved views
    initSavedViewsUI();
    document.getElementById("btn-view-save")?.addEventListener("click", saveCurrentView);
    document.getElementById("btn-view-load")?.addEventListener("click", loadSelectedView);
    document.getElementById("btn-view-del")?.addEventListener("click", deleteSelectedView);
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

    // Enable Logarithmic Depth Buffer to kill long-distance shimmering 
    renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true, logarithmicDepthBuffer: true });
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
    grid.name = "floorGrid"; // Name the grid so we can move it later
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

    if (isFollowEnabled) {
        updateFollowCamera(); // includes controls.update()
    } else {
        controls.update();
    }

    // Fading logic for actively glowing instanced meshes
    if (isPlaying && activelyGlowingRanks.size > 0) {
        activelyGlowingRanks.forEach((state, rankId) => {
            state.intensity -= 0.02;
            if (state.intensity <= 0) {
                state.mesh.setColorAt(state.instanceId, defaultRankColor);
                activelyGlowingRanks.delete(rankId);
            } else {
                const c = defaultRankColor.clone().lerp(state.color, state.intensity);
                state.mesh.setColorAt(state.instanceId, c);
            }
            state.mesh.instanceColor.needsUpdate = true;
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
            transparent: true, opacity: 0.5, blending: THREE.AdditiveBlending, 
            depthWrite: false, depthTest: false
        });

        sharedMaterials[call + "_tube"] = new THREE.MeshLambertMaterial({
            color: cat.color
        });
        
        sharedMaterials[call + "_junction"] = new THREE.MeshLambertMaterial({
            color: cat.color
        });
    });

    // Build the fallback defaults
    sharedMaterials["default_line"] = new THREE.LineBasicMaterial({
        color: DEFAULT_CATEGORY.color, transparent: true, opacity: 0.5, blending: THREE.AdditiveBlending, depthWrite: false, depthTest: false
    });
    sharedMaterials["default_tube"] = new THREE.MeshLambertMaterial({ 
        color: DEFAULT_CATEGORY.color 
    });
    sharedMaterials["default_junction"] = new THREE.MeshLambertMaterial({ 
        color: DEFAULT_CATEGORY.color 
    });
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
            
            try {
                parsedData = JSON.parse(headerText);
                parsedData.timeline = [];
            } catch (parseError) {
                alert("Failed to parse the MPI profile data. The file may be corrupted.");
                return; 
            } 
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

    while (true) {
        const targetIndex = findTargetIndex(time);

        if (targetIndex === currentLoadedChunkIndex) return;

        if (chunkLoadPromise) {
            if (chunkLoadIndexInFlight === targetIndex) {
                await chunkLoadPromise;
                return;
            }
            await chunkLoadPromise;
            continue; 
        }

        const overlay = document.getElementById("loadingOverlay");
        const loadingText = document.getElementById("loadingText");

        chunkLoadIndexInFlight = targetIndex;

        chunkLoadPromise = (async () => {
            try {
                if (overlay) overlay.style.display = "block";
                if (loadingText) {
                    loadingText.textContent = `Unpacking Chunk ${targetIndex + 1} of ${chunks.length}...`;
                }

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
    activelyGlowingRanks.clear();

    const toRemove = [];
    scene.traverse(obj => { 
	if (obj.name === "cabinetBox" || obj.name === "groupBox") { 
            toRemove.push(obj); 
	} 
    });
    
    toRemove.forEach(obj => {
	collectDisposableResources(obj, geoms, mats);
	scene.remove(obj);
    });

    clearLines();

    geoms.forEach(g => g.dispose());
    mats.forEach(m => m.dispose());

    nodeOriginalPos.clear();
    setSelectedObject(null);
    setFollowEnabled(false);

    const chk = document.getElementById("chk-follow");
    if (chk) chk.checked = false;

    const layoutSel = document.getElementById("layoutMode");
    if (layoutSel) layoutSel.value = "blueprint";

    currentLayoutMode = "blueprint";
    defaultCameraPose = null;
}

function initDashboard() {
    if (!parsedData) return;
    pausePlayback();
    clearTopologyScene();
    rankToNodeGroup.clear();   
    
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

    buildHardwareTopology();
    renderMetadata();
    if (window.AnalyticsUI) {
        AnalyticsUI.renderAnalytics();
    }
    renderSpectrogram();
    initDynamicSpectrogram();
    initLegend();
 
    void seekToTime(minTime).catch(err => {
	console.error(err);
	pausePlayback();
    });
}

// ==========================================
// 3D HOVER TOOLTIPS
// ==========================================
function initTooltip() {
    const existing = document.getElementById("mpiTooltip");
    if (existing) {
	tooltipEl = existing;
	return;
    }   
    
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

// =========================================
// CAMERA FUNCTIONS
// =========================================
function getCameraPose() {
    return {
	position: camera.position.clone(),
	target: controls.target.clone(),
	up: camera.up.clone()
    };
}

function setCameraPose(pose) {
    if (pose.up) camera.up.copy(pose.up);
    camera.position.copy(pose.position);
    controls.target.copy(pose.target);
    controls.update();
}

function animateCameraPose(toPose, duration = 650) {
    const from = getCameraPose();
    const start = performance.now();

    function step(t) {
	const u = Math.min(1, (t - start) / duration);
	const ease = 1 - Math.pow(1 - u, 3);

	camera.position.lerpVectors(from.position, toPose.position, ease);
	controls.target.lerpVectors(from.target, toPose.target, ease);

	if (toPose.up) {
	    const up = from.up.clone().lerp(toPose.up, ease).normalize();
	    camera.up.copy(up);
	}

	controls.update();
	if (u < 1) requestAnimationFrame(step);
    }

    requestAnimationFrame(step);
}

function computeTopologyBounds() {
    const box = new THREE.Box3();
    let hasAny = false;

    nodeMap.forEach(group => {
	box.expandByObject(group);
	hasAny = true;
    });

    if (!hasAny) {
	box.min.set(-1, -1, -1);
	box.max.set(1, 1, 1);
    }
    return box;
}

function viewPreset(name) {
    const box = computeTopologyBounds();
    const center = new THREE.Vector3();
    const size = new THREE.Vector3();
    box.getCenter(center);
    box.getSize(size);

    const radius = Math.max(size.length() * 0.5, 10);
    const dist = radius * 2.2;

    const pose = { target: center, position: new THREE.Vector3(), up: new THREE.Vector3(0,1,0) };

    if (name === "iso") {
	pose.position.copy(center).add(new THREE.Vector3(1, 0.7, 1).normalize().multiplyScalar(dist));
	pose.up.set(0, 1, 0);
    } else if (name === "top") {
	pose.position.copy(center).add(new THREE.Vector3(0, 1, 0).multiplyScalar(dist));
	pose.up.set(0, 0, -1);
    } else if (name === "front") {
	pose.position.copy(center).add(new THREE.Vector3(0, 0.2, 1).normalize().multiplyScalar(dist));
	pose.up.set(0, 1, 0);
    } else if (name === "side") {
	pose.position.copy(center).add(new THREE.Vector3(1, 0.2, 0).normalize().multiplyScalar(dist));
	pose.up.set(0, 1, 0);
    }

    animateCameraPose(pose, 650);
}

function cacheDefaultCameraPose() {
    defaultCameraPose = getCameraPose();
}

function resetView() {
    if (!defaultCameraPose) return;
    animateCameraPose(defaultCameraPose, 650);
}

function setSelectedObject(obj) {
    selectedObject = obj || null;

    const status = document.getElementById("camStatus");
    if (!status) return;

    if (!selectedObject) {
	status.textContent = "";
	return;
    }

    if (selectedObject.userData?.rank !== undefined) {
	status.textContent = `Selected rank ${selectedObject.userData.rank} @ ${selectedObject.userData.host}`;
    } else if (selectedObject.userData?.hostname) {
	status.textContent = `Selected node ${selectedObject.userData.hostname}`;
    } else {
	status.textContent = `Selected: ${selectedObject.name || "object"}`;
    }
}

function setFollowEnabled(enabled) {
    isFollowEnabled = !!enabled;
    controls.enabled = !isFollowEnabled;
    if (isFollowEnabled) {
	followOffset.copy(camera.position).sub(controls.target);
    }
}

function updateFollowCamera() {
    if (!isFollowEnabled || !selectedObject) return;

    const targetPos = new THREE.Vector3();
    
    if (selectedObject.userData && selectedObject.userData.isCore) {
	// It's a mock object we made for instanced ranks
	targetPos.copy(selectedObject.worldPos);
    } else {
	selectedObject.getWorldPosition(targetPos);
    }

    controls.target.lerp(targetPos, 0.18);
    desiredCam.copy(controls.target).add(followOffset);
    camera.position.lerp(desiredCam, 0.18);

    controls.update();
}

function animateNodePositions(targetPositions, duration = 650) {
    const start = performance.now();
    const startPositions = new Map();

    nodeMap.forEach((group, hostname) => {
	startPositions.set(hostname, group.position.clone());
    });

    function step(t) {
	const u = Math.min(1, (t - start) / duration);
	const ease = 1 - Math.pow(1 - u, 3);

	nodeMap.forEach((group, hostname) => {
	    const from = startPositions.get(hostname);
	    const to = targetPositions.get(hostname);
	    if (!from || !to) return;
	    group.position.lerpVectors(from, to, ease);
	});

	if (u < 1) requestAnimationFrame(step);
    }

    requestAnimationFrame(step);
}

function applyLayout(mode) {
    currentLayoutMode = mode;

    const targets = new Map();
    const hostnames = Array.from(nodeMap.keys());

    let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity, minZ = Infinity, maxZ = -Infinity;
    hostnames.forEach(h => {
	const p = nodeOriginalPos.get(h);
	if (!p) return;
	minX = Math.min(minX, p.x); maxX = Math.max(maxX, p.x);
	minY = Math.min(minY, p.y); maxY = Math.max(maxY, p.y);
	minZ = Math.min(minZ, p.z); maxZ = Math.max(maxZ, p.z);
    });

    hostnames.forEach((hostname, idx) => {
	const orig = nodeOriginalPos.get(hostname) || new THREE.Vector3();

	if (mode === "blueprint") {
	    targets.set(hostname, orig.clone());
	} else if (mode === "topdown") {
	    targets.set(hostname, new THREE.Vector3(orig.x, 0, orig.z));
	} else if (mode === "rackfront") {
	    targets.set(hostname, new THREE.Vector3(orig.x, orig.y, 0));
	} else if (mode === "line") {
	    targets.set(hostname, new THREE.Vector3((idx - hostnames.length / 2) * 18, 0, 0));
	}
    });

    scene.traverse(obj => {
	if (obj.name === "cabinetBox" || obj.name === "groupBox") {
	    obj.visible = (mode === "blueprint");
	}
    });
    animateNodePositions(targets, 650);

    setTimeout(() => {
	if (mode === "topdown") viewPreset("top");
	else if (mode === "rackfront") viewPreset("front");
	else if (mode === "line") viewPreset("front");
	else resetView();
    }, 680);
}


// ==========================================
// SAVED VIEWS (localStorage)
// ==========================================
const VIEWS_KEY = "mpiVis.savedViews.v1";

function readViews() {
    try { return JSON.parse(localStorage.getItem(VIEWS_KEY) || "{}"); }
    catch { return {}; }
}

function writeViews(views) {
    localStorage.setItem(VIEWS_KEY, JSON.stringify(views));
}

function initSavedViewsUI() {
    const sel = document.getElementById("savedViews");
    if (!sel) return;

    sel.innerHTML = "";
    const views = readViews();
    const names = Object.keys(views).sort();

    if (names.length === 0) {
	const opt = document.createElement("option");
	opt.value = "";
	opt.textContent = "(no saved views)";
	sel.appendChild(opt);
	sel.disabled = true;
	return;
    }

    sel.disabled = false;
    names.forEach(n => {
	const opt = document.createElement("option");
	opt.value = n;
	opt.textContent = n;
	sel.appendChild(opt);
    });
}

function saveCurrentView() {
    const input = document.getElementById("viewName");
    const name = (input?.value || "").trim();
    if (!name) return alert("Enter a view name.");

    const views = readViews();
    const pose = getCameraPose();

    views[name] = {
	position: pose.position.toArray(),
	target: pose.target.toArray(),
	up: pose.up.toArray(),
	layoutMode: currentLayoutMode
    };

    writeViews(views);
    initSavedViewsUI();
}

function loadSelectedView() {
    const sel = document.getElementById("savedViews");
    const name = sel?.value;
    if (!name) return;

    const views = readViews();
    const v = views[name];
    if (!v) return;

    if (v.layoutMode) applyLayout(v.layoutMode);

    const pose = {
	position: new THREE.Vector3().fromArray(v.position),
	target: new THREE.Vector3().fromArray(v.target),
	up: new THREE.Vector3().fromArray(v.up)
    };

    setTimeout(() => animateCameraPose(pose, 650), 680);
}

function deleteSelectedView() {
    const sel = document.getElementById("savedViews");
    const name = sel?.value;
    if (!name) return;

    const views = readViews();
    delete views[name];
    writeViews(views);
    initSavedViewsUI();
}

// ================================
// HARDWARE TOPOLOGY (OPTIMIZED INSTANCING)
// ================================
function buildHardwareTopology() {
    const nodesMap = {};
    const groupBoxes = [];

    const boxW = 7;
    const boxH = 11;
    const boxD = 9;

    if (parsedData.hardware_blueprint) {
        const bp = parsedData.hardware_blueprint;
        let currentCabX = 0;

        if (bp.cabinets && Array.isArray(bp.cabinets)) {
            bp.cabinets.forEach(cabinet => {
                let currentRackX = currentCabX;

                if (cabinet.racks && Array.isArray(cabinet.racks)) {
                    cabinet.racks.forEach(rack => {
                        let rMinX = Infinity, rMaxX = -Infinity;
                        let rMinY = Infinity, rMaxY = -Infinity;

                        if (rack.blades && Array.isArray(rack.blades)) {
                            rack.blades.forEach((blade, bIdx) => {
                                let bMinX = Infinity, bMaxX = -Infinity;
                                let bMinY = Infinity, bMaxY = -Infinity;

                                if (blade.nodes && Array.isArray(blade.nodes)) {
                                    blade.nodes.forEach((node, nIdx) => {
                                        const absoluteX = currentRackX + (nIdx * 8); 
                                        const absoluteY = bIdx * 12; 

                                        nodesMap[node.hostname] = { 
                                            x: absoluteX, y: absoluteY, z: 0, ranks: [],
                                            cpus: node.cpus || 1, coresPerCpu: node.cores_per_cpu || 1 
                                        };

                                        if (absoluteX < bMinX) bMinX = absoluteX;
                                        if (absoluteX > bMaxX) bMaxX = absoluteX;
                                        if (absoluteY < bMinY) bMinY = absoluteY;
                                        if (absoluteY > bMaxY) bMaxY = absoluteY;
                                        
                                        if (absoluteX < rMinX) rMinX = absoluteX;
                                        if (absoluteX > rMaxX) rMaxX = absoluteX;
                                        if (absoluteY < rMinY) rMinY = absoluteY;
                                        if (absoluteY > rMaxY) rMaxY = absoluteY;
                                    });
                                }

                                if (bMinX !== Infinity) {
                                    groupBoxes.push({
                                        type: 'blade',
                                        x: bMinX + (bMaxX - bMinX) / 2, y: bMinY, z: 0,
                                        w: (bMaxX - bMinX) + boxW + 1.5, h: boxH + 1.5, d: boxD + 1.5
                                    });
                                }
                            });
                        }

                        if (rMinX !== Infinity) {
                            groupBoxes.push({
                                type: 'chassis',
                                x: rMinX + (rMaxX - rMinX) / 2, y: rMinY + (rMaxY - rMinY) / 2, z: 0,
                                w: (rMaxX - rMinX) + boxW + 5, h: (rMaxY - rMinY) + boxH + 5, d: boxD + 5
                            });
                            currentRackX += (rMaxX - rMinX) + 25; 
                        }
                    });
                }
                currentCabX = currentRackX + 40; 
            });
        } else {
            Object.keys(bp).forEach(host => {
                const node = bp[host];
                if (host !== "cabinets" && host !== "metadata") {
                    nodesMap[host] = { ranks: [], x: node.x || 0, y: node.y || 0, z: node.z || 0, cpus: node.cpus || 1, coresPerCpu: node.cores_per_cpu || 1 };
                }
            });
        }
    }

    groupBoxes.forEach(g => {
        const geo = new THREE.BoxGeometry(g.w, g.h, g.d);
        const edges = new THREE.EdgesGeometry(geo);
        
        const color = g.type === 'blade' ? 0x4b5563 : 0x8b949e;
        const opacity = g.type === 'blade' ? 0.2 : 0.4;

        const mat = new THREE.LineBasicMaterial({ color: color, transparent: true, opacity: opacity });
        const mesh = new THREE.LineSegments(edges, mat);
        
        mesh.name = "groupBox"; 
        mesh.userData = { isGrouping: true, type: g.type };
        mesh.position.set(g.x, g.y, g.z);
        scene.add(mesh);
    }); 

    if (parsedData.topology && Array.isArray(parsedData.topology)) {
        parsedData.topology.forEach(t => {
            const host = t.hostname || "unknown";
            if (!nodesMap[host]) {
                nodesMap[host] = { x: t.x || 0, y: t.y || 0, z: t.z || 0, ranks: [], cpus: 1, coresPerCpu: 1 };
            }
            if (t.rank !== undefined && t.rank !== null) {
                if (!nodesMap[host].ranks.find(r => r.id === t.rank)) {
                    nodesMap[host].ranks.push({
                        id: t.rank, chip: t.chip !== undefined ? t.chip : 0, core: t.core !== undefined ? t.core : nodesMap[host].ranks.length
                    });
                }
            }
        });
    }

    let maxY = 0, minX = Infinity, maxX = -Infinity;
    let totalNodes = 0;

    const sharedNodeGeo = new THREE.BoxGeometry(boxW, boxH, boxD);
    const sharedNodeEdges = new THREE.EdgesGeometry(sharedNodeGeo);
    
    const sharedActiveNodeMat = new THREE.LineBasicMaterial({ color: 0x58a6ff, transparent: true, opacity: 0.8 }); 
    const sharedIdleNodeMat = new THREE.LineBasicMaterial({ color: 0x4b5563, transparent: true, opacity: 0.2 });   

    const sharedFillGeo = new THREE.BoxGeometry(boxW - 0.2, boxH - 0.2, boxD - 0.2);
    const sharedFillMat = new THREE.MeshBasicMaterial({ color: 0x161b22, transparent: true, opacity: 0.9, polygonOffset: true, polygonOffsetFactor: 1, polygonOffsetUnits: 1 });
    const sharedChipMat = new THREE.MeshBasicMaterial({ color: 0x21262d, transparent: true, opacity: 0.8 });
    
    const sharedIdleCoreMat = new THREE.MeshBasicMaterial({ color: 0x6e7681, transparent: true, opacity: 0.6 });
    // Core Instancing material must use white basic material to allow purely driving colors via instanceColor without lighting interference
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
        scene.add(nodeGroup);
        nData.group = nodeGroup;
        rankToNodeGroup.set(host, nodeGroup);
        nodeMap.set(host, nodeGroup);

        nodeGroup.userData = { host: host };  
        nodeOriginalPos.set(host, nodeGroup.position.clone());

        const isActiveNode = nData.ranks.length > 0;
        const currentNodeMat = isActiveNode ? sharedActiveNodeMat : sharedIdleNodeMat;

        const shellMesh = new THREE.LineSegments(sharedNodeEdges, currentNodeMat);
        shellMesh.name = "mpiNode";
        shellMesh.userData = { hostname: host, isNode: true };
        nodeGroup.add(shellMesh);

        const fillMesh = new THREE.Mesh(sharedFillGeo, sharedFillMat);
        fillMesh.name = "mpiNodeFill";
        fillMesh.userData = { hostname: host, isNode: true };
        nodeGroup.add(fillMesh);

        const numChips = nData.cpus || 1;
        const numCores = nData.coresPerCpu || 1;
        const ranks = nData.ranks.sort((a, b) => a.id - b.id);

        const activeCoreCount = ranks.length;
        const idleCoreCount = (numChips * numCores) - activeCoreCount;

        const chipCols = Math.ceil(Math.sqrt(numChips));
        const chipRows = Math.ceil(numChips / chipCols);
        const chipSpaceX = (boxW - 0.5) / chipCols;
        const chipSpaceY = (boxH - 0.5) / chipRows;

        const coreCols = Math.ceil(Math.sqrt(numCores));
        const coreRows = Math.ceil(numCores / coreCols);
        const maxCoreSpacingX = (chipSpaceX * 0.90) / coreCols; 
        const maxCoreSpacingY = (chipSpaceY * 0.90) / coreRows;
        
        const coreSpacing = Math.min(maxCoreSpacingX, maxCoreSpacingY);
        const coreSize = coreSpacing * 0.80; 

        const chipGeo = new THREE.BoxGeometry(chipSpaceX * 0.9, chipSpaceY * 0.9, 0.2);
        const coreGeo = new THREE.BoxGeometry(coreSize, coreSize, coreSize);
        const actualGridWidth = coreCols * coreSpacing;
        const actualGridHeight = coreRows * coreSpacing;

        // Initialize Instanced Meshes for this Node
        let instActiveCores = null;
        let instIdleCores = null;
        let instChips = new THREE.InstancedMesh(chipGeo, sharedChipMat, numChips);
        nodeGroup.add(instChips);

        const activeRankData = {}; // maps instanceId -> data for raycasting

        if (activeCoreCount > 0) {
            instActiveCores = new THREE.InstancedMesh(coreGeo, sharedActiveRankMat.clone(), activeCoreCount);
            instActiveCores.instanceMatrix.setUsage(THREE.DynamicDrawUsage);
            instActiveCores.instanceColor = new THREE.InstancedBufferAttribute(new Float32Array(activeCoreCount * 3), 3);
            for(let i=0; i<activeCoreCount; i++) instActiveCores.setColorAt(i, defaultRankColor);
            
            instActiveCores.name = "mpiRankInstance";
            instActiveCores.userData = { isInstancedCore: true, host: host, rankData: activeRankData };
            nodeGroup.add(instActiveCores);
        }

        if (idleCoreCount > 0) {
            instIdleCores = new THREE.InstancedMesh(coreGeo, sharedIdleCoreMat, idleCoreCount);
            nodeGroup.add(instIdleCores);
        }

        let chipIdx = 0, activeIdx = 0, idleIdx = 0;

        for (let c = 0; c < numChips; c++) {
            const cRow = Math.floor(c / chipCols);
            const cCol = c % chipCols;
            
            const chipOffsetX = (cCol * chipSpaceX) - ((chipCols - 1) * chipSpaceX) / 2;
            const chipOffsetY = -((cRow * chipSpaceY) - ((chipRows - 1) * chipSpaceY) / 2);
            const chipOffsetZ = (boxD / 2) - 0.3; 

            dummy.position.set(chipOffsetX, chipOffsetY, chipOffsetZ);
            dummy.updateMatrix();
            instChips.setMatrixAt(chipIdx++, dummy.matrix);

            const startX = chipOffsetX - (actualGridWidth / 2) + (coreSpacing / 2);
            const startY = chipOffsetY + (actualGridHeight / 2) - (coreSpacing / 2); 

            for (let i = 0; i < numCores; i++) {
                const iRow = Math.floor(i / coreCols);
                const iCol = i % coreCols;
                
                const coreOffsetX = startX + (iCol * coreSpacing);
                const coreOffsetY = startY - (iRow * coreSpacing);
                const coreOffsetZ = chipOffsetZ + 0.15; 

                const globalSlotIndex = (c * numCores) + i;
                const activeRank = ranks[globalSlotIndex];

                dummy.position.set(coreOffsetX, coreOffsetY, coreOffsetZ);
                dummy.updateMatrix();

                if (activeRank && instActiveCores) {
                    instActiveCores.setMatrixAt(activeIdx, dummy.matrix);
                    
                    activeRankData[activeIdx] = { 
                        rank: activeRank.id, host: host, chip: c, core: i, isCore: true,
                        localPos: dummy.position.clone() 
                    };
                    
                    rankMap.set(activeRank.id, { 
                        mesh: instActiveCores, 
                        instanceId: activeIdx,
                        nodeGroup: nodeGroup,
                        localPos: dummy.position.clone(),
                        depth: coreSize
                    }); 
                    
                    activeIdx++;
                } else if (instIdleCores) {
                    instIdleCores.setMatrixAt(idleIdx++, dummy.matrix);
                }
            }
        }
        
        instChips.instanceMatrix.needsUpdate = true;
        if (instActiveCores) instActiveCores.instanceMatrix.needsUpdate = true;
        if (instIdleCores) instIdleCores.instanceMatrix.needsUpdate = true;
    });

    if (totalNodes > 0) {
        const cabWidth = (maxX - minX);
        const cabCenterX = minX + cabWidth / 2;
        const centerY = maxY / 2;
        const distanceToPullBack = Math.max(300, cabWidth * 1.5, maxY * 1.5);
	
        const floorGrid = scene.getObjectByName("floorGrid");
        if (floorGrid) floorGrid.position.set(cabCenterX, -10, 0);       
	
        camera.position.set(cabCenterX, centerY, distanceToPullBack);
        controls.target.set(cabCenterX, centerY, 0);
        controls.update();
        cacheDefaultCameraPose();
        currentLayoutMode = "blueprint";
    }
}

function onMouseMove(event) {
    if (!tooltipEl) return;

    const rect = renderer.domElement.getBoundingClientRect();
    mouse.x = ((event.clientX - rect.left) / rect.width) * 2 - 1;
    mouse.y = -((event.clientY - rect.top) / rect.height) * 2 + 1;

    raycaster.setFromCamera(mouse, camera);
    const intersects = raycaster.intersectObjects(scene.children, true);

    let hoveredRank = null;
    let hoveredNode = null;

    for (let i = 0; i < intersects.length; i++) {
        const obj = intersects[i].object;
        
        if (obj.userData && obj.userData.isInstancedCore) {
            hoveredRank = obj.userData.rankData[intersects[i].instanceId];
            break; 
        } else if (obj.userData && obj.userData.isNode && !hoveredNode) {
            hoveredNode = obj.userData; 
        }
    }

    const data = hoveredRank || hoveredNode;

    if (data) {
        if (data.isCore) {
            tooltipEl.innerHTML = `
                <strong style="color: #58a6ff; font-size: 1.0rem;">Rank ${data.rank}</strong><br/>
                <hr style="border: 0; border-top: 1px solid #30363d; margin: 6px 0;">
                <span style="color: #8b949e;">Host:</span> ${data.host}<br/>
                <span style="color: #8b949e;">Chip:</span> ${data.chip} | <span style="color: #8b949e;">Core:</span> ${data.core}
            `;
        } else if (data.isNode) {
            tooltipEl.innerHTML = `
                <strong style="color: #8b949e; font-size: 1.0rem;">Node Chassis</strong><br/>
                <hr style="border: 0; border-top: 1px solid #30363d; margin: 6px 0;">
                <span style="color: #8b949e;">Host:</span> <span style="color: #c9d1d9;">${data.hostname}</span>
		`;
        }
        
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
        
        if (object.userData && object.userData.isInstancedCore) {
            const data = object.userData.rankData[intersects[i].instanceId];
            
            // Build a mock object that mimics a standard THREE.Object3D for the Follow logic
            const targetPosition = data.localPos.clone().add(object.parent.position);
            const mockObj = { userData: data, worldPos: targetPosition, name: `Rank ${data.rank}` };
            
            setSelectedObject(mockObj);
            flyCameraTo(targetPosition);
            break;
            
        } else if (object.name === "mpiNode") {
            const selected = object.parent;
            setSelectedObject(selected);
            const targetPosition = new THREE.Vector3();
            selected.getWorldPosition(targetPosition);
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
            controls.enabled = !isFollowEnabled;
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
    
    if (window.AnalyticsUI) {
        AnalyticsUI.updateAnalyticsTimeWindowIndicator(currentTime);
    }

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

    if (timeline && timeline.length > 0) {
        let left = 0, right = timeline.length - 1, mid = 0;
        
        while (left <= right) {
            mid = Math.floor((left + right) / 2);
            if (timeline[mid].time <= currentTime) left = mid + 1;
            else right = mid - 1;
        }
        
        const MAX_VISIBLE_MESSAGES = 800; 
        let eventsCaptured = 0;
 
        for (let i = right; i >= 0; i--) {
            if (timeline[i].time >= minTimeWindow) {
                activeEvents.push(timeline[i]);
                eventsCaptured++;
                if (eventsCaptured >= MAX_VISIBLE_MESSAGES) break; 
            } else break;
        } 
    }

    // Aggregate connections to prevent visual blowout and visualize volume
    const aggregatedConnections = new Map();

    activeEvents.forEach(event => {
        const cat = MPI_CATEGORIES[event.call] || DEFAULT_CATEGORY;
        const sData = rankMap.get(event.sender);
        const rData = rankMap.get(event.receiver);

        // Lighting Logic
        if (event.call !== "MPI_WAIT" && event.call !== "MPI_WAITALL") {
            const color = new THREE.Color(cat.color);
            if (sData) {
                activelyGlowingRanks.set(event.sender, { mesh: sData.mesh, instanceId: sData.instanceId, color: color, intensity: 1.0 });
                sData.mesh.setColorAt(sData.instanceId, color);
                sData.mesh.instanceColor.needsUpdate = true;
            }
            if (rData) {
                activelyGlowingRanks.set(event.receiver, { mesh: rData.mesh, instanceId: rData.instanceId, color: color, intensity: 1.0 });
                rData.mesh.setColorAt(rData.instanceId, color);
                rData.mesh.instanceColor.needsUpdate = true;
            }
        }

        if (event.sender === event.receiver) return;

        // Grouping Logic
        // For collectives, group by chassis to avoid drawing 64 lines for one node.
        // For P2P, group by sender, receiver, AND call type.
        let connectionKey;
        if (cat.type === "collective" && sData && rData) {
           connectionKey = `coll-${sData.nodeGroup.uuid}-${rData.nodeGroup.uuid}-${event.call}`;
        } else {
           connectionKey = `${event.sender}-${event.receiver}-${event.call}`;
        }

        if (!aggregatedConnections.has(connectionKey)) {
            aggregatedConnections.set(connectionKey, {
                sender: event.sender, receiver: event.receiver, call: event.call,
                count: 1, sData: sData, rData: rData
            });
        } else {
            // If the connection already exists this frame, just increment the volume
            aggregatedConnections.get(connectionKey).count++;
        }
    });

    // Draw the aggregated lines
    aggregatedConnections.forEach(conn => {
        const { sender, receiver, call, count, sData, rData } = conn;
        const cat = MPI_CATEGORIES[call] || DEFAULT_CATEGORY;

        if (sData && rData) {
            if (sData.nodeGroup === rData.nodeGroup) {
                const startWorld = sData.localPos.clone().applyMatrix4(sData.nodeGroup.matrixWorld);
                const endWorld = rData.localPos.clone().applyMatrix4(rData.nodeGroup.matrixWorld);
                startWorld.z += (sData.depth / 2) + 0.5; endWorld.z += (rData.depth / 2) + 0.5;
                drawIntraNodeLine(startWorld, endWorld, call, count);
            } else {
                if (cat.type === "collective") {
                    const startWorld = sData.nodeGroup.position.clone();
                    const endWorld = rData.nodeGroup.position.clone();
                    startWorld.z += 5.5; endWorld.z += 5.5;
                    drawInterNodeLine(startWorld, endWorld, call, sender, receiver, count);
                } else {
                    const startWorld = sData.localPos.clone().applyMatrix4(sData.nodeGroup.matrixWorld);
                    const endWorld = rData.localPos.clone().applyMatrix4(rData.nodeGroup.matrixWorld);
                    startWorld.z += (sData.depth / 2) + 0.5; endWorld.z += (rData.depth / 2) + 0.5;
                    drawInterNodeLine(startWorld, endWorld, call, sender, receiver, count);
                }
            }
        }
    });

    return activeEvents;
}

function drawIntraNodeLine(startPos, endPos, callName, msgCount = 1) {
    const midPoint = startPos.clone().lerp(endPos, 0.5);
    const distance = startPos.distanceTo(endPos);
    midPoint.z += Math.max(distance * 0.4, 1.0); 

    // Scale thickness based on volume of messages
    const thicknessMultiplier = 1 + Math.log10(msgCount);
    const radius = 0.04 * thicknessMultiplier;

    const curve = new THREE.QuadraticBezierCurve3(startPos, midPoint, endPos);
    const geometry = new THREE.TubeGeometry(curve, 10, radius, 4, false);
    const material = sharedMaterials[callName + "_tube"] || sharedMaterials["default_tube"];

    const tube = new THREE.Mesh(geometry, material);
    tube.name = "mpiLine";
    scene.add(tube);
    activeLines.push(tube);

    createJunctionPoint(startPos, callName, thicknessMultiplier);
    const tangent = curve.getTangent(1.0).normalize();
    createArrowhead(endPos, tangent, callName, thicknessMultiplier);
}

function drawInterNodeLine(startPos, endPos, callName, sender, receiver, msgCount = 1) {
    const cat = MPI_CATEGORIES[callName] || DEFAULT_CATEGORY;

    const midPoint = startPos.clone().lerp(endPos, 0.5);
    const distance = startPos.distanceTo(endPos);
    const bowDistance = Math.max(distance * 0.3, 8.0); 
    const laneOffset = (sender > receiver) ? 3.0 : -3.0;

    if (cat.type === "collective") {
        midPoint.z += bowDistance * 1.5; 
    } else if (cat.type === "p2p_nonblock") {
        midPoint.z += bowDistance; 
        midPoint.y += laneOffset; 
    } else {
        midPoint.z += bowDistance; 
        midPoint.y -= laneOffset;
    }

    // Scale thickness based on volume of messages
    const thicknessMultiplier = 1 + Math.log10(msgCount);
    const radius = 0.12 * thicknessMultiplier;

    const curve = new THREE.QuadraticBezierCurve3(startPos, midPoint, endPos);
    const geometry = new THREE.TubeGeometry(curve, 12, radius, 4, false);
    const material = sharedMaterials[callName + "_tube"] || sharedMaterials["default_tube"];

    const tube = new THREE.Mesh(geometry, material);
    tube.name = "mpiLine";
    scene.add(tube);
    activeLines.push(tube);

    createJunctionPoint(startPos, callName, thicknessMultiplier);
    const tangent = curve.getTangent(1.0).normalize();
    createArrowhead(endPos, tangent, callName, thicknessMultiplier);
}

function createJunctionPoint(pos, callName, thicknessMultiplier = 1) {
    const mat = sharedMaterials[callName + "_junction"] || sharedMaterials["default_junction"];
    const mesh = new THREE.Mesh(sharedSphereGeo, mat);
    mesh.position.copy(pos);
    
    // Scale the sphere so it doesn't get swallowed by thick tubes
    mesh.scale.set(thicknessMultiplier, thicknessMultiplier, thicknessMultiplier);
    
    scene.add(mesh);
    junctionPoints.push(mesh);
}

function createArrowhead(pos, direction, callName, thicknessMultiplier = 1) {
    const mat = sharedMaterials[callName + "_junction"] || sharedMaterials["default_junction"];
    const mesh = new THREE.Mesh(sharedArrowGeo, mat);
    mesh.position.copy(pos);
    const target = pos.clone().add(direction);
    mesh.lookAt(target);
    
    // Scale the arrowhead so it doesn't get swallowed by thick tubes
    mesh.scale.set(thicknessMultiplier, thicknessMultiplier, thicknessMultiplier);
    
    scene.add(mesh);
    junctionPoints.push(mesh); 
}

function clearLines() {
    activeLines.forEach(line => {
        if (line.geometry) line.geometry.dispose();
        scene.remove(line);
    });
    activeLines = [];

    junctionPoints.forEach(pt => {
        scene.remove(pt);
    });
    junctionPoints = [];
}

function renderMetadata() {
    let container = document.getElementById("metadataContainer");
    
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
            return; 
        }
    }
    
    container.innerHTML = "";
    
    const meta = parsedData.metadata || parsedData.info || {};
    const programName = meta.program || meta.executable || meta.name || "Unknown Program";
    const runDate = meta.date || meta.timestamp || "Unknown Date";
    const systemName = meta.system_name || "Unknown System";
    
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

// ==========================================
// RECORDING
// ==========================================
function pickSupportedMimeType() {
    const candidates = [
	"video/webm;codecs=vp9",
	"video/webm;codecs=vp8",
	"video/webm"
    ];
    return candidates.find(t => window.MediaRecorder?.isTypeSupported?.(t)) || "";
}

function downloadBlob(blob, filename) {
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
}

function getTimestampSlug() {
    const d = new Date();
    const pad = (n) => String(n).padStart(2, "0");
    return `${d.getFullYear()}${pad(d.getMonth()+1)}${pad(d.getDate())}-${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`;
}

async function startUIRecording({ fps = 60, bitsPerSecond = 8_000_000 } = {}) {
    if (uiMediaRecorder) return;

    if (!navigator.mediaDevices?.getDisplayMedia) {
	alert("UI recording not supported: navigator.mediaDevices.getDisplayMedia is unavailable.");
	return;
    }

    uiStream = await navigator.mediaDevices.getDisplayMedia({
	video: {
	    frameRate: fps,
	    width: { ideal: 1920 },
	    height: { ideal: 1080 }
	},
	audio: false
    });

    uiRecordedChunks = [];
    const mimeType = pickSupportedMimeType();
    const options = { bitsPerSecond };
    if (mimeType) options.mimeType = mimeType;

    uiMediaRecorder = new MediaRecorder(uiStream, options);

    uiMediaRecorder.ondataavailable = (e) => {
	if (e.data && e.data.size > 0) uiRecordedChunks.push(e.data);
    };

    uiMediaRecorder.onstop = () => {
	const blob = new Blob(uiRecordedChunks, { type: uiMediaRecorder.mimeType || "video/webm" });
	downloadBlob(blob, `mpi-vis-ui-${getTimestampSlug()}-t${currentTime.toFixed(3)}.webm`);

	uiRecordedChunks = [];
	try { uiStream?.getTracks()?.forEach(t => t.stop()); } catch {}
	uiStream = null;
	uiMediaRecorder = null;

	const status = document.getElementById("recUIStatus");
	if (status) status.textContent = "";
	const startBtn = document.getElementById("btn-rec-ui-start");
	const stopBtn = document.getElementById("btn-rec-ui-stop");
	if (startBtn) startBtn.disabled = false;
	if (stopBtn) stopBtn.disabled = true;
    };

    uiStream.getVideoTracks()[0].addEventListener("ended", () => {
	if (uiMediaRecorder) uiMediaRecorder.stop();
    });

    uiMediaRecorder.start(250);

    const status = document.getElementById("recUIStatus");
    if (status) status.textContent = "Recording UI…";
    const startBtn = document.getElementById("btn-rec-ui-start");
    const stopBtn = document.getElementById("btn-rec-ui-stop");
    if (startBtn) startBtn.disabled = true;
    if (stopBtn) stopBtn.disabled = false;
}

function stopUIRecording() {
    if (!uiMediaRecorder) return;
    uiMediaRecorder.stop();
}

// ======
// LEGEND
// ======
function initLegend() {
    if (document.getElementById("mpiLegend")) return;

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
    legendDiv.style.pointerEvents = "none"; 
    legendDiv.style.boxShadow = "0 4px 12px rgba(0,0,0,0.5)";
    legendDiv.style.backdropFilter = "blur(4px)";

    const title = document.createElement("div");
    title.innerHTML = "<strong>COMMUNICATION TYPES</strong>";
    title.style.marginBottom = "10px";
    title.style.borderBottom = "1px solid #30363d";
    title.style.paddingBottom = "6px";
    title.style.color = "#8b949e";
    legendDiv.appendChild(title);

    const uniqueTypes = {};
    Object.values(MPI_CATEGORIES).forEach(cat => {
        if (!uniqueTypes[cat.type]) uniqueTypes[cat.type] = cat.color;
    });

    const formatName = (str) => {
        if (str === "p2p_block") return "P2P Blocking";
        if (str === "p2p_nonblock") return "P2P Non-Blocking";
        if (str === "state") return "Wait / Sync States";
        if (str === "collective") return "Collectives";
        return str;
    };

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
        colorBox.style.backgroundColor = '#' + hexColor.toString(16).padStart(6, '0');
        colorBox.style.boxShadow = `0 0 5px ${colorBox.style.backgroundColor}`;

        const label = document.createElement("span");
        label.textContent = formatName(type);

        row.appendChild(colorBox);
        row.appendChild(label);
        legendDiv.appendChild(row);
    });

    const container = document.getElementById("visCanvas");
    if (container) {
        container.style.position = "relative";
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
        const tdLabel = document.createElement('td');
        tdLabel.textContent = call.replace('MPI_', '');
        tdLabel.style.color = '#8b949e';
        tdLabel.style.textAlign = 'right';
        tdLabel.style.paddingRight = '8px';
        tr.appendChild(tdLabel);

        Object.entries(stats[call]).forEach(([bin, val]) => {
            const td = document.createElement('td');
            td.style.border = 'none';
            td.style.borderRadius = '3px';
            td.style.height = '20px';
            td.style.width = '35px';
            
            td.title = `${call} (${bin}): ${val} total messages`;
            
            if (val === 0) {
                td.style.backgroundColor = '#161b22';
            } else {
                const intensity = Math.max(0.15, val / maxVal);
                td.style.backgroundColor = `rgba(88, 166, 255, ${intensity})`; 
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

    calls.forEach(call => {
        binsTemplate.forEach(bin => {
            const count = currentCounts[call][bin];
            const td = dynamicCells[call][bin];
            
            // Only rewrite the DOM if the values actually changed
            if (td.dataset.lastCount == count) return; 
            td.dataset.lastCount = count;

            td.title = `${call} (${bin}): ${count} active messages`;
            
            if (count === 0) {
                td.style.backgroundColor = '#161b22';
            } else {
                const denom = globalMax > 0 ? globalMax : 1;
                const intensity = Math.max(0.15, count / denom);
                td.style.backgroundColor = `rgba(46, 160, 67, ${intensity})`; 
            }
        });
    });
}
