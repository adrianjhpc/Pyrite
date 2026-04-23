// Global State
let parsedData = null;
let nodeMap = new Map();

// Playback State
let isPlaying = false;
let currentTime = 0;
let maxTime = 0;
let animationFrameId = null;
const TIME_WINDOW = 0.05; 

// Three.js Core Variables
let scene, camera, renderer, controls;
let linesGroup;

let timeMultiplier = 1;

let dynamicCells = {}; // Fast lookup for the HTML table cells
let lastDynUpdate = 0; // Throttle timer for the play loop

let uploadedFilePointer = null;
let currentLoadedChunkIndex = -1; // Keep track of what chunk is currently in RAM
let headerLengthOffset = 0;       // Math offset for finding chunks

document.addEventListener("DOMContentLoaded", () => {
    initThreeJS();
    document.getElementById("profileLoader").addEventListener("change", handleFileUpload);
    
    // Just update the text label visually while dragging so it feels responsive
    document.getElementById("timeSlider").addEventListener("input", (e) => {
        document.getElementById("currentTimeLabel").textContent = parseFloat(e.target.value).toFixed(3);
    });
    
    // When the user lets go of the mouse, actually do the heavy chunk load
    document.getElementById("timeSlider").addEventListener("change", handleManualSeek);
    
    document.getElementById("btn-play").addEventListener("click", togglePlayback);
});

async function decompressBlob(blob) {
    const ds = new DecompressionStream('deflate'); // zlib uses deflate
    const decompressedStream = blob.stream().pipeThrough(ds);
    return await new Response(decompressedStream).text();
}

function initThreeJS() {
    const container = document.getElementById('visCanvas');
    
    scene = new THREE.Scene();
    scene.background = new THREE.Color(0x0d1117);

    // Camera setup
    camera = new THREE.PerspectiveCamera(60, container.clientWidth / container.clientHeight, 1, 1000);
    camera.position.set(0, 50, 150);

    // Renderer setup
    renderer = new THREE.WebGLRenderer({ antialias: true });
    renderer.setSize(container.clientWidth, container.clientHeight);
    container.appendChild(renderer.domElement);

    // Controls setup (allows dragging to rotate, scrolling to zoom)
    controls = new THREE.OrbitControls(camera, renderer.domElement);
    controls.enableDamping = true;
    controls.dampingFactor = 0.05;

    // Lights
    const ambientLight = new THREE.AmbientLight(0xffffff, 0.4);
    scene.add(ambientLight);
    const pointLight = new THREE.PointLight(0x58a6ff, 1);
    pointLight.position.set(50, 100, 50);
    scene.add(pointLight);

    // Group to hold active communication lines
    linesGroup = new THREE.Group();
    scene.add(linesGroup);

    // Start render loop
    const animate = function () {
        requestAnimationFrame(animate);
        controls.update();
        renderer.render(scene, camera);
    };
    animate();

    // Handle window resize
    window.addEventListener('resize', () => {
        camera.aspect = container.clientWidth / container.clientHeight;
        camera.updateProjectionMatrix();
        renderer.setSize(container.clientWidth, container.clientHeight);
    });
}


async function handleFileUpload(event) {
    uploadedFilePointer = event.target.files[0];
    if (!uploadedFilePointer) return;

    try {
        // Read the first 4 bytes to find out how big the header is
        const sizeBuf = await uploadedFilePointer.slice(0, 4).arrayBuffer();
        const headerSize = new DataView(sizeBuf).getUint32(0, true); // little-endian
        
        headerLengthOffset = 4 + headerSize; // Where the chunks start in the file

        // Slice out the header, decompress it, and parse it
        const headerBlob = uploadedFilePointer.slice(4, headerLengthOffset);
        const headerText = await decompressBlob(headerBlob);
        
        // parsedData now contains everything except the timeline
        parsedData = JSON.parse(headerText);
        parsedData.timeline = []; // Initialize empty timeline
        
        initDashboard();
        
    } catch (error) {
        console.error("Failed to unpack the .mpix container:", error);
    }
}

function initDashboard() {
    pausePlayback();
    nodeMap.clear();

    // Clear existing nodes and lines from the scene
    const objectsToRemove = [];
    scene.traverse(child => {
        if (child.name === "mpiNode" || child.name === "cabinetBox") {
            objectsToRemove.push(child);
        }
    });
    objectsToRemove.forEach(obj => scene.remove(obj));
    clearLines();

    const topology = parsedData.topology;

    const chunks = parsedData.chunks;
    maxTime = (chunks && chunks.length > 0) ? chunks[chunks.length - 1].t_end : 0;

    if (maxTime > 0) {
         timeMultiplier = maxTime / 10.0;
    } else {
         timeMultiplier = 1;
    }    

    // Use "any" so the browser doesn't snap or lock the slider
    document.getElementById("timeSlider").step = "any"; 
    document.getElementById("timeSlider").max = maxTime;
    document.getElementById("timeSlider").disabled = false;
    document.getElementById("btn-play").disabled = false;

    buildHardwareTopology(topology);
    
    // We must build the tables before we seek to 0
    renderSpectrogram();
    initDynamicSpectrogram();
    
    seekToTime(0);
}

// Add a hostname map to the top of your file with the other globals
let hostnameMap = new Map();

function buildHardwareTopology(nodesData) {
    const nodeGeometry = new THREE.BoxGeometry(12, 2, 12); 

    // Aesthetic Materials
    const idleMaterial = new THREE.MeshPhongMaterial({ 
        color: 0x8b949e,       // Lighter gray
        transparent: true, 
        opacity: 0.15,         // Very sheer
        depthWrite: false,     // Fixes overlapping transparency glitches
        blending: THREE.AdditiveBlending // Gives them a slight holographic look
    });
    const allocatedMaterial = new THREE.MeshPhongMaterial({ 
        color: 0x8b949e, emissive: 0x000000 
    });

    let maxY = 0;
    hostnameMap.clear();

    // Draw the Datacenter Floor Grid
    const gridHelper = new THREE.GridHelper(1000, 50, 0x30363d, 0x161b22);
    gridHelper.position.y = -10; // Put it slightly beneath the lowest node
    scene.add(gridHelper);

    // Draw the full hardware blueprint (Idle Nodes & Racks)
    if (parsedData.hardware_blueprint && parsedData.hardware_blueprint.cabinets) {
        parsedData.hardware_blueprint.cabinets.forEach(cab => {
            cab.racks.forEach(rack => {
                const rackGroup = new THREE.Group();
                scene.add(rackGroup);

                rack.nodes.forEach(node => {
                    const x = cab.x + rack.x_offset;
                    const y = node.slot * 4; // Height
                    const z = cab.z + rack.z_offset;

                    // Create idle node
                    const mesh = new THREE.Mesh(nodeGeometry, idleMaterial.clone());
                    mesh.position.set(x, y, z);
                    mesh.name = "mpiNode";
                    rackGroup.add(mesh);

                    // Track highest Y for camera centering
                    if (y > maxY) maxY = y;

                    // Save to hostname map so we can find it later
                    hostnameMap.set(node.hostname, { x, y, z, mesh: mesh, rank: null });
                });

                // Wrap the Rack in a visible wireframe bounding box
                const rackBox = new THREE.BoxHelper(rackGroup, 0x30363d);
                scene.add(rackBox);
            });
        });
    }

    // Highlight the Allocated MPI Ranks
    nodesData.forEach(d => {
        let target = hostnameMap.get(d.hostname);
        
        if (target) {
            // Upgrade the material from "Idle" to "Allocated"
            target.mesh.material = allocatedMaterial.clone();
            target.rank = d.rank;
            // Map the Rank ID to this specific mesh for line drawing
            nodeMap.set(d.rank, target);
        } else {
            // Fallback: If hardware map is missing, just draw them randomly
            const mesh = new THREE.Mesh(nodeGeometry, allocatedMaterial.clone());
            mesh.position.set(d.x, d.y, d.z);
            scene.add(mesh);
            nodeMap.set(d.rank, { x: d.x, y: d.y, z: d.z, mesh: mesh });
        }
    });

    // Center Camera
    const centerY = maxY / 2;
    camera.position.set(0, centerY, maxY > 0 ? maxY * 1.2 : 150);
    controls.target.set(0, centerY, 0);
    controls.update();
}

function initDynamicSpectrogram() {
    const stats = parsedData.statistics;
    if (!stats) return;

    const container = document.getElementById('dynamicSpectrogramContainer');
    container.innerHTML = ''; 
    dynamicCells = {}; 

    const calls = Object.keys(stats);
    if (calls.length === 0) return;
    const bins = Object.keys(stats[calls[0]]);

    const table = document.createElement('table');
    table.style.borderCollapse = 'collapse';
    table.style.width = '100%';
    table.style.color = '#c9d1d9';
    table.style.fontSize = '0.75rem';
    table.style.fontFamily = "'Fira Code', monospace";

    // Header Row
    const thead = document.createElement('tr');
    thead.appendChild(document.createElement('th')); 
    bins.forEach(b => {
        const th = document.createElement('th');
        th.textContent = b.replace(' - ', '\n');
        th.style.padding = '4px 2px';
        th.style.textAlign = 'center';
        th.style.color = '#8b949e';
        th.style.fontWeight = 'normal';
        th.style.whiteSpace = 'pre-wrap';
        thead.appendChild(th);
    });
    table.appendChild(thead);

    // Data Rows
    calls.forEach(call => {
        const tr = document.createElement('tr');
        dynamicCells[call] = {}; // Initialize fast lookup for this row
        
        const tdLabel = document.createElement('td');
        tdLabel.textContent = call.replace('MPI_', ''); 
        tdLabel.style.padding = '4px 8px 4px 0';
        tdLabel.style.textAlign = 'right';
        tdLabel.style.color = '#8b949e';
        tdLabel.style.fontWeight = '600';
        tr.appendChild(tdLabel);

        bins.forEach(bin => {
            const td = document.createElement('td');
            td.style.backgroundColor = `rgba(46, 160, 67, 0)`; // Start empty (Green scale)
            td.style.border = '1px solid #30363d';
            td.style.height = '24px';
            
            dynamicCells[call][bin] = td; // Save reference for fast updates
            tr.appendChild(td);
        });
        table.appendChild(tr);
    });
    container.appendChild(table);
}

function updateDynamicSpectrogram(currentVisualTime) {
    if (!parsedData || !dynamicCells) return;

    const stats = parsedData.statistics;
    const calls = Object.keys(stats);
    if (calls.length === 0) return;
    const binsTemplate = Object.keys(stats[calls[0]]);

    // Initialize empty counts for the current slice of time
    let currentCounts = {};
    calls.forEach(c => {
        currentCounts[c] = {};
        binsTemplate.forEach(b => currentCounts[c][b] = 0);
    });

    // Tally all messages up to the current time slider position
    for (let i = 0; i < parsedData.timeline.length; i++) {
        const event = parsedData.timeline[i];
        if (event.time > currentVisualTime) break; // Timeline is chronological, so we stop early
        
        const call = event.call;
        if (currentCounts[call] !== undefined) {
            const bytes = event.bytes || 0;
            if (bytes < 128) currentCounts[call]["< 128B"]++;
            else if (bytes < 1024) currentCounts[call]["128B - 1KB"]++;
            else if (bytes < 65536) currentCounts[call]["1KB - 64KB"]++;
            else if (bytes < 1048576) currentCounts[call]["64KB - 1MB"]++;
            else if (bytes < 16777216) currentCounts[call]["1MB - 16MB"]++;
            else currentCounts[call]["> 16MB"]++;
        }
    }

    // Find the global max from the STATIC stats so the colors fill up proportionally
    let globalMax = 0;
    calls.forEach(c => binsTemplate.forEach(b => {
        if (stats[c][b] > globalMax) globalMax = stats[c][b];
    }));

    // Update the background colors of our pre-built HTML table
    calls.forEach(call => {
        binsTemplate.forEach(bin => {
            const count = currentCounts[call][bin];
            const td = dynamicCells[call][bin];

            let intensity = 0;
            if (count > 0) {
                intensity = Math.max(0.15, Math.log10(count + 1) / Math.log10(globalMax + 1));
            }
            // Use GitHub Success Green to distinguish from the blue static map
            td.style.backgroundColor = `rgba(46, 160, 67, ${intensity})`; 
            td.title = `${call} | ${bin}:\n${count.toLocaleString()} messages (Accumulated)`; 
        });
    });
}

function renderSpectrogram() {
    const stats = parsedData.statistics;
    if (!stats) return;

    const container = document.getElementById('spectrogramContainer');
    container.innerHTML = ''; // Clear previous data

    const calls = Object.keys(stats);
    if (calls.length === 0) return;

    const bins = Object.keys(stats[calls[0]]); // e.g., ["< 128B", "128B -  1KB", "1KB - 64KB", ...]

    // Find the global maximum to scale our colors properly
    let maxCount = 0;
    calls.forEach(c => {
        bins.forEach(b => {
            if (stats[c][b] > maxCount) maxCount = stats[c][b];
        });
    });

    // Build the HTML Table
    const table = document.createElement('table');
    table.style.borderCollapse = 'collapse';
    table.style.width = '100%';
    table.style.color = '#c9d1d9';
    table.style.fontSize = '0.75rem';
    table.style.fontFamily = "'Fira Code', monospace";

    // Create the Header Row (Size Bins)
    const thead = document.createElement('tr');
    thead.appendChild(document.createElement('th')); // Empty top-left corner
    bins.forEach(b => {
        const th = document.createElement('th');
        th.textContent = b.replace(' - ', '\n'); // Wrap text for space
        th.style.padding = '4px 2px';
        th.style.textAlign = 'center';
        th.style.color = '#8b949e';
        th.style.fontWeight = 'normal';
        th.style.whiteSpace = 'pre-wrap'; // Allow newline
        thead.appendChild(th);
    });
    table.appendChild(thead);

    // Create the Data Rows (MPI Functions)
    calls.forEach(call => {
        const tr = document.createElement('tr');
        
        // Row Label (Remove "MPI_" to save horizontal space)
        const tdLabel = document.createElement('td');
        tdLabel.textContent = call.replace('MPI_', ''); 
        tdLabel.style.padding = '4px 8px 4px 0';
        tdLabel.style.textAlign = 'right';
        tdLabel.style.color = '#8b949e';
        tdLabel.style.fontWeight = '600';
        tr.appendChild(tdLabel);

        // Heatmap Cells
        bins.forEach(bin => {
            const count = stats[call][bin] || 0;
            const td = document.createElement('td');

            // Logarithmic color scaling (0.0 to 1.0 intensity)
            let intensity = 0;
            if (count > 0) {
                // Math.max ensures even 1 message gets a faint color (0.15) instead of being invisible
                intensity = Math.max(0.15, Math.log10(count + 1) / Math.log10(maxCount + 1));
            }

            td.style.backgroundColor = `rgba(88, 166, 255, ${intensity})`; // GitHub Blue
            td.style.border = '1px solid #30363d'; // Cell grid lines
            td.style.height = '24px';
            
            // Add a native hover tooltip so users can see the exact number
            td.title = `${call} | ${bin}:\n${count.toLocaleString()} messages`; 

            // This adds numbers inside the boxes
            // td.textContent = count > 0 ? count : '';
            // td.style.textAlign = 'center';
            // td.style.color = intensity > 0.5 ? '#0d1117' : '#c9d1d9'; // Dynamic text contrast

            tr.appendChild(td);
        });
        table.appendChild(tr);
    });

    container.appendChild(table);
}

function renderActiveCommunications() {
    clearLines();

    // Messages happen every ~3 microseconds. A window of 10 microseconds (0.000010) 
    // is perfect to see 3-4 boxes flying across the gap simultaneously.
    const DYNAMIC_WINDOW = 0.000010;

    const activeEvents = parsedData.timeline.filter(d => 
        d.time <= currentTime && d.time >= (currentTime - DYNAMIC_WINDOW)
        && d.sender !== d.receiver 
    );

    const wireMaterial = new THREE.LineBasicMaterial({ 
        color: 0x58a6ff, 
        transparent: true, 
        opacity: 0.15,
        blending: THREE.AdditiveBlending 
    });

    const packetGeometry = new THREE.BoxGeometry(2.5, 2.5, 2.5);
    const packetMaterial = new THREE.MeshBasicMaterial({ color: 0xffffff });


    activeEvents.forEach(event => {
        const sender = nodeMap.get(event.sender);
        const receiver = nodeMap.get(event.receiver);

        if (sender && receiver) {
            const vStart = new THREE.Vector3(sender.x, sender.y, sender.z);
            const vEnd = new THREE.Vector3(receiver.x, receiver.y, receiver.z);
            
            const distance = vStart.distanceTo(vEnd);
            const vMid = new THREE.Vector3().addVectors(vStart, vEnd).multiplyScalar(0.5);
            
            const laneOffset = (event.sender < event.receiver) ? 1 : -1;
            const bulgeAmount = Math.max(20, distance * 0.4); 
            vMid.x += bulgeAmount * laneOffset; 
            vMid.z += bulgeAmount * 0.2 * laneOffset; 

            const curve = new THREE.QuadraticBezierCurve3(vStart, vMid, vEnd);
            
            // Only draw the faint wire once per direction to prevent it from glowing too brightly 
            // when multiple messages are overlapping on the exact same curve.
            if (event === activeEvents.find(e => e.sender === event.sender)) {
                const points = curve.getPoints(20);
                const geometry = new THREE.BufferGeometry().setFromPoints(points);
                const line = new THREE.Line(geometry, wireMaterial);
                linesGroup.add(line);
            }
            
            const ageOfMessage = currentTime - event.time;
            let progress = ageOfMessage / DYNAMIC_WINDOW;
            
            // Draw every single packet that is currently in flight
            if (progress >= 0 && progress <= 1.0) {
                const packetPos = curve.getPoint(progress);
                const packet = new THREE.Mesh(packetGeometry, packetMaterial);
                packet.position.copy(packetPos);
                linesGroup.add(packet);
            }

            sender.mesh.material.emissive.setHex(0x1f6feb); 
            receiver.mesh.material.emissive.setHex(0x2ea043); 
        }
    });

    nodeMap.forEach((data, rank) => {
        const isActive = activeEvents.some(e => e.sender === rank || e.receiver === rank);
        if (!isActive) {
            data.mesh.material.emissive.setHex(0x000000);
        }
    });
}

async function handleManualSeek(event) {
    pausePlayback();
    // Await the new chunk loading before updating the UI
    await seekToTime(parseFloat(event.target.value));
}

async function seekToTime(time) {
    currentTime = time;
    document.getElementById("timeSlider").value = currentTime;
    document.getElementById("currentTimeLabel").textContent = currentTime.toFixed(3);
    
    // Check if we need to load a new chunk from disk
    await ensureChunkLoadedForTime(currentTime);
    
    renderActiveCommunications();
    updateDynamicSpectrogram(currentTime);
}

async function ensureChunkLoadedForTime(time) {
    const chunks = parsedData.chunks;
    if (!chunks) return;

    // Find which chunk this timestamp belongs to
    let targetIndex = chunks.findIndex(c => time >= c.t_start && time <= c.t_end);
    
    // Fallback: If time is before the first chunk or after the last
    if (targetIndex === -1 && time < chunks[0].t_start) targetIndex = 0;
    if (targetIndex === -1 && time > chunks[chunks.length - 1].t_end) targetIndex = chunks.length - 1;

    // If we already have this chunk in RAM, do nothing!
    if (targetIndex === currentLoadedChunkIndex) return;

    // We need a new chunk! Pause playback while we read from the hard drive
    const wasPlaying = isPlaying;
    if (wasPlaying) pausePlayback();

    const chunk = chunks[targetIndex];
    
    // Calculate exact absolute byte position in the file
    const absoluteStart = headerLengthOffset + chunk.offset;
    const absoluteEnd = absoluteStart + chunk.size;

    // Slice -> Unzip -> Parse
    const chunkBlob = uploadedFilePointer.slice(absoluteStart, absoluteEnd);
    const chunkText = await decompressBlob(chunkBlob);
    
    // Swap the old 500k array out of RAM, let the Garbage Collector eat it, and load the new one
    parsedData.timeline = JSON.parse(chunkText);
    currentLoadedChunkIndex = targetIndex;

    console.log(`Loaded Chunk ${targetIndex + 1}/${chunks.length} into memory.`);

    if (wasPlaying) togglePlayback();
}

async function ensureChunkLoadedForTime(time) {
    const chunks = parsedData.chunks;
    if (!chunks) return;

    // Find which chunk this timestamp belongs to
    let targetIndex = chunks.findIndex(c => time >= c.t_start && time <= c.t_end);
    
    // Fallback: If time is before the first chunk or after the last
    if (targetIndex === -1 && time < chunks[0].t_start) targetIndex = 0;
    if (targetIndex === -1 && time > chunks[chunks.length - 1].t_end) targetIndex = chunks.length - 1;

    // If we already have this chunk in RAM, do nothing!
    if (targetIndex === currentLoadedChunkIndex) return;

    // We need a new chunk! Pause playback.
    const wasPlaying = isPlaying;
    if (wasPlaying) pausePlayback();

    // Show a loading overlay
    const overlay = document.getElementById('loadingOverlay');
    const loadingText = document.getElementById('loadingText');
    overlay.style.display = 'block';
    loadingText.textContent = `Unpacking Chunk ${targetIndex + 1} of ${chunks.length}...`;

    // Give the browser 1 frame to draw the UI before we lock the CPU
//    await new Promise(resolve => requestAnimationFrame(resolve));
    await new Promise(resolve => setTimeout(resolve, 15));

    try {
        const chunk = chunks[targetIndex];
        
        // Calculate exact absolute byte position in the file
        const absoluteStart = headerLengthOffset + chunk.offset;
        const absoluteEnd = absoluteStart + chunk.size;

        // Slice -> Unzip -> Parse
        const chunkBlob = uploadedFilePointer.slice(absoluteStart, absoluteEnd);
        const chunkText = await decompressBlob(chunkBlob);
        
        // Swap the arrays
        parsedData.timeline = JSON.parse(chunkText);
        currentLoadedChunkIndex = targetIndex;

        console.log(`Loaded Chunk ${targetIndex + 1}/${chunks.length} into memory.`);
    } catch (error) {
        console.error("Error loading chunk:", error);
    } finally {
        // Hide the overlay (even if it crashes, we must hide it)
        overlay.style.display = 'none';
    }

    if (wasPlaying) togglePlayback();
}

function clearLines() {
    while(linesGroup.children.length > 0){ 
        linesGroup.remove(linesGroup.children[0]); 
    }
}

// Playback logic remains exactly the same as the 2D version
function togglePlayback() {
    if (isPlaying) pausePlayback();
    else {
        isPlaying = true;
        document.getElementById("btn-play").innerHTML = "⏸ Pause";
        lastFrameTime = performance.now();
        animationFrameId = requestAnimationFrame(playLoop);
    }
}

function pausePlayback() {
    isPlaying = false;
    document.getElementById("btn-play").innerHTML = "▶ Play";
    if (animationFrameId) cancelAnimationFrame(animationFrameId);
}

let lastFrameTime = 0;

let isProcessingFrame = false;

async function playLoop(timestamp) {
    if (!isPlaying) return;

    // Prevent overlapping frames if chunk loading takes longer than a standard monitor refresh
    if (isProcessingFrame) return;
    isProcessingFrame = true;

    const deltaTime = (timestamp - lastFrameTime) / 1000;
    lastFrameTime = timestamp;

    // Max visual jump is 50ms, protecting against chunk-load stutter
    const cappedDelta = Math.min(deltaTime, 0.05);

    const speed = parseFloat(document.getElementById("speedSlider").value);
    let nextTime = currentTime + (cappedDelta * timeMultiplier * speed);

    if (timestamp - lastDynUpdate > 100) {
        updateDynamicSpectrogram(nextTime);
        lastDynUpdate = timestamp;
    }

    if (nextTime >= maxTime) {
        await seekToTime(maxTime);
        pausePlayback();
        isProcessingFrame = false;
        return;
    }

    // Await the seek. The loop will graciously pause here if it has to unzip a chunk
    await seekToTime(nextTime);

    isProcessingFrame = false;

    // Check isPlaying again, just in case the user hit pause while the chunk was loading
    if (isPlaying) {
        animationFrameId = requestAnimationFrame(playLoop);
    }
}
