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

document.addEventListener("DOMContentLoaded", () => {
    initThreeJS();
    document.getElementById("profileLoader").addEventListener("change", handleFileUpload);
    document.getElementById("timeSlider").addEventListener("input", handleManualSeek);
    document.getElementById("btn-play").addEventListener("click", togglePlayback);
});

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

function handleFileUpload(event) {
    const file = event.target.files[0];
    if (!file) return;

    const reader = new FileReader();
    reader.onload = (e) => {
        try {
            parsedData = JSON.parse(e.target.result);
            initDashboard();
        } catch (error) {
            console.error(error);
        }
    };
    reader.readAsText(file);
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

    const timeline = parsedData.timeline;
    const topology = parsedData.topology;

    maxTime = timeline.length > 0 ? timeline[timeline.length - 1].time : 0;

    if (maxTime > 0) {
         timeMultiplier = maxTime / 10.0;
    } else {
         timeMultiplier = 1;
    }    

    document.getElementById("timeSlider").step = (maxTime / 1000).toString();
    document.getElementById("timeSlider").max = maxTime;
    document.getElementById("timeSlider").disabled = false;
    document.getElementById("btn-play").disabled = false;

    buildHardwareTopology(topology);
    seekToTime(0);

    renderSpectrogram();
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

function renderSpectrogram() {
    const stats = parsedData.statistics;
    if (!stats) return;

    const container = document.getElementById('spectrogramContainer');
    container.innerHTML = ''; // Clear previous data

    const calls = Object.keys(stats);
    if (calls.length === 0) return;

    const bins = Object.keys(stats[calls[0]]); // e.g., ["< 128B", "128B < 1KB", "1KB - 64KB", ...]

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

function handleManualSeek(event) {
    pausePlayback();
    seekToTime(parseFloat(event.target.value));
}

function seekToTime(time) {
    currentTime = time;
    document.getElementById("timeSlider").value = currentTime;
    document.getElementById("currentTimeLabel").textContent = currentTime.toFixed(3);
    renderActiveCommunications();
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
function playLoop(timestamp) {
    if (!isPlaying) return;
    
    // Standard delta time calculation (how long since the last screen refresh)
    const deltaTime = (timestamp - lastFrameTime) / 1000; 
    lastFrameTime = timestamp;
    
    const speed = parseFloat(document.getElementById("speedSlider").value);
    
    // Calculate the next time based on our smart multiplier
    let nextTime = currentTime + (deltaTime * timeMultiplier * speed);

    if (nextTime >= maxTime) {
        seekToTime(maxTime);
        pausePlayback();
        return;
    }
    seekToTime(nextTime);
    animationFrameId = requestAnimationFrame(playLoop);
}
