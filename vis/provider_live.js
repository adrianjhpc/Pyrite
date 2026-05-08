// ==========================================
// provider_live.js
// LIVE TELEMETRY POLLING CONTROLLER
// ==========================================

const LiveProvider = {
    eventBuffer: [],
    lastPolledTime: 0,
    currentTime: 0,
    lastFrameTime: 0,
    API_URL: "http://your-telemetry-gateway.local/api", // Update to your FastApi/NodeJS proxy

    init: async function() {
        VisualiserCore.init("visCanvas");
        
        // Hide offline UI elements if they exist on the page
        ["profileLoader", "timeSlider", "btn-play", "speedSlider"].forEach(id => {
            const el = document.getElementById(id); if (el) el.parentElement.style.display = 'none';
        });

        document.getElementById("currentTimeLabel").textContent = "LIVE: Syncing...";

        try {
            // 1. Fetch hardware layout
            const topoRes = await fetch(`${this.API_URL}/topology`);
            const topoData = await topoRes.json();
            VisualiserCore.buildTopology(topoData.hardware_blueprint, topoData.topology, topoData.metadata);
            
            // 2. Sync to DB clock (delay by 2s to allow batch inserts to arrive)
            const syncRes = await fetch(`${this.API_URL}/time`);
            const syncData = await syncRes.json();
            this.currentTime = syncData.current_db_time - 2.0; 
            this.lastPolledTime = this.currentTime;

            // 3. Start Loops
            this.pollDatabase();
            this.lastFrameTime = performance.now();
            this.renderLoop(performance.now());
            
        } catch (e) {
            console.error("Failed to initialize live telemetry stream.", e);
            document.getElementById("currentTimeLabel").textContent = "CONNECTION ERROR";
        }
    },

    pollDatabase: async function() {
        const pollStart = this.lastPolledTime;
        const pollEnd = pollStart + 1.0; 

        try {
            const res = await fetch(`${this.API_URL}/events?start=${pollStart}&end=${pollEnd}`);
            const newEvents = await res.json();
            
            if (newEvents && newEvents.length > 0) {
                this.eventBuffer.push(...newEvents);
                this.eventBuffer.sort((a,b) => a.time - b.time);
                
                // Prune RAM
                if (this.eventBuffer.length > 50000) {
                    this.eventBuffer = this.eventBuffer.slice(-50000);
                }
            }
            this.lastPolledTime = pollEnd;
        } catch (e) {
            console.warn("Polling interrupted", e);
        }

        setTimeout(() => this.pollDatabase(), 1000);
    },

    renderLoop: function(timestamp) {
        // Advance clock relative to wall time
        const dt = (timestamp - this.lastFrameTime) / 1000.0;
        this.currentTime += dt;
        this.lastFrameTime = timestamp;

        if (document.getElementById("currentTimeLabel")) {
            document.getElementById("currentTimeLabel").textContent = `LIVE: T-${(this.lastPolledTime - this.currentTime).toFixed(1)}s`;
        }

        const winSize = 0.1; // 100ms trailing laser tails
        const minWin = this.currentTime - winSize;
        
        // Grab recent events from RAM
        const activeEvents = this.eventBuffer.filter(e => e.time <= this.currentTime && e.time >= minWin);

        VisualiserCore.renderFrame(activeEvents);
        
        requestAnimationFrame((ts) => this.renderLoop(ts));
    }
};

document.addEventListener("DOMContentLoaded", () => LiveProvider.init());
