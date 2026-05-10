// ==========================================
// provider_live.js
// LIVE TELEMETRY POLLING CONTROLLER
// ==========================================

const LiveProvider = {
    eventBuffer: [],
    lastPolledTime: 0,
    currentTime: 0,
    lastFrameTime: 0,
    lastDynUpdate: 0,
    globalStatsTemplate: {}, 
   
    // Placeholder 
    API_URL: "http://your-telemetry-gateway.local/api", 

    init: async function() {
        VisualiserCore.init("visCanvas");
        
        // Hide offline UI elements if they exist on the page
        ["profileLoader", "timeSlider", "btn-play", "speedSlider", "overallStatsContainer"].forEach(id => {
            const el = document.getElementById(id); 
            if (el) {
                // If the element is a container itself (like overallStats), hide it. 
                // Otherwise, hide its parent (like the slider controls).
                if (id.includes("Stats")) el.style.display = 'none';
                else el.parentElement.style.display = 'none';
            }
        });

        // Generate a fake stats template so the UI knows which calls to build rows for
        Object.keys(MPI_CATEGORIES).forEach(call => {
            this.globalStatsTemplate[call] = { 
                "< 128B": 0, "128B - 1KB": 0, "1KB - 64KB": 0, 
                "64KB - 1MB": 0, "1MB - 16MB": 0, "> 16MB": 0 
            };
        });
        VisualiserCore.initSpectrograms(this.globalStatsTemplate);

        document.getElementById("currentTimeLabel").textContent = "LIVE: Syncing...";

        try {
            // Fetch hardware layout
            const topoRes = await fetch(`${this.API_URL}/topology`);
            const topoData = await topoRes.json();
            VisualiserCore.buildTopology(topoData.hardware_blueprint, topoData.topology, topoData.metadata);
            
            // Sync to DB clock (delay by 2.0s to allow batch inserts to arrive in the DB)
            const syncRes = await fetch(`${this.API_URL}/time`);
            const syncData = await syncRes.json();
            this.currentTime = syncData.current_db_time - 2.0; 
            this.lastPolledTime = this.currentTime;

            // Start Loops
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
                // No need to sort if we trust the DB, but good for safety:
                this.eventBuffer.sort((a,b) => a.time - b.time);
            }
            this.lastPolledTime = pollEnd;
        } catch (e) {
            console.warn("Polling interrupted", e);
        }

        // This stops the 60fps renderLoop from choking on array filtering.
        const pruneTime = this.currentTime - 3.0; 
        this.eventBuffer = this.eventBuffer.filter(e => e.time >= pruneTime);

        // Poll again in 1 second
        setTimeout(() => this.pollDatabase(), 1000);
    },

    getActiveEventsForWindow: function() {
        // Base window sizes (similar to offline mode at 1x speed)
        const winSize = 0.05; 
        const minWin = this.currentTime - winSize;
        const minCollWin = this.currentTime - 0.5; // Collectives linger for 500ms

        // Since we aggressively prune the buffer, filtering is very fast
        return this.eventBuffer.filter(ev => {
            if (ev.time > this.currentTime) return false;
            
            const callType = ev.call || ev.message_type;
            const cat = MPI_CATEGORIES[callType] || DEFAULT_CATEGORY;
            
            if (cat.type === "collective") {
                return ev.time >= minCollWin;
            }
            return ev.time >= minWin;
        });
    },

    renderLoop: function(timestamp) {
        // Advance clock relative to wall time
        const dt = (timestamp - this.lastFrameTime) / 1000.0;
        this.currentTime += dt;
        this.lastFrameTime = timestamp;

        if (document.getElementById("currentTimeLabel")) {
            // Show how far behind the database 'edge' we currently are
            document.getElementById("currentTimeLabel").textContent = `LIVE: T-${(this.lastPolledTime - this.currentTime).toFixed(1)}s`;
        }
        
        // Grab recent events using the Two-Tiered window
        const activeEvents = this.getActiveEventsForWindow();

        VisualiserCore.renderFrame(activeEvents);
        
        // Update the Dynamic Spectrogram every 100ms
        if (timestamp - this.lastDynUpdate > 100) {
            VisualiserCore.updateDynamicSpectrogram(activeEvents, this.globalStatsTemplate);
            this.lastDynUpdate = timestamp;
        }
        
        requestAnimationFrame((ts) => this.renderLoop(ts));
    }
};

document.addEventListener("DOMContentLoaded", () => LiveProvider.init());
