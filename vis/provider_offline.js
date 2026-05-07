// ==========================================
// provider_offline.js
// OFFLINE FILE PARSER & PLAYBACK CONTROLLER
// ==========================================

const OfflineProvider = {
    parsedData: null,
    uploadedFilePointer: null,
    currentLoadedChunkIndex: -1,
    headerLengthOffset: 0,
    chunkLoadPromise: null,
    chunkLoadIndexInFlight: -1,

    currentTime: 0, minTime: 0, maxTime: 0, timeMultiplier: 1,
    isPlaying: false, animationFrameId: null, lastFrameTime: 0, lastDynUpdate: 0,

    init: function() {
        document.getElementById("profileLoader")?.addEventListener("change", (e) => this.handleFileUpload(e));
        document.getElementById("btn-play")?.addEventListener("click", () => this.togglePlayback());
        document.getElementById("timeSlider")?.addEventListener("input", (e) => { document.getElementById("currentTimeLabel").textContent = parseFloat(e.target.value).toFixed(3); });
        document.getElementById("timeSlider")?.addEventListener("change", (e) => { this.pausePlayback(); this.seekToTime(parseFloat(e.target.value)); });
        document.getElementById("speedSlider")?.addEventListener("input", (e) => { document.getElementById("speedLabel").textContent = Math.pow(10, parseFloat(e.target.value)).toFixed(3) + "x"; });
        
        VisualizerCore.init("visCanvas");
    },

    decompressBlob: async function(blob) {
        const ds = new DecompressionStream('deflate');
        return await new Response(blob.stream().pipeThrough(ds)).text();
    },

    handleFileUpload: async function(event) {
        this.uploadedFilePointer = event.target.files[0];
        if (!this.uploadedFilePointer) return;

        try {
            const sizeBuf = await this.uploadedFilePointer.slice(0, 4).arrayBuffer();
            this.headerLengthOffset = 4 + new DataView(sizeBuf).getUint32(0, true);
            const headerText = await this.decompressBlob(this.uploadedFilePointer.slice(4, this.headerLengthOffset));
            
            this.parsedData = JSON.parse(headerText);
            this.parsedData.timeline = [];
            this.currentLoadedChunkIndex = -1;
            
            this.initDashboard();
        } catch (error) {
            alert("Failed to parse the MPI profile data.");
            console.error(error);
        }
    },

    initDashboard: function() {
        this.pausePlayback();
        VisualizerCore.clearTopology();

        const chunks = this.parsedData.chunks;
        this.minTime = (chunks && chunks.length > 0) ? chunks[0].t_start : 0;
        this.maxTime = (chunks && chunks.length > 0) ? chunks[chunks.length - 1].t_end : 0;
        this.timeMultiplier = (this.maxTime - this.minTime > 0) ? (this.maxTime - this.minTime) / 10.0 : 1;

        const slider = document.getElementById("timeSlider");
        if (slider) { slider.step = "any"; slider.min = this.minTime; slider.max = this.maxTime; slider.disabled = false; }
        if (document.getElementById("btn-play")) document.getElementById("btn-play").disabled = false;

        // Hand the blueprint off to the core
        VisualizerCore.buildTopology(this.parsedData.hardware_blueprint, this.parsedData.topology, this.parsedData.metadata || this.parsedData.info);
        VisualizerCore.initSpectrograms(this.parsedData.statistics);

        this.seekToTime(this.minTime).catch(err => { console.error(err); this.pausePlayback(); });
    },

    ensureChunkLoadedForTime: async function(time) {
        if (!this.parsedData?.chunks || !this.uploadedFilePointer) return;
        const chunks = this.parsedData.chunks;

        while (true) {
            let targetIndex = chunks.findIndex(c => time <= c.t_end);
            if (targetIndex === -1) targetIndex = chunks.length - 1;
            if (targetIndex === this.currentLoadedChunkIndex) return;

            if (this.chunkLoadPromise) {
                if (this.chunkLoadIndexInFlight === targetIndex) { await this.chunkLoadPromise; return; }
                await this.chunkLoadPromise; continue; 
            }

            this.chunkLoadIndexInFlight = targetIndex;
            const overlay = document.getElementById("loadingOverlay"), loadingText = document.getElementById("loadingText");

            this.chunkLoadPromise = (async () => {
                try {
                    if (overlay) overlay.style.display = "block";
                    if (loadingText) loadingText.textContent = `Unpacking Chunk ${targetIndex + 1}...`;
                    await new Promise(r => setTimeout(r, 0));

                    const c = chunks[targetIndex];
                    const blob = this.uploadedFilePointer.slice(this.headerLengthOffset + c.offset, this.headerLengthOffset + c.offset + c.size);
                    const timeline = JSON.parse(await this.decompressBlob(blob));
                    if (timeline.length > 1) timeline.sort((a,b) => a.time - b.time);

                    this.parsedData.timeline = timeline;
                    this.currentLoadedChunkIndex = targetIndex;
                } finally {
                    if (overlay) overlay.style.display = "none";
                }
            })();

            await this.chunkLoadPromise;
            this.chunkLoadPromise = null;
            this.chunkLoadIndexInFlight = -1;
            return;
        }
    },

    seekToTime: async function(time) {
        this.currentTime = time;
        if (document.getElementById("timeSlider")) document.getElementById("timeSlider").value = this.currentTime;
        if (document.getElementById("currentTimeLabel")) document.getElementById("currentTimeLabel").textContent = this.currentTime.toFixed(3);
        
        await this.ensureChunkLoadedForTime(this.currentTime);
        const activeEvents = this.getActiveEventsForWindow();
        
        VisualizerCore.renderFrame(activeEvents);
        VisualizerCore.updateDynamicSpectrogram(activeEvents, this.parsedData.statistics);
    },

    getActiveEventsForWindow: function() {
        const raw = parseFloat(document.getElementById("speedSlider")?.value || "0");
        const winSize = Math.min(0.2, 0.05 * Math.pow(10, raw));
        const minWin = this.currentTime - winSize;
        const minCollWin = this.currentTime - Math.max(winSize * 8.0, 0.5);
        
        const timeline = this.parsedData.timeline, activeEvents = [];
        if (!timeline || timeline.length === 0) return activeEvents;

        let l = 0, r = timeline.length - 1, m = 0;
        while (l <= r) { m = Math.floor((l+r)/2); if (timeline[m].time <= this.currentTime) l = m + 1; else r = m - 1; }
        
        let captured = 0;
        for (let i = r; i >= 0; i--) {
            const evTime = timeline[i].time;
            if (evTime >= minCollWin) {
                const isColl = (timeline[i].call && timeline[i].call.includes("BCAST")) || (timeline[i].call && timeline[i].call.includes("REDUCE")) || (timeline[i].call && timeline[i].call.includes("GATHER"));
                if (evTime >= minWin || isColl) {
                    activeEvents.push(timeline[i]);
                    if (++captured >= 800) break;
                }
            } else break;
        }
        return activeEvents;
    },

    togglePlayback: function() {
        this.isPlaying = !this.isPlaying;
        const btn = document.getElementById("btn-play");
        if (btn) btn.innerHTML = this.isPlaying ? "<b>|| Pause</b>" : "<b>▶ Play</b>";
        if (this.isPlaying) { this.lastFrameTime = performance.now(); this.playLoop(performance.now()); }
        else cancelAnimationFrame(this.animationFrameId);
    },

    pausePlayback: function() {
        this.isPlaying = false;
        const btn = document.getElementById("btn-play");
        if (btn) btn.innerHTML = "<b>▶ Play</b>";
        cancelAnimationFrame(this.animationFrameId);
    },

    playLoop: async function(timestamp) {
        if (!this.isPlaying) return;
        const dt = Math.min((timestamp - this.lastFrameTime) / 1000, 0.05);
        this.lastFrameTime = timestamp;
        
        const speed = Math.pow(10, parseFloat(document.getElementById("speedSlider")?.value || "0"));
        let nextTime = this.currentTime + (dt * this.timeMultiplier * speed);

        if (nextTime >= this.maxTime) { await this.seekToTime(this.maxTime); this.pausePlayback(); return; }

        this.currentTime = nextTime;
        if (document.getElementById("timeSlider")) document.getElementById("timeSlider").value = this.currentTime;
        if (document.getElementById("currentTimeLabel")) document.getElementById("currentTimeLabel").textContent = this.currentTime.toFixed(3);

        await this.ensureChunkLoadedForTime(this.currentTime);
        const activeEvents = this.getActiveEventsForWindow();
        
        VisualizerCore.renderFrame(activeEvents);
        if (timestamp - this.lastDynUpdate > 100) { VisualizerCore.updateDynamicSpectrogram(activeEvents, this.parsedData.statistics); this.lastDynUpdate = timestamp; }

        if (this.isPlaying) this.animationFrameId = requestAnimationFrame((ts) => this.playLoop(ts));
    }
};

document.addEventListener("DOMContentLoaded", () => OfflineProvider.init());
