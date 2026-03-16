/**
 * NodusEdge Dashboard — Client
 *
 * SSE client, tab logic, rendering, audio playback.
 * Zero external dependencies.
 */

(function () {
    'use strict';

    const MAX_FEED_ITEMS = 200;
    let feedCount = 0;
    let currentAudio = null;
    let currentPlayBtn = null;

    /** Return auth headers for mutative dashboard requests. */
    function authHeaders() {
        const token = localStorage.getItem('dashboardToken');
        if (!token) return {};
        return { 'Authorization': 'Bearer ' + token };
    }

    /** Wrap fetch for mutative endpoints — prompt for token on 401. */
    async function authFetch(url, opts = {}) {
        opts.headers = Object.assign({}, opts.headers || {}, authHeaders());
        let resp = await fetch(url, opts);
        if (resp.status === 401) {
            const token = prompt('Dashboard token required:');
            if (token) {
                localStorage.setItem('dashboardToken', token);
                opts.headers['Authorization'] = 'Bearer ' + token;
                resp = await fetch(url, opts);
            }
        }
        return resp;
    }

    // Playback speed — persisted in localStorage (ecosystem-wide key)
    const SPEED_KEY = 'nodus-playback-speed';
    const SPEED_OPTIONS = [0.5, 0.75, 1, 1.25, 1.5, 2];
    let playbackSpeed = parseFloat(localStorage.getItem(SPEED_KEY)) || 1;

    function setPlaybackSpeed(speed) {
        playbackSpeed = speed;
        localStorage.setItem(SPEED_KEY, speed);
        if (currentAudio) currentAudio.playbackRate = speed;
        // Update all speed selectors on the page
        document.querySelectorAll('.speed-select').forEach(sel => { sel.value = speed; });
    }

    function buildSpeedSelect() {
        const sel = document.createElement('select');
        sel.className = 'speed-select';
        SPEED_OPTIONS.forEach(s => {
            const opt = document.createElement('option');
            opt.value = s;
            opt.textContent = s + 'x';
            if (s === playbackSpeed) opt.selected = true;
            sel.appendChild(opt);
        });
        sel.addEventListener('change', function () { setPlaybackSpeed(parseFloat(this.value)); });
        return sel;
    }

    // Header clock state
    let _uptimeBase = null;   // seconds from API
    let _uptimeStamp = null;  // Date.now() when seeded
    let _localTz = '';        // IANA timezone from config
    let _synapseConfigured = false; // whether synapse endpoint is set

    // Debug tab state
    let _debugData = null;
    let _debugFilter = null;  // current outcome filter or null

    // Log console state
    let _logEventSource = null;
    let _logLines = [];
    let _logAutoScroll = true;
    const LOG_MAX_LINES = 500;

    // Settings tab state
    let _envOriginal = {};    // key -> original value (for dirty tracking)

    // ========== Tab Navigation ==========

    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
            document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
            btn.classList.add('active');
            const tab = btn.dataset.tab;
            document.getElementById('tab-' + tab).classList.add('active');

            // Refresh tab data on switch
            if (tab === 'frequencies') refreshFrequencies();
            else if (tab === 'traffic') refreshTraffic();
            else if (tab === 'status') refreshStatus();
            else if (tab === 'spectrum') initSpectrum();
            else if (tab === 'debug') { refreshDebug(); connectLogSSE(); }
            else if (tab === 'settings') refreshSettings();
            else if (tab === 'support') refreshSupport();

            // Disconnect log SSE when leaving debug tab
            if (tab !== 'debug') disconnectLogSSE();
        });
    });

    // ========== SSE Connection ==========

    function connectSSE() {
        const es = new EventSource('/events');

        es.onmessage = function (e) {
            try {
                const data = JSON.parse(e.data);
                if (data.__type === 'notification') {
                    addNotification({
                        title: data.title || 'NodusRF',
                        body: data.body || '',
                        type: 'server_push',
                        id: data.id,
                        created_at: data.created_at,
                    });
                    return;
                }
                addSegmentToFeed(data);
                updateSpectrumFromSegment(data);
                updateSpectrumMapFromSegment(data);
            } catch (err) {
                console.error('SSE parse error:', err);
            }
        };

        es.onerror = function () {
            // EventSource auto-reconnects
            console.log('SSE connection lost, reconnecting...');
        };
    }

    // ========== Live Feed ==========

    function addSegmentToFeed(seg) {
        const feedList = document.getElementById('feed-list');
        const empty = document.getElementById('feed-empty');
        if (empty) empty.remove();

        const card = document.createElement('div');
        card.className = 'segment-card new';

        const rfChannel = seg.rf_channel || {};
        const freqHz = rfChannel.frequency_hz || 0;
        const freqMhz = (freqHz / 1000000).toFixed(3);
        const repeater = rfChannel.repeater_callsign || '';
        const transcription = seg.transcription || {};
        const text = transcription.text || '';
        const duration = (seg.audio || {}).duration_seconds;
        const callsigns = seg.detected_callsigns || [];
        const audioBase64 = (seg.audio || {}).audio_data_base64 || '';
        const signalType = seg.signal_type || '';
        const timestamp = formatTime(seg.timestamp);

        // Header
        let headerHtml = `
            <span class="segment-time">${timestamp}</span>
            <span class="segment-freq">${freqMhz} MHz</span>
        `;
        if (repeater) {
            headerHtml += `<span class="segment-repeater">${repeater}</span>`;
        }
        if (signalType) {
            headerHtml += `<span class="segment-signal-type">${signalType}</span>`;
        }
        if (duration) {
            headerHtml += `<span class="segment-duration">${duration.toFixed(1)}s</span>`;
        }

        // Text
        let textHtml;
        if (text) {
            textHtml = highlightCallsigns(text, callsigns);
        } else {
            textHtml = '<span class="segment-text audio-only">[Audio only]</span>';
        }

        // Footer
        let footerHtml = '';
        const hasAudio = !!audioBase64;
        if (hasAudio) {
            const btnId = 'play-' + (seg.segment_id || Math.random().toString(36).slice(2));
            footerHtml += `<button class="play-btn" id="${btnId}" data-audio="${audioBase64}">&#9654; Play</button>`;
            footerHtml += '<span class="speed-slot"></span>';
        }
        if (callsigns.length > 0) {
            footerHtml += '<div class="callsign-list">';
            callsigns.forEach(cs => {
                footerHtml += `<span class="callsign-tag">${escapeHtml(cs)}</span>`;
            });
            footerHtml += '</div>';
        }

        card.innerHTML = `
            <div class="segment-header">${headerHtml}</div>
            <div class="segment-text">${text ? textHtml : '<span class="audio-only">[Audio only]</span>'}</div>
            <div class="segment-footer">${footerHtml}</div>
        `;

        feedList.insertBefore(card, feedList.firstChild);

        // Attach play handler + speed selector
        if (hasAudio) {
            card.querySelector('.play-btn').addEventListener('click', function () {
                playAudio(this, audioBase64);
            });
            const slot = card.querySelector('.speed-slot');
            if (slot) slot.appendChild(buildSpeedSelect());
        }

        // Cap DOM elements
        feedCount++;
        updateFeedCount();
        while (feedList.children.length > MAX_FEED_ITEMS) {
            feedList.removeChild(feedList.lastChild);
        }

        // Remove animation class after it plays
        setTimeout(() => card.classList.remove('new'), 400);
    }

    function updateFeedCount() {
        document.getElementById('feed-count').textContent = feedCount;
    }

    // ========== Spectrum Coverage Map ==========

    // Ham band definitions: sub-regions within each band
    const SPECTRUM_BANDS = [
        {
            id: '6m', label: '6m', range: [50000000, 54000000], color: '#f472b6',
            regions: [
                { start: 50000000, end: 50100000, label: 'CW' },
                { start: 50100000, end: 50300000, label: 'SSB' },
                { start: 51000000, end: 51100000, label: 'Repeater In' },
                { start: 52000000, end: 52050000, label: 'Simplex' },
                { start: 53000000, end: 53100000, label: 'Repeater Out' },
            ],
        },
        {
            id: '2m', label: '2m', range: [144000000, 148000000], color: '#60a5fa',
            regions: [
                { start: 144000000, end: 144200000, label: 'CW' },
                { start: 144200000, end: 144400000, label: 'SSB' },
                { start: 145100000, end: 145500000, label: 'Rptr In' },
                { start: 146520000, end: 146580000, label: 'Simplex' },
                { start: 146610000, end: 147000000, label: 'Rptr Out' },
                { start: 147000000, end: 147390000, label: 'Rptr Out' },
            ],
        },
        {
            id: '220', label: '1.25m', range: [222000000, 225000000], color: '#34d399',
            regions: [
                { start: 223400000, end: 223520000, label: 'Simplex' },
                { start: 223520000, end: 224980000, label: 'Repeater' },
            ],
        },
        {
            id: '70cm', label: '70cm', range: [420000000, 450000000], color: '#a78bfa',
            regions: [
                { start: 420000000, end: 426000000, label: 'ATV' },
                { start: 432000000, end: 432100000, label: 'CW' },
                { start: 442000000, end: 445000000, label: 'Rptr In' },
                { start: 446000000, end: 446025000, label: 'Simplex' },
                { start: 447000000, end: 450000000, label: 'Rptr Out' },
            ],
        },
    ];

    // Track frequency state: { freq_hz -> { segments, lastHeard, signalDb } }
    const _spectrumFreqs = {};

    function updateSpectrumMapFromSegment(seg) {
        const freqHz = seg.rf_channel?.frequency_hz || seg.frequency_hz || 0;
        if (!freqHz) return;
        if (!_spectrumFreqs[freqHz]) {
            _spectrumFreqs[freqHz] = { segments: 0, lastHeard: null, signalDb: null };
        }
        _spectrumFreqs[freqHz].segments++;
        _spectrumFreqs[freqHz].lastHeard = new Date().toISOString();
        const sig = seg.rf_channel?.signal_strength_db;
        if (sig != null) _spectrumFreqs[freqHz].signalDb = sig;
        renderSpectrumMap();
    }

    function initSpectrumMapFromFrequencies(freqData) {
        // Clear
        Object.keys(_spectrumFreqs).forEach(k => delete _spectrumFreqs[k]);
        const freqs = freqData.frequencies || {};
        Object.values(freqs).forEach(f => {
            _spectrumFreqs[f.frequency_hz] = {
                segments: f.count || 0,
                lastHeard: f.last_heard || null,
                signalDb: f.avg_signal_db || null,
            };
        });
        renderSpectrumMap();
    }

    function renderSpectrumMap() {
        const container = document.getElementById('spectrum-map');
        if (!container) return;

        const now = Date.now();
        let totalCovered = 0;
        let totalActive = 0;
        let totalWeak = 0;

        let html = '<div class="spectrum-map-title">Spectrum Coverage</div>';

        SPECTRUM_BANDS.forEach(band => {
            const [bStart, bEnd] = band.range;
            const bSpan = bEnd - bStart;

            // Find frequencies in this band
            const bandFreqs = [];
            for (const [fStr, data] of Object.entries(_spectrumFreqs)) {
                const fhz = parseInt(fStr);
                if (fhz >= bStart && fhz <= bEnd) {
                    const isActive = data.lastHeard && (now - new Date(data.lastHeard).getTime()) < 300000;
                    const isWeak = data.signalDb != null && data.signalDb < -80;
                    bandFreqs.push({ fhz, isActive, isWeak, ...data });
                    totalCovered++;
                    if (isActive) totalActive++;
                    if (isWeak) totalWeak++;
                }
            }

            const activeCount = bandFreqs.filter(f => f.isActive).length;

            // Band header
            html += `<div class="spectrum-band-row">`;
            html += `<div class="spectrum-band-header">`;
            html += `<span class="spectrum-band-name" style="color:${band.color}">${band.label}</span>`;
            html += `<span class="spectrum-band-range">${(bStart/1e6).toFixed(0)}&ndash;${(bEnd/1e6).toFixed(0)} MHz</span>`;
            html += `<span class="spectrum-band-stats">`;
            html += `<span class="active-count">${activeCount}</span>/${bandFreqs.length} active`;
            html += `</span>`;
            html += `</div>`;

            // Strip
            html += `<div class="spectrum-strip">`;

            // Sub-band regions
            band.regions.forEach(r => {
                const left = ((r.start - bStart) / bSpan) * 100;
                const width = ((r.end - r.start) / bSpan) * 100;
                html += `<div class="spectrum-region" style="left:${left}%;width:${width}%">`;
                html += `<span class="spectrum-region-label">${r.label}</span>`;
                html += `</div>`;
            });

            // Frequency ticks
            bandFreqs.forEach(f => {
                const left = ((f.fhz - bStart) / bSpan) * 100;
                const classes = ['spectrum-tick'];
                if (f.segments > 0) classes.push('covered');
                if (f.isActive) classes.push('active');
                if (f.isWeak) classes.push('weak');
                const mhz = (f.fhz / 1e6).toFixed(3);
                const sigLabel = f.signalDb != null ? ` / ${f.signalDb.toFixed(0)} dB` : '';
                const title = `${mhz} MHz: ${f.segments} seg${sigLabel}`;
                html += `<div class="${classes.join(' ')}" style="left:${left}%;background:${band.color};color:${band.color}" title="${title}"></div>`;
            });

            html += `</div>`; // strip

            // Scale labels
            html += `<div class="spectrum-scale">`;
            const steps = 5;
            for (let i = 0; i <= steps; i++) {
                const mhz = (bStart + (bSpan * i / steps)) / 1e6;
                html += `<span>${mhz.toFixed(1)}</span>`;
            }
            html += `</div>`;

            html += `</div>`; // band-row
        });

        // Summary
        html += `<div class="spectrum-summary">`;
        html += `<span class="spectrum-summary-item"><span class="spectrum-legend-dot" style="background:#60a5fa;box-shadow:0 0 4px #60a5fa"></span> Active (heard &lt;5m)</span>`;
        html += `<span class="spectrum-summary-item"><span class="spectrum-legend-dot" style="background:#60a5fa;opacity:0.5"></span> Covered</span>`;
        html += `<span class="spectrum-summary-item"><span class="spectrum-legend-dot" style="background:repeating-linear-gradient(45deg,#eab308,#eab308 1px,transparent 1px,transparent 3px)"></span> Weak signal</span>`;
        html += `<span class="spectrum-summary-item">${totalCovered} freqs monitored, ${totalActive} active, ${totalWeak} weak</span>`;
        html += `</div>`;

        container.innerHTML = html;
    }

    function highlightCallsigns(text, callsigns) {
        let html = escapeHtml(text);
        callsigns.forEach(cs => {
            const escaped = escapeHtml(cs);
            const re = new RegExp('\\b' + escaped + '\\b', 'gi');
            html = html.replace(re, `<span class="callsign">${escaped}</span>`);
        });
        return html;
    }

    function formatTime(ts) {
        if (!ts) return '--:--:--';
        const d = new Date(ts);
        return d.toLocaleTimeString('en-US', { hour12: false });
    }

    function timeAgo(isoStr) {
        if (!isoStr) return '--';
        const then = new Date(isoStr).getTime();
        const now = Date.now();
        const diff = Math.floor((now - then) / 1000);
        if (diff < 60) return diff + 's ago';
        if (diff < 3600) return Math.floor(diff / 60) + 'm ago';
        if (diff < 86400) return Math.floor(diff / 3600) + 'h ago';
        return Math.floor(diff / 86400) + 'd ago';
    }

    // ========== Audio Playback ==========

    function playAudio(btn, base64) {
        // Stop current playback
        if (currentAudio) {
            currentAudio.pause();
            currentAudio = null;
            if (currentPlayBtn) {
                currentPlayBtn.classList.remove('playing');
                currentPlayBtn.innerHTML = '&#9654; Play';
            }
        }

        if (btn === currentPlayBtn) {
            currentPlayBtn = null;
            return;
        }

        const audio = new Audio('data:audio/mpeg;base64,' + base64);
        audio.playbackRate = playbackSpeed;
        audio.play().catch(err => console.error('Audio play error:', err));
        currentAudio = audio;
        currentPlayBtn = btn;
        btn.classList.add('playing');
        btn.innerHTML = '&#9724; Stop';

        audio.onended = function () {
            btn.classList.remove('playing');
            btn.innerHTML = '&#9654; Play';
            currentAudio = null;
            currentPlayBtn = null;
        };
    }

    function playAudioFile(btn, filename) {
        // Stop current playback
        if (currentAudio) {
            currentAudio.pause();
            currentAudio = null;
            if (currentPlayBtn) {
                currentPlayBtn.classList.remove('playing');
                currentPlayBtn.innerHTML = '&#9654;';
            }
        }

        if (btn === currentPlayBtn) {
            currentPlayBtn = null;
            return;
        }

        const audio = new Audio('/api/audio/' + encodeURIComponent(filename));
        audio.playbackRate = playbackSpeed;
        audio.play().catch(err => console.error('Audio file play error:', err));
        currentAudio = audio;
        currentPlayBtn = btn;
        if (btn) {
            btn.classList.add('playing');
            btn.innerHTML = '&#9724;';
        }

        audio.onended = function () {
            if (btn) {
                btn.classList.remove('playing');
                btn.innerHTML = '&#9654;';
            }
            currentAudio = null;
            currentPlayBtn = null;
        };
    }

    // ========== Frequencies Tab ==========

    async function refreshFrequencies() {
        try {
            const resp = await fetch('/api/frequencies');
            const data = await resp.json();
            renderFrequencies(data);
        } catch (err) {
            console.error('Frequencies fetch error:', err);
        }
    }

    function renderFrequencies(data) {
        const container = document.getElementById('freq-content');
        const freqs = data.frequencies || {};
        const synced = data.synced;
        const entries = Object.values(freqs).sort((a, b) => b.count - a.count);

        if (entries.length === 0) {
            container.innerHTML = `
                <div class="empty-state">
                    <h3>No frequency data yet</h3>
                    <p>Frequency stats will populate as segments arrive.</p>
                </div>
            `;
            return;
        }

        let html = `<table class="freq-table">
            <thead><tr>
                <th></th>
                <th>Freq</th>
                <th>Repeater</th>
                <th>PL</th>
                <th>Last Heard</th>
                <th>Segments</th>
                ${synced ? '<th>Upcoming Net</th>' : ''}
            </tr></thead><tbody>`;

        entries.forEach(f => {
            const isActive = f.last_heard && (Date.now() - new Date(f.last_heard).getTime()) < 300000;
            const dotClass = isActive ? 'active' : 'quiet';
            const repeater = f.repeater_callsign || (f.frequency_mhz === 146.52 ? 'Simplex' : '');
            const pl = f.pl_tone || '--';
            const lastHeard = timeAgo(f.last_heard);
            const net = f.upcoming_net || '--';

            html += `<tr>
                <td><span class="status-dot ${dotClass}"></span></td>
                <td><span class="segment-freq">${f.frequency_mhz.toFixed(3)}</span></td>
                <td>${escapeHtml(repeater)}</td>
                <td>${escapeHtml(String(pl))}</td>
                <td>${lastHeard}</td>
                <td>${f.count}</td>
                ${synced ? `<td>${escapeHtml(String(net))}</td>` : ''}
            </tr>`;
        });

        html += '</tbody></table>';

        if (!synced) {
            html += `
                <div class="sync-prompt">
                    <p>Connect to NodusNet to load repeater and net data for your area.</p>
                    <button class="btn btn-primary" onclick="document.querySelector('[data-tab=status]').click()">Go to Status</button>
                </div>
            `;
        }

        container.innerHTML = html;
    }

    // ========== Traffic Tab ==========

    async function refreshTraffic() {
        try {
            const resp = await fetch('/api/traffic');
            const data = await resp.json();
            renderTraffic(data);
        } catch (err) {
            console.error('Traffic fetch error:', err);
        }
    }

    function renderTraffic(data) {
        document.getElementById('stat-segments').textContent = data.today_segments || 0;
        document.getElementById('stat-callsigns').textContent = data.today_unique_callsigns || 0;
        document.getElementById('stat-stored').textContent = data.total_stored || 0;

        // Hourly bar chart
        const hourly = data.hourly || {};
        const chartEl = document.getElementById('hourly-chart');
        const hours = Object.keys(hourly).sort();
        const maxVal = Math.max(1, ...Object.values(hourly));

        let chartHtml = '';
        hours.forEach(h => {
            const val = hourly[h] || 0;
            const pct = (val / maxVal) * 100;
            chartHtml += `
                <div class="bar-wrapper" title="${h}: ${val} segments">
                    <div class="bar" style="height: ${Math.max(2, pct)}%"></div>
                    <span class="bar-label">${h.replace(':00', '')}</span>
                </div>
            `;
        });
        chartEl.innerHTML = chartHtml;

        // Top frequencies
        const topList = document.getElementById('top-freq-list');
        const topFreqs = data.top_frequencies || [];
        if (topFreqs.length === 0) {
            topList.innerHTML = '<li class="top-freq-item" style="color:var(--text-muted)">No data yet</li>';
        } else {
            let listHtml = '';
            topFreqs.forEach((f, i) => {
                listHtml += `
                    <li class="top-freq-item">
                        <span><span class="segment-freq">${f.frequency_mhz.toFixed(3)} MHz</span></span>
                        <span style="color:var(--text-secondary)">${f.count} segments</span>
                    </li>
                `;
            });
            topList.innerHTML = listHtml;
        }
    }

    // ========== Debug Tab ==========

    async function refreshDebug(outcomeFilter) {
        try {
            const resp = await fetch('/api/debug');
            _debugData = await resp.json();
            _debugFilter = outcomeFilter || null;
            renderDebugStats(_debugData);
            renderDebugFilters(_debugData);
            renderDebugAudit(_debugData, _debugFilter);
            updateLogPushButton(_debugData.has_rem);

            // Insert speed selector once
            const slot = document.getElementById('audit-speed-slot');
            if (slot && !slot.hasChildNodes()) slot.appendChild(buildSpeedSelect());

            // Fetch SDR status (non-blocking)
            refreshSdrStatus();
        } catch (err) {
            console.error('Debug fetch error:', err);
        }
    }

    async function refreshSdrStatus() {
        try {
            const resp = await fetch('/api/sdr-config');
            if (resp.ok) renderSdrStatus(await resp.json());
        } catch (err) {
            console.debug('SDR status fetch error:', err);
        }
    }

    function renderSdrStatus(data) {
        const grid = document.getElementById('sdr-info-grid');
        if (!grid) return;

        function fmtDuration(s) {
            if (s == null) return '--';
            if (s < 60) return Math.round(s) + 's';
            if (s < 3600) return Math.floor(s / 60) + 'm ' + Math.round(s % 60) + 's';
            const h = Math.floor(s / 3600);
            const m = Math.floor((s % 3600) / 60);
            return h + 'h ' + m + 'm';
        }

        const alive = data.process_alive;
        const items = [
            { label: 'Process', value: alive ? 'Running' : 'Stopped', cls: alive ? 'good' : 'error' },
            { label: 'PID', value: data.process_pid || '--' },
            { label: 'Uptime', value: fmtDuration(data.process_uptime_seconds) },
            { label: 'Squelch', value: data.squelch_db != null ? data.squelch_db.toFixed(0) + ' dBFS' : '--' },
            { label: 'Channels', value: data.active_channels || '--' },
            { label: 'Center Freq', value: data.center_freq_mhz ? data.center_freq_mhz.toFixed(3) + ' MHz' : '--' },
            { label: 'FFT Size', value: data.fft_size || '--' },
            { label: 'Gain', value: data.gain || '--' },
            { label: 'Device', value: data.device_index != null ? data.device_index : '--' },
        ];

        grid.innerHTML = items.map(function (item) {
            const cls = item.cls ? ' ' + item.cls : '';
            return '<div class="sdr-info-item">'
                + '<span class="sdr-info-label">' + escapeHtml(item.label) + '</span>'
                + '<span class="sdr-info-value' + cls + '">' + escapeHtml(String(item.value)) + '</span>'
                + '</div>';
        }).join('');

        const configPre = document.getElementById('sdr-config-contents');
        if (configPre) configPre.textContent = data.config_file_contents || '(no config)';

        const stderrPre = document.getElementById('sdr-stderr-contents');
        const stderrCount = document.getElementById('sdr-stderr-count');
        const lines = data.recent_stderr || [];
        if (stderrPre) {
            stderrPre.textContent = lines.length > 0 ? lines.join('\n') : '(no output)';
            stderrPre.scrollTop = stderrPre.scrollHeight;
        }
        if (stderrCount) stderrCount.textContent = '(' + lines.length + ' lines)';
    }

    function renderDebugStats(data) {
        const container = document.getElementById('debug-stats');
        const stats = data.stats || {};
        const metrics = data.metrics || {};
        const scanner = stats.scanner || {};

        const cards = [
            { label: 'Spillover Dropped', value: scanner.spillover_dropped || 0 },
            { label: 'Pass Rate', value: metrics.pass_rate !== undefined ? (metrics.pass_rate * 100).toFixed(0) + '%' : '--' },
            { label: 'Watchdog Restarts', value: scanner.watchdog_restarts || 0 },
            { label: 'Whisper', value: stats.whisper_available ? 'Up' : 'Down' },
        ];

        container.innerHTML = cards.map(c => `
            <div class="stat-card">
                <div class="stat-label">${c.label}</div>
                <div class="stat-value">${c.value}</div>
            </div>
        `).join('');
    }

    function renderDebugFilters(data) {
        const container = document.getElementById('debug-filters');
        const metrics = data.metrics || {};
        const counts = metrics.outcome_counts || {};

        const filters = [
            { key: 'beacon', label: 'Beacon', count: counts.beacon || 0 },
            { key: 'kerchunk', label: 'Kerchunk', count: counts.kerchunk || 0 },
            { key: 'hallucination', label: 'Hallucination', count: counts.hallucination || 0 },
            { key: 'error', label: 'Errors', count: counts.error || 0 },
        ];

        container.innerHTML = filters.map(f => `
            <div class="filter-card ${_debugFilter === f.key ? 'selected' : ''}" data-outcome="${f.key}">
                <span class="filter-value">${f.count}</span>
                <span class="filter-label">${f.label}</span>
            </div>
        `).join('');

        // Attach click handlers
        container.querySelectorAll('.filter-card').forEach(card => {
            card.addEventListener('click', function () {
                const outcome = this.dataset.outcome;
                if (_debugFilter === outcome) {
                    // Deselect
                    _debugFilter = null;
                } else {
                    _debugFilter = outcome;
                }
                // Re-render filters and audit with new filter
                renderDebugFilters(_debugData);
                renderDebugAudit(_debugData, _debugFilter);
            });
        });
    }

    function renderDebugAudit(data, outcomeFilter) {
        const tbody = document.getElementById('debug-audit-body');
        const audit = data.audit || {};
        let entries = audit.entries || [];

        if (outcomeFilter) {
            entries = entries.filter(e => e.outcome === outcomeFilter);
        }

        if (entries.length === 0) {
            tbody.innerHTML = '<tr><td colspan="6" style="text-align:center;color:var(--text-muted);padding:20px">No entries</td></tr>';
            return;
        }

        window._auditPayloads = [];
        tbody.innerHTML = entries.slice(0, 50).map(e => {
            const time = formatTime(e.timestamp);
            const freqMhz = e.frequency_hz ? (e.frequency_hz / 1_000_000).toFixed(3) : '--';
            const outcome = e.outcome || 'unknown';
            const badgeClass = outcomeClass(outcome);
            const text = escapeHtml((e.text || '').substring(0, 80));
            const audioFile = e.audio_file || '';
            const playBtn = audioFile
                ? `<button class="audit-play-btn" onclick="window._playAudioFile('${escapeHtml(audioFile)}', this)">&#9654;</button>`
                : '';

            const payloadIdx = window._auditPayloads.length;
            window._auditPayloads.push(e);

            return `<tr>
                <td>${time}</td>
                <td><span class="segment-freq">${freqMhz}</span></td>
                <td><span class="outcome-badge ${badgeClass}">${outcome}</span></td>
                <td title="${escapeHtml(e.text || '')}">${text}</td>
                <td>${playBtn}</td>
                <td><button class="payload-btn" onclick="window._showPayload(${payloadIdx})">JSON</button></td>
            </tr>`;
        }).join('');
    }

    function outcomeClass(outcome) {
        if (outcome === 'pass') return 'pass';
        if (outcome === 'beacon') return 'beacon';
        if (outcome === 'kerchunk') return 'kerchunk';
        if (outcome === 'hallucination') return 'hallucination';
        if (outcome === 'error') return 'error';
        return 'filtered';
    }

    // Expose for inline onclick
    window._playAudioFile = function (filename, btnEl) { playAudioFile(btnEl || null, filename); };

    // ========== Settings Tab ==========

    async function refreshSettings() {
        // Load current squelch from spectrum endpoint
        try {
            const specResp = await fetch('/api/spectrum');
            const specData = await specResp.json();
            const slider = document.getElementById('squelch-slider');
            const valLabel = document.getElementById('squelch-value');
            const recLabel = document.getElementById('squelch-recommended-label');

            if (specData.squelch_db !== undefined) {
                slider.value = specData.squelch_db;
                valLabel.textContent = parseFloat(specData.squelch_db).toFixed(0);
            }

            if (specData.recommended_squelch_db !== undefined) {
                recLabel.innerHTML = '<span class="squelch-recommended-marker">Recommended: ' + specData.recommended_squelch_db.toFixed(1) + ' dBFS</span>';
            } else {
                recLabel.textContent = '';
            }
        } catch (err) {
            // Non-critical
        }

        // Load .env fields
        try {
            const envResp = await fetch('/api/env');
            const envData = await envResp.json();
            renderEnvEditor(envData);
        } catch (err) {
            console.error('Env fetch error:', err);
        }
    }

    function renderEnvEditor(data) {
        const container = document.getElementById('env-editor');
        const fields = data.fields || [];
        const categories = data.categories || [];

        // Group by category
        const grouped = {};
        categories.forEach(cat => { grouped[cat] = []; });
        fields.forEach(f => {
            if (!grouped[f.category]) grouped[f.category] = [];
            grouped[f.category].push(f);
            _envOriginal[f.key] = f.value;
        });

        let html = '';
        categories.forEach(cat => {
            const catFields = grouped[cat];
            if (!catFields || catFields.length === 0) return;
            html += `<div class="env-category">
                <div class="env-category-title">${escapeHtml(cat)}</div>`;
            catFields.forEach(f => {
                html += `<div class="env-field">
                    <label title="${escapeHtml(f.key)}">${escapeHtml(f.label)}</label>
                    <input class="env-input" data-key="${escapeHtml(f.key)}" value="${escapeHtml(f.value)}">
                </div>`;
            });
            html += '</div>';
        });

        container.innerHTML = html;

        // Dirty tracking
        container.querySelectorAll('.env-input').forEach(input => {
            input.addEventListener('input', function () {
                const key = this.dataset.key;
                if (this.value !== _envOriginal[key]) {
                    this.classList.add('changed');
                } else {
                    this.classList.remove('changed');
                }
                updateEnvSaveBtn();
            });
        });
    }

    function updateEnvSaveBtn() {
        const changed = document.querySelectorAll('.env-input.changed');
        const btn = document.getElementById('env-save-btn');
        btn.disabled = changed.length === 0;
    }

    // Squelch slider — debounced to prevent rapid update_squelch() calls
    // that can crash rtl_airband and wedge the USB device (issue #354).
    let _squelchDebounceTimer = null;
    let _squelchRequestInFlight = false;

    document.getElementById('squelch-slider').addEventListener('input', function () {
        document.getElementById('squelch-value').textContent = parseFloat(this.value).toFixed(1);
    });

    document.getElementById('squelch-apply').addEventListener('click', function () {
        const btn = this;
        // Ignore clicks while a request is already in flight
        if (_squelchRequestInFlight) return;
        // Debounce: reset timer on each click, only fire after 1s of no clicks
        if (_squelchDebounceTimer) clearTimeout(_squelchDebounceTimer);
        btn.disabled = true;
        btn.textContent = 'Applying...';
        _squelchDebounceTimer = setTimeout(() => _applySquelch(btn), 1000);
    });

    async function _applySquelch(btn) {
        const slider = document.getElementById('squelch-slider');
        const squelchDb = parseFloat(slider.value);
        _squelchRequestInFlight = true;
        try {
            const resp = await authFetch('/api/squelch', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ squelch_db: squelchDb }),
            });
            const data = await resp.json();
            if (data.applied) {
                btn.textContent = 'Applied';
            } else {
                btn.textContent = 'Error';
            }
        } catch (err) {
            btn.textContent = 'Error';
        } finally {
            _squelchRequestInFlight = false;
            setTimeout(() => { btn.textContent = 'Apply'; btn.disabled = false; }, 2000);
        }
    }

    // Save .env
    document.getElementById('env-save-btn').addEventListener('click', async function () {
        const btn = this;
        const statusEl = document.getElementById('env-save-status');
        const changedInputs = document.querySelectorAll('.env-input.changed');

        if (changedInputs.length === 0) return;

        const fields = {};
        changedInputs.forEach(input => {
            fields[input.dataset.key] = input.value;
        });

        btn.disabled = true;
        btn.textContent = 'Saving...';
        statusEl.textContent = '';

        try {
            const resp = await authFetch('/api/env', {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ fields }),
            });
            const data = await resp.json();

            if (resp.ok) {
                // Update original values
                changedInputs.forEach(input => {
                    _envOriginal[input.dataset.key] = input.value;
                    input.classList.remove('changed');
                });
                statusEl.textContent = 'Saved';
                setTimeout(() => { statusEl.textContent = ''; }, 3000);

                // Show restart banner
                document.getElementById('restart-banner').style.display = 'flex';
            } else {
                statusEl.textContent = 'Error: ' + (data.error || 'Unknown');
                statusEl.style.color = 'var(--red)';
            }
        } catch (err) {
            statusEl.textContent = 'Error: ' + err.message;
            statusEl.style.color = 'var(--red)';
        } finally {
            btn.textContent = 'Save Changes';
            btn.disabled = false;
            updateEnvSaveBtn();
        }
    });

    // Restart button
    document.getElementById('restart-btn').addEventListener('click', async function () {
        if (!confirm('Restart the container? This will briefly interrupt scanning.')) return;
        try {
            await authFetch('/api/restart', { method: 'POST' });
            this.textContent = 'Signal sent';
            this.disabled = true;
        } catch (err) {
            alert('Restart error: ' + err.message);
        }
    });

    // SDR Status toggle
    document.getElementById('sdr-toggle-btn').addEventListener('click', function () {
        const body = document.getElementById('sdr-status-body');
        const expanded = body.style.display !== 'none';
        body.style.display = expanded ? 'none' : 'block';
        this.classList.toggle('expanded', !expanded);
        if (!expanded) refreshSdrStatus();
    });

    // ========== Log Console ==========

    function connectLogSSE() {
        if (_logEventSource) return;
        _logEventSource = new EventSource('/events/logs');
        _logEventSource.onmessage = function (e) {
            try {
                var data = JSON.parse(e.data);
                appendLogLine(data.line);
            } catch (err) {
                // ignore malformed events
            }
        };
        _logEventSource.onerror = function () {
            // EventSource auto-reconnects
        };
    }

    function disconnectLogSSE() {
        if (_logEventSource) {
            _logEventSource.close();
            _logEventSource = null;
        }
    }

    function appendLogLine(text) {
        _logLines.push(text);
        if (_logLines.length > LOG_MAX_LINES) {
            _logLines.shift();
        }

        var container = document.getElementById('log-terminal-content');
        if (!container) return;

        var div = document.createElement('div');
        div.className = 'log-line';
        div.textContent = text;
        container.appendChild(div);

        // Cap DOM elements
        while (container.children.length > LOG_MAX_LINES) {
            container.removeChild(container.firstChild);
        }

        // Update line count
        var countEl = document.getElementById('log-line-count');
        if (countEl) countEl.textContent = _logLines.length + ' lines';

        // Auto-scroll
        if (_logAutoScroll) {
            var terminal = document.getElementById('log-terminal');
            if (terminal) terminal.scrollTop = terminal.scrollHeight;
        }
    }

    // Auto-scroll pause/resume on user scroll
    (function () {
        var terminal = document.getElementById('log-terminal');
        if (!terminal) return;
        terminal.addEventListener('scroll', function () {
            var atBottom = terminal.scrollHeight - terminal.scrollTop - terminal.clientHeight < 20;
            _logAutoScroll = atBottom;
            var indicator = document.getElementById('log-autoscroll-indicator');
            if (indicator) {
                if (_logAutoScroll) {
                    indicator.textContent = 'Auto-scroll';
                    indicator.classList.remove('is-paused');
                } else {
                    indicator.textContent = 'Auto-scroll (paused)';
                    indicator.classList.add('is-paused');
                }
            }
        });
    })();

    // Save Logs button
    var logSaveBtn = document.getElementById('log-save-btn');
    if (logSaveBtn) {
        logSaveBtn.addEventListener('click', function () {
            if (_logLines.length === 0) return;
            var content = _logLines.join('\n');
            var blob = new Blob([content], { type: 'text/plain' });
            var url = URL.createObjectURL(blob);
            var a = document.createElement('a');
            var ts = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
            a.href = url;
            a.download = 'nodus-edge-logs-' + ts + '.txt';
            a.click();
            URL.revokeObjectURL(url);
        });
    }

    // Push to NodusNet button
    var logPushBtn = document.getElementById('log-push-btn');
    if (logPushBtn) {
        logPushBtn.addEventListener('click', async function () {
            logPushBtn.disabled = true;
            logPushBtn.textContent = 'Pushing...';
            var statusEl = document.getElementById('log-push-status');
            if (statusEl) { statusEl.textContent = ''; statusEl.className = 'log-push-status'; }

            try {
                var resp = await authFetch('/api/debug/push-logs', { method: 'POST' });
                var data = await resp.json();
                if (resp.ok) {
                    if (statusEl) { statusEl.textContent = 'Uploaded'; statusEl.className = 'log-push-status success'; }
                } else {
                    if (statusEl) { statusEl.textContent = data.error || 'Error'; statusEl.className = 'log-push-status error'; }
                }
            } catch (err) {
                if (statusEl) { statusEl.textContent = err.message; statusEl.className = 'log-push-status error'; }
            } finally {
                logPushBtn.textContent = 'Push to NodusNet';
                logPushBtn.disabled = false;
                setTimeout(function () {
                    if (statusEl) statusEl.textContent = '';
                }, 5000);
            }
        });
    }

    // Update push button state based on has_rem from debug data
    function updateLogPushButton(hasRem) {
        var btn = document.getElementById('log-push-btn');
        if (!btn) return;
        if (!hasRem) {
            btn.disabled = true;
            btn.title = 'REM not configured. Connect to NodusNet first.';
        } else {
            btn.disabled = false;
            btn.title = 'Push diagnostic dump to NodusNet';
        }
    }

    // ========== Status Tab ==========

    async function refreshStatus() {
        try {
            const resp = await fetch('/api/status');
            const data = await resp.json();
            renderStatus(data);
        } catch (err) {
            console.error('Status fetch error:', err);
            document.getElementById('status-content').innerHTML = `
                <div class="empty-state"><h3>Failed to load status</h3></div>
            `;
        }
    }

    function renderStatus(data) {
        const container = document.getElementById('status-content');
        const pipeline = data.pipeline || {};
        const cache = data.cache || {};
        const nodeId = data.node_id || 'unknown';

        // Update header
        document.getElementById('node-id').textContent = nodeId;
        if (data.version) {
            document.getElementById('node-version').textContent = 'v' + data.version;
        }

        // Seed uptime clock
        if (pipeline.uptime_seconds !== undefined && pipeline.uptime_seconds !== null) {
            _uptimeBase = pipeline.uptime_seconds;
            _uptimeStamp = Date.now();
        }

        // Connection status
        const synapse = pipeline.synapse || {};
        _synapseConfigured = !!synapse.endpoint;
        let connClass, connTitle;
        if (synapse.paused) {
            connClass = 'paused';
            connTitle = 'Paused — click to resume';
        } else if (synapse.enabled) {
            connClass = 'connected';
            connTitle = 'Connected to NodusNet — click to pause';
        } else {
            connClass = 'standalone';
            connTitle = 'Standalone — no NodusNet endpoint configured';
        }
        const connBtn = document.getElementById('connection-btn');
        connBtn.className = 'header-btn ' + connClass;
        if (_synapseConfigured) connBtn.classList.add('clickable');
        connBtn.title = connTitle;
        // Toggle icon visibility
        document.getElementById('conn-icon-on').style.display = (connClass === 'standalone') ? 'none' : '';
        document.getElementById('conn-icon-off').style.display = (connClass === 'standalone') ? '' : 'none';

        let html = '';

        // Connectivity
        html += `<div class="status-section">
            <h3>Connectivity</h3>
            <div class="status-row">
                <span class="label">Mode</span>
                <span class="value">${synapse.enabled ? 'Connected to NodusNet' : 'Standalone'}</span>
            </div>
            ${synapse.endpoint ? `<div class="status-row"><span class="label">Endpoint</span><span class="value">${escapeHtml(synapse.endpoint)}</span></div>` : ''}
            ${synapse.enabled ? `<div class="status-row"><span class="label">Published</span><span class="value">${pipeline.synapse_published_count || 0}</span></div>` : ''}
        </div>`;

        // Pipeline
        html += `<div class="status-section">
            <h3>Pipeline</h3>
            <div class="status-row"><span class="label">Processed</span><span class="value">${pipeline.processed_count || 0}</span></div>
            <div class="status-row"><span class="label">Transcribed</span><span class="value">${pipeline.transcribed_count || 0}</span></div>
            <div class="status-row"><span class="label">Filtered</span><span class="value">${pipeline.filtered_count || 0}</span></div>
            <div class="status-row"><span class="label">Errors</span><span class="value ${(pipeline.error_count || 0) > 0 ? 'warn' : ''}">${pipeline.error_count || 0}</span></div>
            <div class="status-row"><span class="label">Whisper</span><span class="value ${pipeline.whisper_available ? 'good' : 'error'}">${pipeline.whisper_available ? 'Available' : 'Unavailable'}</span></div>
        </div>`;

        // Scanner
        const scanner = pipeline.scanner || {};
        if (Object.keys(scanner).length > 0) {
            html += `<div class="status-section">
                <h3>Scanner</h3>
                ${scanner.backend ? `<div class="status-row"><span class="label">Backend</span><span class="value">${escapeHtml(scanner.backend)}</span></div>` : ''}
                ${scanner.channels !== undefined ? `<div class="status-row"><span class="label">Channels</span><span class="value">${scanner.channels}</span></div>` : ''}
                ${scanner.center_freq_mhz ? `<div class="status-row"><span class="label">Center Freq</span><span class="value">${scanner.center_freq_mhz} MHz</span></div>` : ''}
                ${scanner.uptime_seconds !== undefined ? `<div class="status-row"><span class="label">Uptime</span><span class="value">${formatUptime(scanner.uptime_seconds)}</span></div>` : ''}
                ${scanner.watchdog_restarts !== undefined ? `<div class="status-row"><span class="label">Watchdog Restarts</span><span class="value ${scanner.watchdog_restarts > 0 ? 'warn' : ''}">${scanner.watchdog_restarts}</span></div>` : ''}
            </div>`;
        }

        // Whisper model info
        html += `<div class="status-section">
            <h3>Whisper Model</h3>
            <div class="status-row"><span class="label">Transcription</span><span class="value ${pipeline.transcription_enabled ? 'good' : 'warn'}">${pipeline.transcription_enabled ? 'Enabled' : 'Disabled'}</span></div>
            <p class="whisper-note">To change model: edit WHISPER_MODEL in .env and restart</p>
        </div>`;

        // Cache
        html += `<div class="status-section">
            <h3>Cache</h3>
            <div class="status-row"><span class="label">Repeater DB</span><span class="value">${cache.synced ? 'synced ' + formatCacheAge(cache.repeaters_age_hours) : 'bundled default'}</span></div>
            <div class="status-row"><span class="label">Repeaters</span><span class="value">${cache.repeaters_count || 0}</span></div>
            <div class="status-row"><span class="label">Net Schedules</span><span class="value">${cache.nets_count || 0}</span></div>
            ${cache.can_sync ? `<div style="margin-top:12px"><button class="btn btn-primary" id="sync-btn" onclick="doSync()">Sync Now</button></div>` : ''}
        </div>`;

        // Node
        html += `<div class="status-section">
            <h3>Node</h3>
            <div class="status-row"><span class="label">Node ID</span><span class="value">${escapeHtml(nodeId)}</span></div>
            ${pipeline.uptime_seconds !== undefined ? `<div class="status-row"><span class="label">Uptime</span><span class="value">${formatUptime(pipeline.uptime_seconds)}</span></div>` : ''}
        </div>`;

        container.innerHTML = html;
    }

    function formatUptime(seconds) {
        if (!seconds && seconds !== 0) return '--';
        const h = Math.floor(seconds / 3600);
        const m = Math.floor((seconds % 3600) / 60);
        if (h > 0) return h + 'h ' + m + 'm';
        return m + 'm';
    }

    function formatCacheAge(hours) {
        if (!hours && hours !== 0) return '';
        if (hours < 1) return '(< 1 hour ago)';
        if (hours < 24) return '(' + Math.round(hours) + ' hours ago)';
        return '(' + Math.round(hours / 24) + ' days ago)';
    }

    // ========== Spectrum + Waterfall ==========

    const spectrum = {
        channels: [],          // [{frequency_hz, frequency_mhz, repeater_callsign, repeater_city}]
        barLevels: {},         // freq_hz -> {db, timestamp}
        waterfallRows: [],     // newest first: [{freq_hz -> db}]
        currentBucket: {},     // freq_hz -> [db values] accumulating for current 30s window
        bucketStart: 0,        // timestamp when current bucket started
        squelchDb: -30,        // squelch threshold in dB (set from API)
        recommendedDb: null,   // recommended squelch in raw dB (from API)
        barCanvas: null,
        wfCanvas: null,
        barCtx: null,
        wfCtx: null,
        rafId: null,
        initialized: false,

        // Constants
        BAR_HEIGHT: 180,
        WF_HEIGHT: 340,
        BUCKET_SEC: 30,
        MAX_ROWS: 60,
        DB_FLOOR: -80,         // absolute floor (never display below this)
        DB_CEIL: 0,            // absolute ceiling (never display above this)
        DB_MIN: -50,           // dynamic range min (auto-scales down)
        DB_MAX: -5,            // dynamic range max (auto-scales up)
        DB_DEFAULT_MIN: -50,   // initial range min
        DB_DEFAULT_MAX: -5,    // initial range max
        DB_MARGIN: 5,          // padding beyond observed extremes
        DECAY_SEC: 30,
    };

    function autoScaleDbRange() {
        // Collect all observed dB values from bar levels and waterfall
        let minDb = Infinity;
        let maxDb = -Infinity;
        for (const freqHz in spectrum.barLevels) {
            const lvl = spectrum.barLevels[freqHz];
            if (lvl.db > spectrum.DB_FLOOR && lvl.timestamp > 0) {
                if (lvl.db < minDb) minDb = lvl.db;
                if (lvl.db > maxDb) maxDb = lvl.db;
            }
        }
        spectrum.waterfallRows.forEach(row => {
            for (const key in row) {
                if (key === '_ts') continue;
                const db = row[key];
                if (db < minDb) minDb = db;
                if (db > maxDb) maxDb = db;
            }
        });

        if (minDb === Infinity) {
            // No data — keep defaults
            spectrum.DB_MIN = spectrum.DB_DEFAULT_MIN;
            spectrum.DB_MAX = spectrum.DB_DEFAULT_MAX;
            return;
        }

        // Expand range with margin, clamped to absolute bounds
        spectrum.DB_MIN = Math.max(spectrum.DB_FLOOR, Math.min(spectrum.DB_DEFAULT_MIN, minDb - spectrum.DB_MARGIN));
        spectrum.DB_MAX = Math.min(spectrum.DB_CEIL, Math.max(spectrum.DB_DEFAULT_MAX, maxDb + spectrum.DB_MARGIN));
    }

    function updateSquelchHint() {
        const hint = document.getElementById('squelch-hint');
        if (!hint) return;
        if (spectrum.recommendedDb === null) {
            hint.textContent = 'Collecting data for squelch recommendation\u2026';
            hint.style.display = '';
        } else {
            hint.textContent = 'Recommended: ' + spectrum.recommendedDb.toFixed(1) + ' dB';
            hint.style.display = '';
        }
    }

    async function initSpectrum() {
        if (spectrum.initialized) {
            startSpectrumLoop();
            return;
        }

        try {
            const resp = await fetch('/api/spectrum');
            const data = await resp.json();
            spectrum.channels = data.channels || [];
            // Convert squelch SNR to approximate dB threshold
            spectrum.squelchDb = data.squelch_db ?? -18;
            // Recommended squelch (raw dB, may be null)
            spectrum.recommendedDb = data.recommended_squelch_db || null;
            updateSquelchHint();

            // Init bar levels
            spectrum.channels.forEach(ch => {
                spectrum.barLevels[ch.frequency_hz] = { db: spectrum.DB_MIN, timestamp: 0 };
            });

            // Set up canvases
            spectrum.barCanvas = document.getElementById('spectrum-bars');
            spectrum.wfCanvas = document.getElementById('spectrum-waterfall');
            if (!spectrum.barCanvas || !spectrum.wfCanvas) return;

            resizeSpectrumCanvases();
            window.addEventListener('resize', resizeSpectrumCanvases);

            // Seed waterfall from historical signal data
            seedWaterfallFromHistory(data.channels);
            autoScaleDbRange();

            // Seed bar levels from most recent signal per channel
            spectrum.channels.forEach(ch => {
                if (ch.signals && ch.signals.length > 0) {
                    const latest = ch.signals[ch.signals.length - 1];
                    spectrum.barLevels[ch.frequency_hz] = { db: latest.db, timestamp: latest.timestamp };
                }
            });

            spectrum.bucketStart = Date.now() / 1000;
            spectrum.initialized = true;

            // Waterfall click handler — inspect cell segments
            spectrum.wfCanvas.addEventListener('click', onWaterfallClick);

            // Replay demo button
            const replayBtn = document.getElementById('replay-demo-btn');
            if (replayBtn) {
                replayBtn.addEventListener('click', replayDemo);
            }

            startSpectrumLoop();
        } catch (err) {
            console.error('Spectrum init error:', err);
        }
    }

    function seedWaterfallFromHistory(channels) {
        // Group all signals into 30s buckets across channels
        const allTimes = [];
        channels.forEach(ch => {
            (ch.signals || []).forEach(s => allTimes.push(s.timestamp));
        });
        if (allTimes.length === 0) return;

        const minT = Math.min(...allTimes);
        const maxT = Math.max(...allTimes);
        const bucketSec = spectrum.BUCKET_SEC;
        const bucketStart = Math.floor(minT / bucketSec) * bucketSec;
        const buckets = [];

        for (let t = bucketStart; t <= maxT; t += bucketSec) {
            const row = {};
            let hasData = false;
            channels.forEach(ch => {
                const sigs = (ch.signals || []).filter(s => s.timestamp >= t && s.timestamp < t + bucketSec);
                if (sigs.length > 0) {
                    row[ch.frequency_hz] = sigs.reduce((sum, s) => sum + s.db, 0) / sigs.length;
                    hasData = true;
                }
            });
            if (hasData) {
                buckets.push(row);
            }
        }

        // Newest first
        spectrum.waterfallRows = buckets.reverse().slice(0, spectrum.MAX_ROWS);
    }

    function startSpectrumLoop() {
        if (spectrum.rafId) return;
        function loop() {
            const activeTab = document.querySelector('.tab-btn.active');
            if (!activeTab || activeTab.dataset.tab !== 'spectrum') {
                spectrum.rafId = null;
                return;
            }
            const now = Date.now() / 1000;
            tickWaterfallBucket(now);
            drawBarPanel(now);
            drawWaterfallPanel();
            spectrum.rafId = requestAnimationFrame(loop);
        }
        spectrum.rafId = requestAnimationFrame(loop);
    }

    function drawBarPanel(now) {
        const ctx = spectrum.barCtx;
        const canvas = spectrum.barCanvas;
        if (!ctx || !canvas) return;
        const W = canvas.width;
        const H = canvas.height;
        const dpr = window.devicePixelRatio || 1;
        const channels = spectrum.channels;
        if (channels.length === 0) return;

        ctx.clearRect(0, 0, W, H);

        const labelArea = 50 * dpr;
        const bottomPad = 55 * dpr;
        const topPad = 10 * dpr;
        const drawH = H - bottomPad - topPad;
        const drawW = W - labelArea;
        const barW = Math.max(4, (drawW / channels.length) - 2 * dpr);
        const gap = (drawW - barW * channels.length) / (channels.length + 1);

        // Draw squelch line (red)
        const sqNorm = (spectrum.squelchDb - spectrum.DB_MIN) / (spectrum.DB_MAX - spectrum.DB_MIN);
        const sqY = topPad + drawH * (1 - Math.max(0, Math.min(1, sqNorm)));
        ctx.setLineDash([6 * dpr, 4 * dpr]);
        ctx.strokeStyle = '#f85149';
        ctx.lineWidth = 1.5 * dpr;
        ctx.beginPath();
        ctx.moveTo(labelArea, sqY);
        ctx.lineTo(W, sqY);
        ctx.stroke();
        ctx.setLineDash([]);

        // Draw recommended squelch line (gold, dashed)
        if (spectrum.recommendedDb !== null) {
            const recNorm = (spectrum.recommendedDb - spectrum.DB_MIN) / (spectrum.DB_MAX - spectrum.DB_MIN);
            const recY = topPad + drawH * (1 - Math.max(0, Math.min(1, recNorm)));
            ctx.setLineDash([4 * dpr, 6 * dpr]);
            ctx.strokeStyle = '#d29922';
            ctx.lineWidth = 1.5 * dpr;
            ctx.beginPath();
            ctx.moveTo(labelArea, recY);
            ctx.lineTo(W, recY);
            ctx.stroke();
            ctx.setLineDash([]);
        }

        // dB axis labels
        ctx.fillStyle = '#6e7681';
        ctx.font = (10 * dpr) + 'px monospace';
        ctx.textAlign = 'right';
        const steps = [spectrum.DB_MIN, spectrum.DB_MIN + (spectrum.DB_MAX - spectrum.DB_MIN) * 0.25,
                        spectrum.DB_MIN + (spectrum.DB_MAX - spectrum.DB_MIN) * 0.5,
                        spectrum.DB_MIN + (spectrum.DB_MAX - spectrum.DB_MIN) * 0.75, spectrum.DB_MAX];
        steps.forEach(db => {
            const norm = (db - spectrum.DB_MIN) / (spectrum.DB_MAX - spectrum.DB_MIN);
            const y = topPad + drawH * (1 - norm);
            ctx.fillText(Math.round(db) + '', labelArea - 6 * dpr, y + 3 * dpr);
        });

        // Bars
        channels.forEach((ch, i) => {
            const level = spectrum.barLevels[ch.frequency_hz];
            if (!level) return;
            const age = now - level.timestamp;
            const alpha = Math.max(0, 1 - age / spectrum.DECAY_SEC);
            if (alpha <= 0) return;

            const norm = Math.max(0, Math.min(1, (level.db - spectrum.DB_MIN) / (spectrum.DB_MAX - spectrum.DB_MIN)));
            const barH = norm * drawH;
            const x = labelArea + gap + i * (barW + gap);
            const y = topPad + drawH - barH;

            ctx.globalAlpha = alpha;
            ctx.fillStyle = '#58a6ff';
            ctx.fillRect(x, y, barW, barH);
            ctx.globalAlpha = 1;
        });

        // Frequency labels (rotated)
        ctx.fillStyle = '#8b949e';
        ctx.font = (9 * dpr) + 'px monospace';
        ctx.textAlign = 'right';
        channels.forEach((ch, i) => {
            const x = labelArea + gap + i * (barW + gap) + barW / 2;
            const y = topPad + drawH + 6 * dpr;
            ctx.save();
            ctx.translate(x, y);
            ctx.rotate(-Math.PI / 3);
            ctx.fillText(ch.frequency_mhz.toFixed(3), 0, 0);
            ctx.restore();
        });

        // Repeater callsigns below
        ctx.fillStyle = '#6e7681';
        ctx.font = (8 * dpr) + 'px sans-serif';
        ctx.textAlign = 'center';
        channels.forEach((ch, i) => {
            if (!ch.repeater_callsign) return;
            const x = labelArea + gap + i * (barW + gap) + barW / 2;
            const y = H - 4 * dpr;
            ctx.fillText(ch.repeater_callsign, x, y);
        });
    }

    function drawWaterfallPanel() {
        const ctx = spectrum.wfCtx;
        const canvas = spectrum.wfCanvas;
        if (!ctx || !canvas) return;
        const W = canvas.width;
        const H = canvas.height;
        const dpr = window.devicePixelRatio || 1;
        const channels = spectrum.channels;
        const rows = spectrum.waterfallRows;
        if (channels.length === 0) return;

        ctx.clearRect(0, 0, W, H);

        const labelArea = 50 * dpr;
        const bottomPad = 45 * dpr;
        const drawH = H - bottomPad;
        const drawW = W - labelArea;
        const colW = drawW / channels.length;
        const rowH = rows.length > 0 ? Math.min(drawH / rows.length, 12 * dpr) : 8 * dpr;
        const maxVisible = Math.floor(drawH / rowH);

        // Time labels
        ctx.fillStyle = '#6e7681';
        ctx.font = (9 * dpr) + 'px monospace';
        ctx.textAlign = 'right';

        const visibleRows = rows.slice(0, maxVisible);
        visibleRows.forEach((row, ri) => {
            const y = ri * rowH;
            channels.forEach((ch, ci) => {
                const db = row[ch.frequency_hz];
                if (db !== undefined) {
                    const color = dbToHeat(db);
                    ctx.fillStyle = color;
                } else {
                    ctx.fillStyle = '#0d1117';
                }
                ctx.fillRect(labelArea + ci * colW, y, colW, rowH);
            });

            // Time label every 5 rows
            if (ri % 5 === 0) {
                const minAgo = Math.round(ri * spectrum.BUCKET_SEC / 60);
                ctx.fillStyle = '#6e7681';
                ctx.fillText(minAgo + 'm', labelArea - 6 * dpr, y + rowH - 2 * dpr);
            }
        });

        // Column separators
        ctx.strokeStyle = 'rgba(48,54,61,0.5)';
        ctx.lineWidth = 1;
        for (let ci = 1; ci < channels.length; ci++) {
            const x = labelArea + ci * colW;
            ctx.beginPath();
            ctx.moveTo(x, 0);
            ctx.lineTo(x, visibleRows.length * rowH);
            ctx.stroke();
        }

        // Frequency labels along bottom (rotated like bar chart)
        const freqLabelY = visibleRows.length * rowH + 6 * dpr;
        ctx.fillStyle = '#8b949e';
        ctx.font = (9 * dpr) + 'px monospace';
        ctx.textAlign = 'right';
        channels.forEach((ch, ci) => {
            const x = labelArea + ci * colW + colW / 2;
            ctx.save();
            ctx.translate(x, freqLabelY);
            ctx.rotate(-Math.PI / 3);
            ctx.fillText(ch.frequency_mhz.toFixed(3), 0, 0);
            ctx.restore();
        });

    }

    function onWaterfallClick(evt) {
        const canvas = spectrum.wfCanvas;
        if (!canvas || spectrum.channels.length === 0) return;

        const dpr = window.devicePixelRatio || 1;
        const rect = canvas.getBoundingClientRect();
        const clickX = (evt.clientX - rect.left) * dpr;
        const clickY = (evt.clientY - rect.top) * dpr;

        // Replicate layout constants from drawWaterfallPanel
        const labelArea = 50 * dpr;
        const bottomPad = 45 * dpr;
        const W = canvas.width;
        const H = canvas.height;
        const drawH = H - bottomPad;
        const drawW = W - labelArea;
        const channels = spectrum.channels;
        const rows = spectrum.waterfallRows;
        const colW = drawW / channels.length;
        const rowH = rows.length > 0 ? Math.min(drawH / rows.length, 12 * dpr) : 8 * dpr;
        const maxVisible = Math.floor(drawH / rowH);
        var visibleCount = Math.min(rows.length, maxVisible);

        // Check bounds — must be within the data grid
        if (clickX < labelArea || clickX >= W) return;
        if (clickY < 0 || clickY >= visibleCount * rowH) return;

        var ci = Math.floor((clickX - labelArea) / colW);
        var ri = Math.floor(clickY / rowH);
        if (ci < 0 || ci >= channels.length) return;
        if (ri < 0 || ri >= visibleCount) return;

        var freqHz = channels[ci].frequency_hz;
        var freqMhz = channels[ci].frequency_mhz;

        // Compute time range for this bucket
        // Row 0 is "now" (most recent bucket). Each row is BUCKET_SEC older.
        var now = Date.now() / 1000;
        var bucketEnd = now - ri * spectrum.BUCKET_SEC;
        var bucketStart = bucketEnd - spectrum.BUCKET_SEC;

        showWaterfallEvents(freqHz, freqMhz, bucketStart, bucketEnd);
    }

    async function showWaterfallEvents(freqHz, freqMhz, fromTs, toTs) {
        var panel = document.getElementById('waterfall-events-panel');
        var title = document.getElementById('waterfall-events-title');
        var tbody = document.getElementById('waterfall-events-body');
        var emptyEl = document.getElementById('waterfall-events-empty');
        var table = document.getElementById('waterfall-events-table');
        if (!panel || !tbody) return;

        // Format the time range for the title
        var fromDate = new Date(fromTs * 1000);
        var toDate = new Date(toTs * 1000);
        var timeFmt = function (d) {
            return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
        };
        title.textContent = freqMhz.toFixed(3) + ' MHz \u2014 ' + timeFmt(fromDate) + ' to ' + timeFmt(toDate);

        panel.style.display = '';
        tbody.innerHTML = '';
        emptyEl.style.display = 'none';
        table.style.display = '';

        try {
            var url = '/api/spectrum/events?freq_hz=' + freqHz +
                      '&from_ts=' + fromTs + '&to_ts=' + toTs;
            var resp = await fetch(url);
            var data = await resp.json();
            var segments = data.segments || [];

            if (segments.length === 0) {
                table.style.display = 'none';
                emptyEl.style.display = '';
                return;
            }

            segments.forEach(function (seg) {
                var tr = document.createElement('tr');

                // Time cell
                var tdTime = document.createElement('td');
                tdTime.className = 'wf-ev-time';
                var ts = seg.timestamp || '';
                if (ts) {
                    try {
                        var d = new Date(ts.replace ? ts.replace('Z', '+00:00') : ts);
                        tdTime.textContent = d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
                    } catch (e) {
                        tdTime.textContent = ts;
                    }
                }
                tr.appendChild(tdTime);

                // Callsigns cell
                var tdCall = document.createElement('td');
                tdCall.className = 'wf-ev-callsigns';
                var callsigns = seg.detected_callsigns || [];
                callsigns.forEach(function (cs) {
                    var span = document.createElement('span');
                    span.className = 'callsign-tag';
                    span.textContent = cs;
                    tdCall.appendChild(span);
                });
                if (callsigns.length === 0) {
                    tdCall.style.color = 'var(--text-muted)';
                    tdCall.textContent = '--';
                }
                tr.appendChild(tdCall);

                // Transcript cell
                var tdText = document.createElement('td');
                tdText.className = 'wf-ev-text';
                var text = (seg.transcription || {}).text || '';
                tdText.textContent = text || '(audio only)';
                if (!text) tdText.style.color = 'var(--text-muted)';
                tr.appendChild(tdText);

                // Signal dB cell
                var tdDb = document.createElement('td');
                tdDb.className = 'wf-ev-db';
                var sigDb = (seg.rf_channel || {}).signal_strength_db;
                tdDb.textContent = sigDb !== undefined && sigDb !== null ? sigDb.toFixed(1) : '--';
                tr.appendChild(tdDb);

                tbody.appendChild(tr);
            });
        } catch (err) {
            console.error('Waterfall events fetch error:', err);
            table.style.display = 'none';
            emptyEl.style.display = '';
            emptyEl.textContent = 'Error loading segments.';
        }
    }

    function dbToHeat(db) {
        const norm = Math.max(0, Math.min(1, (db - spectrum.DB_MIN) / (spectrum.DB_MAX - spectrum.DB_MIN)));
        // 4-stop gradient: dark blue -> green -> yellow -> red
        let r, g, b;
        if (norm < 0.33) {
            const t = norm / 0.33;
            r = Math.round(10 + t * 10);
            g = Math.round(20 + t * 180);
            b = Math.round(80 + t * (-30));
        } else if (norm < 0.66) {
            const t = (norm - 0.33) / 0.33;
            r = Math.round(20 + t * 210);
            g = Math.round(200 - t * 10);
            b = Math.round(50 - t * 30);
        } else {
            const t = (norm - 0.66) / 0.34;
            r = Math.round(230 + t * 18);
            g = Math.round(190 - t * 140);
            b = Math.round(20 - t * 10);
        }
        return 'rgb(' + r + ',' + g + ',' + b + ')';
    }

    function updateSpectrumFromSegment(seg) {
        if (!spectrum.initialized) return;
        const rfChannel = seg.rf_channel || {};
        const freqHz = rfChannel.frequency_hz;
        const db = rfChannel.signal_strength_db;
        if (!freqHz || db === undefined || db === null) return;

        const now = Date.now() / 1000;
        spectrum.barLevels[freqHz] = { db: db, timestamp: now };

        // Accumulate into current waterfall bucket
        if (!spectrum.currentBucket[freqHz]) {
            spectrum.currentBucket[freqHz] = [];
        }
        spectrum.currentBucket[freqHz].push(db);
    }

    function tickWaterfallBucket(now) {
        if (!spectrum.bucketStart) {
            spectrum.bucketStart = now;
            return;
        }
        if (now - spectrum.bucketStart < spectrum.BUCKET_SEC) return;

        // Flush current bucket to a waterfall row
        const row = {};
        let hasData = false;
        for (const freqHz in spectrum.currentBucket) {
            const vals = spectrum.currentBucket[freqHz];
            if (vals.length > 0) {
                row[parseInt(freqHz)] = vals.reduce((a, b) => a + b, 0) / vals.length;
                hasData = true;
            }
        }
        if (hasData) {
            spectrum.waterfallRows.unshift(row);
            if (spectrum.waterfallRows.length > spectrum.MAX_ROWS) {
                spectrum.waterfallRows.length = spectrum.MAX_ROWS;
            }
        }
        spectrum.currentBucket = {};
        spectrum.bucketStart = now;
        autoScaleDbRange();
    }

    function resizeSpectrumCanvases() {
        const dpr = window.devicePixelRatio || 1;
        [
            { canvas: spectrum.barCanvas, height: spectrum.BAR_HEIGHT, ctxKey: 'barCtx' },
            { canvas: spectrum.wfCanvas, height: spectrum.WF_HEIGHT, ctxKey: 'wfCtx' },
        ].forEach(({ canvas, height, ctxKey }) => {
            if (!canvas) return;
            const rect = canvas.getBoundingClientRect();
            canvas.width = rect.width * dpr;
            canvas.height = height * dpr;
            canvas.style.height = height + 'px';
            spectrum[ctxKey] = canvas.getContext('2d');
        });
    }

    function replayDemo() {
        if (spectrum.channels.length === 0) {
            // Generate demo channels if none exist
            const demoFreqs = [145.230, 146.520, 146.940, 147.060, 147.360, 148.150, 449.500, 442.100];
            spectrum.channels = demoFreqs.map(mhz => ({
                frequency_hz: Math.round(mhz * 1_000_000),
                frequency_mhz: mhz,
                repeater_callsign: '',
                repeater_city: '',
            }));
            spectrum.channels.forEach(ch => {
                spectrum.barLevels[ch.frequency_hz] = { db: spectrum.DB_MIN, timestamp: 0 };
            });
            resizeSpectrumCanvases();
        }

        // Generate 60 rows of synthetic waterfall data
        spectrum.waterfallRows = [];
        const now = Date.now() / 1000;
        for (let r = 0; r < 60; r++) {
            const row = {};
            spectrum.channels.forEach(ch => {
                if (Math.random() > 0.4) {
                    row[ch.frequency_hz] = spectrum.DB_MIN + Math.random() * (spectrum.DB_MAX - spectrum.DB_MIN);
                }
            });
            spectrum.waterfallRows.push(row);
        }

        // Set bar levels to random current values
        spectrum.channels.forEach(ch => {
            spectrum.barLevels[ch.frequency_hz] = {
                db: spectrum.DB_MIN + Math.random() * (spectrum.DB_MAX - spectrum.DB_MIN),
                timestamp: now,
            };
        });

        spectrum.bucketStart = now;
        startSpectrumLoop();
    }

    // Periodically refresh recommended squelch on spectrum tab
    setInterval(async function () {
        if (!spectrum.initialized) return;
        const activeTab = document.querySelector('.tab-btn.active');
        if (!activeTab || activeTab.dataset.tab !== 'spectrum') return;
        try {
            const resp = await fetch('/api/spectrum');
            const data = await resp.json();
            spectrum.squelchDb = data.squelch_db ?? -18;
            spectrum.recommendedDb = data.recommended_squelch_db || null;
            updateSquelchHint();
        } catch (err) {
            // Non-critical
        }
    }, 60000);

    // ========== Sync ==========

    window.doSync = async function () {
        const btn = document.getElementById('sync-btn');
        if (btn) {
            btn.disabled = true;
            btn.textContent = 'Syncing...';
        }
        try {
            const resp = await fetch('/api/sync', { method: 'POST' });
            const data = await resp.json();
            if (data.error) {
                alert('Sync failed: ' + data.error);
            } else {
                refreshStatus();
                refreshFrequencies();
            }
        } catch (err) {
            alert('Sync error: ' + err.message);
        } finally {
            if (btn) {
                btn.disabled = false;
                btn.textContent = 'Sync Now';
            }
        }
    };

    // ========== Utilities ==========

    function escapeHtml(str) {
        if (!str) return '';
        return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
    }

    // ========== Synapse Toggle ==========

    async function toggleSynapse(action) {
        try {
            const resp = await authFetch('/api/synapse/toggle', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ action: action }),
            });
            if (resp.ok) refreshStatus();
        } catch (err) {
            console.error('Synapse toggle error:', err);
        }
    }

    document.getElementById('connection-btn').addEventListener('click', function () {
        if (!_synapseConfigured) return;
        const btn = this;
        if (btn.classList.contains('paused')) {
            toggleSynapse('unpause');
        } else if (btn.classList.contains('connected')) {
            if (confirm('Disconnect from NodusNet? Segments will queue locally.')) {
                toggleSynapse('pause');
            }
        }
    });

    // ========== Uptime Clock ==========

    function formatUptimeLong(totalSec) {
        const d = Math.floor(totalSec / 86400);
        const h = Math.floor((totalSec % 86400) / 3600);
        const m = Math.floor((totalSec % 3600) / 60);
        const s = Math.floor(totalSec % 60);
        const pad = n => String(n).padStart(2, '0');
        return d + 'd ' + pad(h) + ':' + pad(m) + ':' + pad(s);
    }

    setInterval(function () {
        if (_uptimeBase === null || _uptimeStamp === null) return;
        const elapsed = (Date.now() - _uptimeStamp) / 1000;
        const total = _uptimeBase + elapsed;
        const el = document.getElementById('uptime-clock');
        if (el) el.innerHTML = '<b>Uptime</b> \u2014 ' + formatUptimeLong(total);
    }, 1000);

    // ========== NTP Clock ==========

    function formatNtpTime(tz) {
        const now = new Date();
        const utc = now.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit', timeZone: 'UTC' });
        let local = '';
        if (tz) {
            try {
                local = now.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit', timeZone: tz });
            } catch (e) {
                local = '';
            }
        }
        if (local && local !== utc) {
            const short = tz.split('/').pop().replace(/_/g, ' ');
            return utc + ' UTC / ' + local + ' ' + short;
        }
        return utc + ' UTC';
    }

    setInterval(function () {
        const el = document.getElementById('ntp-clock');
        if (el) el.innerHTML = '<b>NTP</b> \u2014 ' + formatNtpTime(_localTz);
    }, 1000);

    // ========== Config Fetch ==========

    async function fetchConfig() {
        try {
            const resp = await fetch('/api/config');
            if (resp.ok) {
                const data = await resp.json();
                if (data.timezone) _localTz = data.timezone;
                if (data.metro) {
                    const badge = document.getElementById('metro-badge');
                    const label = document.getElementById('metro-label');
                    if (badge && label) {
                        label.textContent = data.metro.charAt(0).toUpperCase() + data.metro.slice(1);
                        badge.style.display = '';
                    }
                }
            }
        } catch (err) {
            // Config endpoint optional
        }
    }

    // ========== Version Check ==========

    let _versionDismissed = false;
    let _composeDismissed = false;

    async function checkVersion() {
        if (_versionDismissed) return;
        try {
            const resp = await fetch('/api/version-check');
            if (!resp.ok) return;
            const data = await resp.json();
            const banner = document.getElementById('version-banner');
            const text = document.getElementById('version-banner-text');
            if (!banner || !text) return;
            if (data.update_available && data.latest_version) {
                text.textContent = 'Update available: ' + data.current_version + ' \u2192 ' + data.latest_version;
                banner.style.display = 'flex';
            } else {
                banner.style.display = 'none';
            }

            // Compose file update banner
            const composeBanner = document.getElementById('compose-banner');
            const composeText = document.getElementById('compose-banner-text');
            if (composeBanner && composeText && !_composeDismissed && data.compose_update_available && data.compose_version) {
                composeText.innerHTML =
                    '<strong>Configuration update available (v' + data.compose_version.current +
                    ' \u2192 v' + data.compose_version.latest + ').</strong> Run these commands to update:' +
                    '<pre style="margin:0.4em 0 0;padding:0.4em 0.6em;background:rgba(0,0,0,0.2);border-radius:4px;font-size:0.85em;overflow-x:auto">' +
                    'curl -sfo docker-compose.yml https://net.nodusrf.com/v1/edge/compose\n' +
                    'docker compose up -d</pre>';
                composeBanner.style.display = 'flex';
            } else if (composeBanner) {
                composeBanner.style.display = 'none';
            }
        } catch (err) {
            // Non-critical
        }
    }

    // "Update Now" triggers container restart (pulls latest on restart)
    const versionUpdateBtn = document.getElementById('version-update-btn');
    if (versionUpdateBtn) {
        versionUpdateBtn.addEventListener('click', async function () {
            if (!confirm('Restart the container to apply the update? Scanning will briefly pause.')) return;
            try {
                await authFetch('/api/restart', { method: 'POST' });
                this.textContent = 'Restarting...';
                this.disabled = true;
            } catch (err) {
                alert('Restart error: ' + err.message);
            }
        });
    }

    // Dismiss hides for the session
    const versionDismissBtn = document.getElementById('version-dismiss-btn');
    if (versionDismissBtn) {
        versionDismissBtn.addEventListener('click', function () {
            _versionDismissed = true;
            document.getElementById('version-banner').style.display = 'none';
        });
    }

    // Dismiss compose banner for the session
    const composeDismissBtn = document.getElementById('compose-dismiss-btn');
    if (composeDismissBtn) {
        composeDismissBtn.addEventListener('click', function () {
            _composeDismissed = true;
            document.getElementById('compose-banner').style.display = 'none';
        });
    }

    // Poll every 5 minutes
    setInterval(checkVersion, 5 * 60 * 1000);

    // ========== Validation Warnings ==========

    const WARNING_ICON_SVG = '<svg class="warning-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z"/><line x1="12" x2="12" y1="9" y2="13"/><line x1="12" x2="12.01" y1="17" y2="17"/></svg>';

    async function refreshWarnings() {
        try {
            const resp = await fetch('/api/warnings');
            if (!resp.ok) return;
            const data = await resp.json();
            renderWarnings(data);
        } catch (e) {
            // silent
        }
    }

    function renderWarnings(data) {
        const banner = document.getElementById('warnings-banner');
        const list = document.getElementById('warnings-list');
        if (!banner || !list) return;

        const startup = data.startup || [];
        const segIssues = data.segment_issues || {};
        const alerts = data.alerts || [];
        const hasContent = startup.length > 0 || Object.keys(segIssues).length > 0 || alerts.length > 0;

        if (!hasContent) {
            banner.style.display = 'none';
            return;
        }

        let html = '';

        // Startup warnings
        startup.forEach(function (w) {
            const cls = 'severity-' + (w.severity || 'warning');
            let fixBtn = '';
            if (w.fix) {
                if (w.fix.method === 'NAV') {
                    fixBtn = '<button class="warning-fix-btn" onclick="document.querySelector(\'[data-tab=settings]\').click()">' + escHtml(w.fix.label) + '</button>';
                } else {
                    fixBtn = '<button class="warning-fix-btn" data-endpoint="' + escHtml(w.fix.endpoint) + '" data-method="' + escHtml(w.fix.method) + '"'
                        + (w.fix.payload ? ' data-payload=\'' + JSON.stringify(w.fix.payload).replace(/'/g, '&#39;') + '\'' : '')
                        + (w.fix.restart ? ' data-restart="1"' : '')
                        + ' onclick="executeWarningFix(this)">' + escHtml(w.fix.label) + '</button>';
                }
            }
            html += '<div class="warning-row ' + cls + '">'
                + WARNING_ICON_SVG
                + '<span class="warning-message">' + escHtml(w.message) + '</span>'
                + fixBtn
                + '</div>';
        });

        // Segment issue summaries
        var issueLabels = {
            missing_repeater_callsign: { msg: 'segments missing repeater callsign', fixLabel: 'Sync Repeaters', fixEndpoint: '/api/sync' },
            missing_metro: { msg: 'segments missing metro area' },
            missing_transcription: { msg: 'segments missing transcription' },
            missing_node_id: { msg: 'segments missing node ID' },
        };
        Object.keys(segIssues).forEach(function (code) {
            var info = segIssues[code];
            var label = issueLabels[code] || { msg: 'segments with ' + code };
            var fixBtn = '';
            if (label.fixEndpoint) {
                fixBtn = '<button class="warning-fix-btn" data-endpoint="' + label.fixEndpoint + '" data-method="POST" onclick="executeWarningFix(this)">' + escHtml(label.fixLabel) + '</button>';
            }
            html += '<div class="warning-segment-summary">'
                + WARNING_ICON_SVG
                + '<span><span class="warning-segment-count">' + info.count + '</span> ' + label.msg + ' in the last hour</span>'
                + fixBtn
                + '</div>';
        });

        // System alerts (zero captures, support sidecar prompt)
        alerts.forEach(function (a) {
            var cls = 'severity-' + (a.severity || 'warning');
            var icon = a.severity === 'error'
                ? '<svg class="warning-icon" viewBox="0 0 20 20" fill="currentColor"><circle cx="10" cy="10" r="9" fill="none" stroke="currentColor" stroke-width="1.5"/><path d="M10 5v6M10 13.5v1"/></svg>'
                : WARNING_ICON_SVG;
            html += '<div class="warning-row ' + cls + '">'
                + icon
                + '<span class="warning-message">' + escHtml(a.message) + '</span>'
                + '</div>';
        });

        list.innerHTML = html;
        banner.style.display = 'block';
    }

    function escHtml(s) {
        if (!s) return '';
        return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    }

    // Global so inline onclick can call it
    window.executeWarningFix = async function (btn) {
        var endpoint = btn.dataset.endpoint;
        var method = btn.dataset.method || 'POST';
        var payload = btn.dataset.payload ? JSON.parse(btn.dataset.payload) : null;
        var needsRestart = btn.dataset.restart === '1';

        btn.disabled = true;
        btn.textContent = 'Working...';

        try {
            var opts = { method: method };
            if (payload) {
                opts.headers = { 'Content-Type': 'application/json' };
                opts.body = JSON.stringify(payload);
            }
            var resp = await fetch(endpoint, opts);
            if (resp.ok) {
                btn.textContent = 'Done';
                btn.classList.add('success');
                if (needsRestart) {
                    var restartBanner = document.getElementById('restart-banner');
                    if (restartBanner) restartBanner.style.display = 'flex';
                }
                // Re-fetch warnings after a short delay to update banner
                setTimeout(refreshWarnings, 1500);
            } else {
                var errData = await resp.json().catch(function () { return {}; });
                btn.textContent = errData.error || 'Error';
                btn.disabled = false;
            }
        } catch (e) {
            btn.textContent = 'Error';
            btn.disabled = false;
        }
    };

    // ========== Support Tab ==========

    const TAG_KEYWORDS = {
        'no-audio':       ['no audio', 'not hearing', 'silent', 'no signal', 'no sound', 'stopped working', 'nothing coming'],
        'no-transcript':  ['no transcript', 'not transcribing', 'whisper', 'no text', 'transcription'],
        'crashed':        ['crashed', 'down', 'not running', "won't start", 'dead', 'stopped', 'exited', 'error'],
        'usb':            ['usb', 'dongle', 'rtl-sdr', 'device', 'sdr', 'unplug', 'replug'],
        'disk-full':      ['disk', 'space', 'storage', 'full', 'no room'],
        'network':        ['network', 'synapse', "can't connect", 'offline', 'upload', 'sync', 'timeout', 'connection'],
        'slow':           ['slow', 'lag', 'delayed', 'behind', 'backed up', 'taking forever'],
        'update':         ['update', 'upgrade', 'version', 'outdated', 'pull', 'old version'],
    };

    let _supportSession = null;
    let _supportPollInterval = null;
    let _supportTtlInterval = null;
    let _supportProgressInterval = null;
    let _supportProgressSeen = 0;  // how many events we've already shown
    let _supportSessionStart = null;
    const SUPPORT_STEPS = ['submit', 'received', 'diagnosing', 'complete'];

    // Auto-suggest tags from description text
    const supportDesc = document.getElementById('support-description');
    if (supportDesc) {
        supportDesc.addEventListener('input', function () {
            const text = this.value.toLowerCase();
            document.querySelectorAll('.support-tag').forEach(function (btn) {
                const tag = btn.dataset.tag;
                const keywords = TAG_KEYWORDS[tag] || [];
                const match = keywords.some(function (kw) { return text.includes(kw); });
                if (match && !btn.classList.contains('active')) {
                    btn.classList.add('active');
                } else if (!match && btn.classList.contains('active') && !btn.dataset.manual) {
                    btn.classList.remove('active');
                }
            });
        });
    }

    // Tag toggle buttons
    document.querySelectorAll('.support-tag').forEach(function (btn) {
        btn.addEventListener('click', function () {
            this.classList.toggle('active');
            this.dataset.manual = this.classList.contains('active') ? '1' : '';
        });
    });

    function setSupportStep(stepName, detail) {
        var idx = SUPPORT_STEPS.indexOf(stepName);
        document.querySelectorAll('.support-step').forEach(function (el, i) {
            el.classList.remove('active', 'done');
            if (i < idx) el.classList.add('done');
            else if (i === idx) el.classList.add('active');
        });
        if (detail) {
            var detailEl = document.getElementById('step-' + stepName + '-detail');
            if (detailEl) detailEl.textContent = detail;
        }
    }

    function openSupportModal() {
        _supportModalVisible = true;
        document.getElementById('support-modal-overlay').style.display = 'flex';
        document.getElementById('support-modal-results').style.display = 'none';
        document.getElementById('support-close-btn').style.display = 'none';
        document.getElementById('support-stop-btn').style.display = 'inline-block';
        // Reset steps
        document.querySelectorAll('.support-step').forEach(function (el) {
            el.classList.remove('active', 'done', 'error');
        });
        document.querySelectorAll('.support-step-detail').forEach(function (el) {
            el.textContent = '';
        });
        var indicator = document.querySelector('.support-modal-indicator');
        if (indicator) { indicator.classList.remove('done', 'error'); }
    }

    let _supportModalVisible = false;

    function closeSupportModal(keepPolling) {
        document.getElementById('support-modal-overlay').style.display = 'none';
        _supportModalVisible = false;
        _supportSession = null;
        _supportSessionStart = null;
        if (_supportTtlInterval) { clearInterval(_supportTtlInterval); _supportTtlInterval = null; }
        if (_supportProgressInterval) { clearInterval(_supportProgressInterval); _supportProgressInterval = null; }
        _supportProgressSeen = 0;

        if (keepPolling && _supportPollInterval) {
            // Modal closed but result pending — keep polling in background
            // Give up after 2 minutes
            setTimeout(function () {
                if (_supportPollInterval) {
                    clearInterval(_supportPollInterval);
                    _supportPollInterval = null;
                }
            }, 120000);
        } else {
            if (_supportPollInterval) { clearInterval(_supportPollInterval); _supportPollInterval = null; }
        }
    }

    function _appendProgressEvent(step, elapsedS) {
        var log = document.getElementById('support-activity-log');
        if (!log) return;
        var entry = document.createElement('div');
        entry.className = 'support-activity-entry';
        var mins = Math.floor(elapsedS / 60);
        var secs = Math.round(elapsedS % 60);
        var timeStr = mins > 0 ? mins + 'm ' + secs + 's' : secs + 's';
        entry.innerHTML = '<span class="support-activity-time">' + timeStr + '</span><span class="support-activity-step">' + escapeHtml(step) + '</span>';
        log.appendChild(entry);
        // Keep last 6 entries visible
        while (log.children.length > 6) log.removeChild(log.firstChild);
        log.scrollTop = log.scrollHeight;
    }

    function _updateElapsedClock() {
        if (!_supportSessionStart) return;
        var el = document.getElementById('support-elapsed');
        if (!el) return;
        var elapsed = Math.floor((Date.now() - _supportSessionStart) / 1000);
        var mins = Math.floor(elapsed / 60);
        var secs = elapsed % 60;
        el.textContent = (mins > 0 ? mins + 'm ' : '') + secs + 's elapsed · ~60–90s typical';
    }

    function startProgressPolling() {
        if (_supportProgressInterval) clearInterval(_supportProgressInterval);
        _supportProgressSeen = 0;
        _supportProgressInterval = setInterval(async function () {
            if (!_supportModalVisible) return;
            try {
                var resp = await fetch('/api/support/progress');
                var data = await resp.json();
                var events = data.events || [];
                // Append only new events
                for (var i = _supportProgressSeen; i < events.length; i++) {
                    _appendProgressEvent(events[i].step, events[i].elapsed_s);
                }
                _supportProgressSeen = events.length;
                // Update step 3 detail with latest event
                if (events.length > 0) {
                    var latest = events[events.length - 1].step;
                    var detailEl = document.getElementById('step-diagnosing-detail');
                    if (detailEl) detailEl.textContent = latest;
                }
            } catch (e) { /* ignore */ }
            _updateElapsedClock();
        }, 2500);
    }

    // Submit support request
    var supportSubmitBtn = document.getElementById('support-submit-btn');
    if (supportSubmitBtn) {
        supportSubmitBtn.addEventListener('click', async function () {
            var description = (document.getElementById('support-description').value || '').trim();
            var tags = [];
            document.querySelectorAll('.support-tag.active').forEach(function (btn) {
                tags.push(btn.dataset.tag);
            });

            if (!description && tags.length === 0) {
                alert('Please describe the issue or select at least one tag.');
                return;
            }

            supportSubmitBtn.disabled = true;
            supportSubmitBtn.textContent = 'Starting...';

            openSupportModal();
            setSupportStep('submit', 'Sending to NodusNet...');

            try {
                var resp = await authFetch('/api/support/start', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ description: description, tags: tags }),
                });
                var data = await resp.json();
                if (resp.ok) {
                    _supportSession = data;
                    _supportSessionStart = Date.now();
                    setSupportStep('received', 'NodusNet is preparing diagnostics');
                    startProgressPolling();

                    // Advance to diagnosing after a short delay
                    setTimeout(function () {
                        if (_supportSession && !_supportSession.result) {
                            setSupportStep('diagnosing', '');
                        }
                    }, 1500);

                    // Start fast polling for results
                    startSupportPolling(data.expires_at);
                } else {
                    setSupportStep('submit', 'Failed: ' + (data.error || resp.statusText));
                    document.querySelector('.support-step[data-step="submit"]').classList.add('error');
                    document.querySelector('.support-modal-indicator').classList.add('error');
                    document.getElementById('support-close-btn').style.display = 'inline-block';
                    document.getElementById('support-stop-btn').style.display = 'none';
                }
            } catch (err) {
                setSupportStep('submit', 'Error: ' + err.message);
                document.querySelector('.support-step[data-step="submit"]').classList.add('error');
                document.querySelector('.support-modal-indicator').classList.add('error');
                document.getElementById('support-close-btn').style.display = 'inline-block';
                document.getElementById('support-stop-btn').style.display = 'none';
            } finally {
                supportSubmitBtn.disabled = false;
                supportSubmitBtn.textContent = 'Request Support';
            }
        });
    }

    function startSupportPolling(expiresAt) {
        // Poll every 2 seconds for results
        if (_supportPollInterval) clearInterval(_supportPollInterval);
        _supportModalVisible = true;
        if (!_supportSessionStart) _supportSessionStart = Date.now();
        startProgressPolling();
        _supportPollInterval = setInterval(async function () {
            try {
                var resp = await fetch('/api/support/status');
                var data = await resp.json();
                if (data.result) {
                    clearInterval(_supportPollInterval);
                    _supportPollInterval = null;
                    if (_supportProgressInterval) { clearInterval(_supportProgressInterval); _supportProgressInterval = null; }
                    if (_supportModalVisible) {
                        showSupportComplete(data.result);
                    } else {
                        // Modal was closed (TTL expired) — push to notification bell
                        var statusLabels = {
                            resolved: 'All issues resolved',
                            partial: 'Partially resolved',
                            host_issue: 'Host-side issues found',
                            escalated: 'Escalated for review'
                        };
                        addNotification({
                            type: 'support_result',
                            title: 'Support: ' + (statusLabels[data.result.status] || data.result.status),
                            body: data.result.message || data.result.operator_recommendation || '',
                            data: data.result
                        });
                    }
                } else if (!data.active && _supportModalVisible) {
                    clearInterval(_supportPollInterval);
                    _supportPollInterval = null;
                    closeSupportModal();
                }
            } catch (err) {
                console.error('Support poll error:', err);
            }
        }, 2000);

        // TTL countdown in modal footer
        if (_supportTtlInterval) clearInterval(_supportTtlInterval);
        if (expiresAt) {
            _supportTtlInterval = setInterval(function () {
                var remaining = Math.max(0, Math.floor((new Date(expiresAt) - Date.now()) / 1000));
                var ttlEl = document.getElementById('support-modal-ttl');
                if (remaining <= 0) {
                    clearInterval(_supportTtlInterval);
                    if (ttlEl) ttlEl.textContent = 'Session expired';
                    closeSupportModal(true);
                    return;
                }
                var mins = Math.floor(remaining / 60);
                var secs = remaining % 60;
                if (ttlEl) ttlEl.textContent = 'Expires in ' + mins + 'm ' + (secs < 10 ? '0' : '') + secs + 's';
            }, 1000);
        }
    }

    function showSupportComplete(result) {
        // Advance stepper to complete
        var statusLabels = {
            resolved: 'All issues resolved',
            partial: 'Partially resolved',
            host_issue: 'Host-side issues found',
            escalated: 'Escalated for review'
        };
        setSupportStep('complete', statusLabels[result.status] || result.status);

        var indicator = document.querySelector('.support-modal-indicator');
        if (indicator) {
            indicator.classList.add('done');
            if (result.status === 'resolved') {
                indicator.style.background = 'var(--green)';
            } else {
                indicator.style.background = 'var(--yellow)';
            }
        }

        // Render results
        var container = document.getElementById('support-results-content');
        if (!container) return;

        var html = '';

        // Status banner
        html += '<div class="support-result-status ' + escapeHtml(result.status) + '">';
        if (result.status === 'resolved') html += '&#10003; ';
        else html += '&#9888; ';
        html += escapeHtml(result.message || result.status);
        html += '</div>';

        // Findings
        if (result.findings && result.findings.length > 0) {
            result.findings.forEach(function (f) {
                var icon = f.resolved ? '&#10003;' : '&#10007;';
                var iconColor = f.resolved ? 'var(--green)' : 'var(--red)';
                html += '<div class="support-result-finding">';
                html += '<span class="support-finding-icon" style="color:' + iconColor + '">' + icon + '</span>';
                html += '<span class="support-finding-text">' + escapeHtml(f.finding);
                if (f.zone) html += ' <span class="support-finding-zone">' + escapeHtml(f.zone) + '</span>';
                html += '</span></div>';
            });
        }

        // Actions taken
        if (result.actions_taken && result.actions_taken.length > 0) {
            html += '<div style="margin-top:10px;font-size:11px;color:var(--text-muted)">';
            html += result.actions_taken.length + ' action(s) taken';
            html += '</div>';
        }

        // Operator recommendation
        if (result.operator_recommendation) {
            html += '<div class="support-recommendation">';
            html += '<div class="support-recommendation-label">Recommended Action</div>';
            html += '<div>' + escapeHtml(result.operator_recommendation) + '</div>';
            html += '</div>';
        }

        // Proposed fixes
        if (result.proposed_fixes && result.proposed_fixes.length > 0) {
            html += '<div class="support-fix-panel" id="support-fix-panel">';
            html += '<div class="support-fix-title">&#9881; Fix Available</div>';
            html += '<div class="support-fix-subtitle">The agent found ' + result.proposed_fixes.length + ' configuration change(s) that can be applied automatically:</div>';
            html += '<table class="support-fix-table">';
            html += '<thead><tr><th>Setting</th><th>Current</th><th>Proposed</th></tr></thead><tbody>';
            result.proposed_fixes.forEach(function (fix) {
                html += '<tr>';
                html += '<td class="support-fix-var">' + escapeHtml(fix.var) + '</td>';
                html += '<td class="support-fix-val support-fix-old">' + escapeHtml(fix.current_value) + '</td>';
                html += '<td class="support-fix-val support-fix-new">' + escapeHtml(fix.proposed_value) + '</td>';
                html += '</tr>';
                html += '<tr class="support-fix-reason"><td colspan="3">' + escapeHtml(fix.reason) + '</td></tr>';
            });
            html += '</tbody></table>';
            var needsRestart = result.proposed_fixes.some(function (f) { return f.requires_restart !== false; });
            if (needsRestart) {
                html += '<div class="support-fix-restart-note">&#9888; Applying this fix will restart the container to take effect.</div>';
            }
            html += '<div class="support-fix-actions">';
            html += '<button class="support-fix-apply-btn btn btn-primary" id="support-fix-apply-btn">Apply Fix</button>';
            html += '<button class="support-fix-skip-btn btn btn-secondary" id="support-fix-skip-btn">Skip</button>';
            html += '</div>';
            html += '<div class="support-fix-feedback" id="support-fix-feedback" style="display:none"></div>';
            html += '</div>';
        }

        container.innerHTML = html;
        var activityPanel = document.getElementById('support-activity-panel');
        if (activityPanel) activityPanel.style.display = 'none';
        document.getElementById('support-modal-results').style.display = 'block';
        document.getElementById('support-close-btn').style.display = 'inline-block';
        document.getElementById('support-stop-btn').style.display = 'none';

        // Wire up fix apply/skip buttons after rendering
        var applyBtn = document.getElementById('support-fix-apply-btn');
        var skipBtn = document.getElementById('support-fix-skip-btn');
        var fixFeedback = document.getElementById('support-fix-feedback');
        if (applyBtn && result.proposed_fixes) {
            applyBtn.addEventListener('click', async function () {
                applyBtn.disabled = true;
                applyBtn.textContent = 'Applying...';
                try {
                    var resp = await authFetch('/api/support/apply-fix', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ fixes: result.proposed_fixes }),
                    });
                    var data = await resp.json();
                    if (resp.ok) {
                        fixFeedback.style.display = 'block';
                        fixFeedback.className = 'support-fix-feedback support-fix-feedback-ok';
                        var msg = 'Fix applied: ' + (data.updated || []).join(', ');
                        if (data.restart_required) msg += ' — container restarting, page will reload\u2026';
                        fixFeedback.textContent = msg;
                        applyBtn.style.display = 'none';
                        if (skipBtn) skipBtn.style.display = 'none';
                        if (data.restart_required) {
                            // Poll until the server comes back, then reload
                            setTimeout(function waitForRestart() {
                                fetch('/api/status').then(function (r) {
                                    if (r.ok) { window.location.reload(); }
                                    else { setTimeout(waitForRestart, 2000); }
                                }).catch(function () { setTimeout(waitForRestart, 2000); });
                            }, 3000);
                        }
                    } else {
                        fixFeedback.style.display = 'block';
                        fixFeedback.className = 'support-fix-feedback support-fix-feedback-err';
                        fixFeedback.textContent = 'Error: ' + (data.error || resp.statusText);
                        applyBtn.disabled = false;
                        applyBtn.textContent = 'Apply Fix';
                    }
                } catch (err) {
                    fixFeedback.style.display = 'block';
                    fixFeedback.className = 'support-fix-feedback support-fix-feedback-err';
                    fixFeedback.textContent = 'Error: ' + err.message;
                    applyBtn.disabled = false;
                    applyBtn.textContent = 'Apply Fix';
                }
            });
        }
        if (skipBtn) {
            skipBtn.addEventListener('click', function () {
                var panel = document.getElementById('support-fix-panel');
                if (panel) panel.style.display = 'none';
            });
        }

        // --- Tier escalation feedback ---
        var tier = result.escalation_tier || 1;

        if (tier === 1 && result.status !== 'resolved') {
            // Tier 1 not fully resolved: ask user for feedback
            var fbHtml = '<div class="support-feedback-section" id="support-feedback-section">';
            fbHtml += '<div class="support-feedback-question">Did this resolve your issue?</div>';
            fbHtml += '<div class="support-feedback-actions">';
            fbHtml += '<button class="btn btn-primary" id="support-resolved-btn">Yes, resolved</button>';
            fbHtml += '<button class="btn btn-secondary" id="support-not-resolved-btn">No, still having issues</button>';
            fbHtml += '</div></div>';
            container.insertAdjacentHTML('beforeend', fbHtml);

            document.getElementById('support-resolved-btn').addEventListener('click', async function () {
                try { await authFetch('/api/support/stop', { method: 'POST' }); } catch (e) {}
                closeSupportModal();
            });

            document.getElementById('support-not-resolved-btn').addEventListener('click', function () {
                var sec = document.getElementById('support-feedback-section');
                sec.innerHTML = '<div class="support-feedback-question">What is still wrong?</div>'
                    + '<textarea id="support-escalate-feedback" class="support-escalate-textarea" '
                    + 'placeholder="Describe what you expected or what is still not working..." '
                    + 'rows="3" maxlength="500"></textarea>'
                    + '<button class="btn btn-primary" id="support-escalate-btn">Escalate to Help Desk</button>';

                document.getElementById('support-escalate-btn').addEventListener('click', async function () {
                    var feedback = (document.getElementById('support-escalate-feedback').value || '').trim();
                    if (!feedback) return;
                    this.disabled = true;
                    this.textContent = 'Escalating...';
                    try {
                        var resp = await authFetch('/api/support/escalate', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ feedback: feedback, session_id: result.session_id || '' }),
                        });
                        if (resp.ok) {
                            // Reset UI for Tier 2 progress
                            sec.innerHTML = '<div style="color:var(--green);font-size:12px;">Escalated to Help Desk. Advanced diagnostics in progress...</div>';
                            document.getElementById('support-modal-results').style.display = 'none';
                            var activityPanel = document.getElementById('support-activity-panel');
                            if (activityPanel) activityPanel.style.display = 'block';
                            setSupportStep('diagnosing', 'Help Desk analyzing...');
                            document.getElementById('support-close-btn').style.display = 'none';
                            document.getElementById('support-stop-btn').style.display = 'inline-block';
                            // Resume polling for Tier 2 result
                            startSupportPolling(_supportSession && _supportSession.expires_at);
                        }
                    } catch (err) {
                        this.disabled = false;
                        this.textContent = 'Escalate to Help Desk';
                    }
                });
            });
        } else if (tier === 1 && result.status === 'resolved') {
            // Tier 1 resolved: still offer confirmation
            var fbHtml2 = '<div class="support-feedback-section">';
            fbHtml2 += '<div class="support-feedback-question">Did this resolve your issue?</div>';
            fbHtml2 += '<div class="support-feedback-actions">';
            fbHtml2 += '<button class="btn btn-primary" id="support-resolved-btn">Yes, resolved</button>';
            fbHtml2 += '<button class="btn btn-secondary" id="support-not-resolved-btn">No, still having issues</button>';
            fbHtml2 += '</div></div>';
            container.insertAdjacentHTML('beforeend', fbHtml2);

            document.getElementById('support-resolved-btn').addEventListener('click', async function () {
                try { await authFetch('/api/support/stop', { method: 'POST' }); } catch (e) {}
                closeSupportModal();
            });

            document.getElementById('support-not-resolved-btn').addEventListener('click', function () {
                var sec = document.getElementById('support-feedback-section') || this.closest('.support-feedback-section');
                sec.innerHTML = '<div class="support-feedback-question">What is still wrong?</div>'
                    + '<textarea id="support-escalate-feedback" class="support-escalate-textarea" '
                    + 'placeholder="Describe what you expected or what is still not working..." '
                    + 'rows="3" maxlength="500"></textarea>'
                    + '<button class="btn btn-primary" id="support-escalate-btn">Escalate to Help Desk</button>';

                document.getElementById('support-escalate-btn').addEventListener('click', async function () {
                    var feedback = (document.getElementById('support-escalate-feedback').value || '').trim();
                    if (!feedback) return;
                    this.disabled = true;
                    this.textContent = 'Escalating...';
                    try {
                        var resp = await authFetch('/api/support/escalate', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ feedback: feedback, session_id: result.session_id || '' }),
                        });
                        if (resp.ok) {
                            sec.innerHTML = '<div style="color:var(--green);font-size:12px;">Escalated to Help Desk. Advanced diagnostics in progress...</div>';
                            document.getElementById('support-modal-results').style.display = 'none';
                            var activityPanel = document.getElementById('support-activity-panel');
                            if (activityPanel) activityPanel.style.display = 'block';
                            setSupportStep('diagnosing', 'Help Desk analyzing...');
                            document.getElementById('support-close-btn').style.display = 'none';
                            document.getElementById('support-stop-btn').style.display = 'inline-block';
                            startSupportPolling(_supportSession && _supportSession.expires_at);
                        }
                    } catch (err) {
                        this.disabled = false;
                        this.textContent = 'Escalate to Help Desk';
                    }
                });
            });
        } else if (tier === 2) {
            // Tier 2 result
            if (result.status === 'needs_human') {
                container.insertAdjacentHTML('beforeend',
                    '<div class="support-tier3-notice">'
                    + 'This issue has been escalated to the NodusRF operator for review. '
                    + 'You will be contacted when a resolution is available.'
                    + '</div>');
            }
        }
    }

    // Stop support session
    var supportStopBtn = document.getElementById('support-stop-btn');
    if (supportStopBtn) {
        supportStopBtn.addEventListener('click', async function () {
            if (!confirm('Cancel the support session?')) return;
            supportStopBtn.disabled = true;
            try {
                await authFetch('/api/support/stop', { method: 'POST' });
            } catch (err) {
                console.error('Stop support error:', err);
            }
            closeSupportModal();
        });
    }

    // Close button (after results shown — also resets server state)
    var supportCloseBtn = document.getElementById('support-close-btn');
    if (supportCloseBtn) {
        supportCloseBtn.addEventListener('click', async function () {
            try {
                await authFetch('/api/support/stop', { method: 'POST' });
            } catch (err) { /* ignore */ }
            closeSupportModal();
        });
    }

    // Check for existing active session on tab load
    async function refreshSupport() {
        try {
            var resp = await fetch('/api/support/status');
            var data = await resp.json();

            // Show standalone notice when not connected to NodusNet
            var notice = document.getElementById('support-standalone-notice');
            var submitBtn = document.getElementById('support-submit-btn');
            if (notice) {
                if (data.has_nodusnet === false) {
                    notice.style.display = 'block';
                    if (submitBtn) { submitBtn.disabled = true; submitBtn.title = 'Requires NodusNet connection'; }
                } else {
                    notice.style.display = 'none';
                    if (submitBtn) { submitBtn.disabled = false; submitBtn.title = ''; }
                }
            }

            if (data.active) {
                if (!_supportSession) {
                    _supportSession = data;
                    openSupportModal();
                    if (data.result) {
                        showSupportComplete(data.result);
                    } else {
                        setSupportStep('diagnosing', 'Checking containers, services, hardware...');
                        startSupportPolling(data.expires_at);
                    }
                }
            }
        } catch (err) {
            console.error('Support status fetch error:', err);
        }
    }

    // ========== Init ==========

    // Load initial segments
    async function loadInitialSegments() {
        try {
            const resp = await fetch('/api/segments?limit=50');
            const data = await resp.json();
            const segments = data.segments || [];
            // Add in reverse order so newest is on top
            segments.reverse().forEach(seg => addSegmentToFeed(seg));
        } catch (err) {
            console.error('Initial segments load error:', err);
        }
    }

    // Boot
    fetchConfig();
    loadInitialSegments();
    connectSSE();
    refreshStatus();
    checkVersion();
    refreshWarnings();

    // Initialize spectrum map from frequency data
    (async function initSpectrumCoverage() {
        try {
            const resp = await fetch('/api/frequencies');
            const data = await resp.json();
            initSpectrumMapFromFrequencies(data);
        } catch (err) {
            renderSpectrumMap(); // Render empty map
        }
    })();

    // Periodic refresh for non-live tabs + warnings
    setInterval(() => {
        refreshWarnings();
        const activeTab = document.querySelector('.tab-btn.active');
        if (activeTab) {
            const tab = activeTab.dataset.tab;
            if (tab === 'traffic') refreshTraffic();
            else if (tab === 'status') refreshStatus();
            else if (tab === 'debug') refreshDebug(_debugFilter);
        }
    }, 30000);

    // ========== Feedback Dialog ==========

    const feedbackBtn = document.getElementById('feedback-btn');
    const feedbackOverlay = document.getElementById('feedback-overlay');
    const feedbackClose = document.getElementById('feedback-close');
    const feedbackCancel = document.getElementById('feedback-cancel');
    const feedbackSubmit = document.getElementById('feedback-submit');
    const feedbackTitle = document.getElementById('feedback-title');
    const feedbackText = document.getElementById('feedback-text');
    const feedbackCharCount = document.getElementById('feedback-char-count');
    var _feedbackCategoryMap = { bug: 'Bug Report', feature: 'Feature Request', general: 'General Feedback', question: 'Question' };
    let feedbackType = 'bug';

    function openFeedback() { feedbackOverlay.style.display = 'flex'; }
    function closeFeedback() {
        feedbackOverlay.style.display = 'none';
        if (feedbackTitle) feedbackTitle.value = '';
        feedbackText.value = '';
        if (feedbackCharCount) feedbackCharCount.textContent = '0 / 2000';
    }

    if (feedbackBtn) feedbackBtn.addEventListener('click', openFeedback);
    if (feedbackClose) feedbackClose.addEventListener('click', closeFeedback);
    if (feedbackCancel) feedbackCancel.addEventListener('click', closeFeedback);
    if (feedbackOverlay) feedbackOverlay.addEventListener('click', function (e) {
        if (e.target === feedbackOverlay) closeFeedback();
    });

    if (feedbackText && feedbackCharCount) {
        feedbackText.addEventListener('input', function () {
            feedbackCharCount.textContent = feedbackText.value.length + ' / 2000';
        });
    }

    document.querySelectorAll('.feedback-type-btn').forEach(function (btn) {
        btn.addEventListener('click', function () {
            document.querySelectorAll('.feedback-type-btn').forEach(function (b) { b.classList.remove('selected'); });
            btn.classList.add('selected');
            feedbackType = btn.dataset.type;
        });
    });

    if (feedbackSubmit) feedbackSubmit.addEventListener('click', function () {
        var title = feedbackTitle ? feedbackTitle.value.trim() : '';
        var body = feedbackText.value.trim();
        if (!body) return;
        feedbackSubmit.disabled = true;
        feedbackSubmit.textContent = 'Sending...';
        fetch('/api/feedback', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                title: title || (_feedbackCategoryMap[feedbackType] || feedbackType),
                body: body,
                category: _feedbackCategoryMap[feedbackType] || feedbackType,
                page_url: window.location.href,
                node_id: _nodeId,
            }),
        }).then(function (resp) {
            if (resp.ok) {
                feedbackSubmit.textContent = 'Sent!';
                setTimeout(function () {
                    closeFeedback();
                    feedbackSubmit.disabled = false;
                    feedbackSubmit.textContent = 'Send';
                }, 1200);
            } else {
                feedbackSubmit.textContent = 'Error';
                setTimeout(function () {
                    feedbackSubmit.textContent = 'Send';
                    feedbackSubmit.disabled = false;
                }, 2000);
            }
        }).catch(function () {
            feedbackSubmit.textContent = 'Send';
            feedbackSubmit.disabled = false;
        });
    });

    // ========== JSON Payload Modal ==========

    const payloadOverlay = document.getElementById('payload-overlay');
    const payloadClose = document.getElementById('payload-close');
    const payloadDismiss = document.getElementById('payload-dismiss');
    const payloadCopy = document.getElementById('payload-copy');
    const payloadJson = document.getElementById('payload-json');

    function closePayload() { if (payloadOverlay) payloadOverlay.style.display = 'none'; }

    window._showPayload = function (idx) {
        const entry = window._auditPayloads && window._auditPayloads[idx];
        if (!entry || !payloadJson || !payloadOverlay) return;
        payloadJson.textContent = JSON.stringify(entry, null, 2);
        payloadOverlay.style.display = 'flex';
    };

    if (payloadClose) payloadClose.addEventListener('click', closePayload);
    if (payloadDismiss) payloadDismiss.addEventListener('click', closePayload);
    if (payloadOverlay) payloadOverlay.addEventListener('click', function (e) {
        if (e.target === payloadOverlay) closePayload();
    });
    if (payloadCopy) payloadCopy.addEventListener('click', function () {
        if (payloadJson) {
            navigator.clipboard.writeText(payloadJson.textContent).then(function () {
                payloadCopy.textContent = 'Copied!';
                setTimeout(function () { payloadCopy.textContent = 'Copy'; }, 1200);
            });
        }
    });

    // ========== Notification Bell (localStorage-backed) ==========

    const NOTIF_STORAGE_KEY = 'nodus_notifications';
    const NOTIF_MAX = 50;

    function getNotifications() {
        try { return JSON.parse(localStorage.getItem(NOTIF_STORAGE_KEY)) || []; }
        catch (e) { return []; }
    }

    function saveNotifications(list) {
        localStorage.setItem(NOTIF_STORAGE_KEY, JSON.stringify(list.slice(0, NOTIF_MAX)));
    }

    function addNotification(notif) {
        var list = getNotifications();
        notif.id = notif.id || Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
        notif.created_at = notif.created_at || new Date().toISOString();
        notif.read = false;
        list.unshift(notif);
        saveNotifications(list);
        renderNotifications();
    }

    function renderNotifications() {
        var list = getNotifications();
        var badge = document.getElementById('notif-badge');
        var body = document.getElementById('notif-list');
        var unread = list.filter(function (n) { return !n.read; }).length;

        if (badge) {
            badge.textContent = unread;
            badge.style.display = unread > 0 ? 'flex' : 'none';
        }

        if (!body) return;
        if (list.length === 0) {
            body.innerHTML = '<div class="notif-empty">No notifications yet</div>';
            return;
        }

        body.innerHTML = list.map(function (n) {
            var icon = n.type === 'support_result' ? '\u{1F6E0}' : n.type === 'server_push' ? '\u{1F4E8}' : '\u{1F514}';
            var unreadClass = n.read ? '' : ' notif-unread';
            var ago = _timeAgo(n.created_at);
            return '<div class="notif-item' + unreadClass + '" data-notif-id="' + escapeHtml(n.id) + '">'
                + '<span class="notif-icon">' + icon + '</span>'
                + '<div class="notif-body">'
                + '<div class="notif-title">' + escapeHtml(n.title || 'Notification') + '</div>'
                + '<div class="notif-text">' + escapeHtml(n.body || '') + '</div>'
                + '<div class="notif-time">' + ago + '</div>'
                + '</div></div>';
        }).join('');
    }

    function _timeAgo(iso) {
        var diff = Math.floor((Date.now() - new Date(iso).getTime()) / 1000);
        if (diff < 60) return 'just now';
        if (diff < 3600) return Math.floor(diff / 60) + 'm ago';
        if (diff < 86400) return Math.floor(diff / 3600) + 'h ago';
        return Math.floor(diff / 86400) + 'd ago';
    }

    const notifBtn = document.getElementById('notif-btn');
    const notifDropdown = document.getElementById('notif-dropdown');

    if (notifBtn) notifBtn.addEventListener('click', function (e) {
        e.stopPropagation();
        const isOpen = notifDropdown.style.display !== 'none';
        notifDropdown.style.display = isOpen ? 'none' : 'flex';
        if (!isOpen) {
            // Mark all as read on open
            var list = getNotifications();
            var changed = false;
            list.forEach(function (n) { if (!n.read) { n.read = true; changed = true; } });
            if (changed) { saveNotifications(list); renderNotifications(); }
        }
    });

    // Click a notification to expand support result
    if (document.getElementById('notif-list')) {
        document.getElementById('notif-list').addEventListener('click', function (e) {
            var item = e.target.closest('.notif-item');
            if (!item) return;
            var id = item.dataset.notifId;
            var list = getNotifications();
            var notif = list.find(function (n) { return n.id === id; });
            if (notif && notif.type === 'support_result' && notif.data) {
                // Switch to support tab and show the result in the modal
                document.querySelector('.tab-btn[data-tab="support"]').click();
                openSupportModal();
                showSupportComplete(notif.data);
                notifDropdown.style.display = 'none';
            }
        });
    }

    // Close dropdown on outside click
    document.addEventListener('click', function (e) {
        if (notifDropdown && !notifDropdown.contains(e.target) && e.target !== notifBtn) {
            notifDropdown.style.display = 'none';
        }
    });

    // Fetch server-side notifications on load (catches ones received while dashboard was closed)
    fetch('/api/notifications')
        .then(function (r) { return r.json(); })
        .then(function (data) {
            var existing = getNotifications();
            var existingIds = new Set(existing.map(function (n) { return n.id; }));
            (data.notifications || []).forEach(function (n) {
                if (!existingIds.has(n.id)) {
                    addNotification({
                        title: n.title || 'NodusRF',
                        body: n.body || '',
                        type: 'server_push',
                        id: n.id,
                        created_at: n.created_at,
                    });
                }
            });
        })
        .catch(function () {});

    // Render on load
    renderNotifications();

})();
