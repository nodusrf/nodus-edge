#!/usr/bin/env python3
"""
Nodus Edge - Main Entry Point

Distributed radio ingestion for public safety intelligence.

Usage:
    nodus-edge                    # Run daemon (watch mode)
    nodus-edge --backfill         # Process existing files then watch
    nodus-edge --once             # Process existing files and exit
"""

import argparse
import logging
import signal
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from queue import Queue, Empty, Full
from threading import Thread, Event
from typing import Optional

import structlog

from .config import settings
from .ingestion.watcher import SDRTrunkWatcher
from .ingestion.fm_scanner import FMScanner
from .ingestion.adaptive_scanner import AdaptiveFMScanner
from .ingestion.airband_scanner import AirbandScanner
from .ingestion.tr_watcher import TRWatcher
from .ingestion.tr_schema import TRCallJSON
from .pipeline import EdgePipeline
from .fm_pipeline import FMPipeline
from .hf_pipeline import HFPipeline
from .aprs_pipeline import APRSPipeline
from .coverage import CoverageReporter
from .heartbeat import HeartbeatEmitter
from .health_server import HealthServer
from .transcription.audit_log import audit_log
from .dashboard.segment_store import SegmentStore
from .dashboard.sync_cache import SyncCache
from .dashboard.server import start_dashboard
from .threading.thread_grouper import ThreadGrouper
from .threading.keyword_scanner import KeywordScanner
from .connectivity import ConnectivityProbe
from .rem_checkin import REMCheckIn
from .validation import validate_startup_config


def configure_logging() -> None:
    """Configure structured logging."""
    # Configure stdlib logging first (required for structlog.stdlib integration)
    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level,
    )

    processors = [
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if settings.log_format == "json":
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer(colors=True))

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


logger = structlog.get_logger(__name__)


class EdgeDaemon:
    """
    Main daemon for Nodus Edge.

    Supports two modes:
    - P25 mode: Watches SDRTrunk or Trunk Recorder output for recordings
    - FM mode: Scans ham radio frequencies and captures audio

    P25 mode supports two sources:
    - sdrtrunk: Watches SDRTrunk recording directories
    - trunk-recorder: Watches Trunk Recorder JSON/WAV output

    Both modes process audio through Whisper transcription and
    emit segments to Synapse.
    """

    # Interval for scanning event logs for encrypted calls (seconds, P25 mode only)
    ENCRYPTED_SCAN_INTERVAL = 30

    def __init__(self):
        self.mode = settings.mode
        self.p25_source = settings.p25_source if self.mode == "p25" else None

        # Mode-specific components
        if self.mode == "p25":
            self.pipeline = EdgePipeline()
            self.watcher: Optional[SDRTrunkWatcher] = None
            self.tr_watcher: Optional[TRWatcher] = None
        elif self.mode == "fm":
            self.pipeline = FMPipeline()
            self.fm_scanner: Optional[FMScanner] = None
        elif self.mode == "hf":
            self.pipeline = HFPipeline()
        elif self.mode == "aprs":
            self.pipeline = APRSPipeline()
        else:
            raise ValueError(f"Unknown mode: {self.mode}")

        self.heartbeat_emitter: Optional[HeartbeatEmitter] = None
        self.rem_checkin: Optional[REMCheckIn] = None
        self.coverage_reporter: Optional[CoverageReporter] = None
        self.health_server: Optional[HealthServer] = None
        self.thread_grouper: Optional[ThreadGrouper] = None
        self.keyword_scanner: Optional[KeywordScanner] = None
        self.connectivity_probe: Optional[ConnectivityProbe] = None

        self._work_queue: Queue[tuple] = Queue(maxsize=settings.whisper_queue_maxsize)
        self._shutdown_event = Event()
        self._worker_thread: Optional[Thread] = None
        self._segments_dropped_count: int = 0
        self._encrypted_scanner_thread: Optional[Thread] = None
        self._last_encrypted_scan: Optional[datetime] = None

    def _start_rem_checkin(
        self,
        get_frequencies: Optional[callable] = None,
    ) -> None:
        """Start REM check-in if endpoint is configured.

        Blocks up to 15s for the first check-in so that a compliance
        token is available before any segments are emitted.
        """
        if not settings.rem_endpoint:
            return

        self.rem_checkin = REMCheckIn(
            rem_endpoint=settings.rem_endpoint,
            node_id=settings.node_id,
            auth_token=settings.synapse_auth_token,
            get_stats=self.pipeline.get_stats,
            get_frequencies=get_frequencies,
        )
        self.rem_checkin.start()

        # Wire compliance token into Synapse publisher so segments carry it
        if hasattr(self.pipeline, 'synapse_publisher') and self.pipeline.synapse_publisher:
            self.pipeline.synapse_publisher.rem_checkin = self.rem_checkin

        # Wait for first check-in to complete so segments carry a token
        if not self.rem_checkin.first_checkin_done.wait(timeout=15):
            logger.warning("First REM check-in did not complete within 15s, starting without token")

    def start(self, backfill: bool = False, once: bool = False) -> None:
        """
        Start the daemon.

        Args:
            backfill: Process existing files before watching (P25 mode only)
            once: Process existing files and exit (P25 mode only)
        """
        if self.mode == "p25":
            self._start_p25_mode(backfill, once)
        elif self.mode == "fm":
            self._start_fm_mode()
        elif self.mode == "hf":
            self._start_hf_mode()
        elif self.mode == "aprs":
            self._start_aprs_mode()

    def _start_p25_mode(self, backfill: bool = False, once: bool = False) -> None:
        """Start in P25 mode - dispatch to SDRTrunk or Trunk Recorder path."""
        if self.p25_source == "trunk-recorder":
            self._start_p25_tr_mode()
        else:
            self._start_p25_sdrtrunk_mode(backfill, once)

    def _start_p25_sdrtrunk_mode(self, backfill: bool = False, once: bool = False) -> None:
        """Start in P25 mode with SDRTrunk as source."""
        logger.info(
            "Starting Nodus Edge daemon (P25/SDRTrunk mode)",
            node_id=settings.node_id,
            recordings_dir=str(settings.recordings_path),
            output_dir=str(settings.output_path),
            transcription_enabled=settings.transcription_enabled,
        )

        # Start heartbeat emitter if diagnostics endpoint configured
        if settings.diagnostics_endpoint:
            self.heartbeat_emitter = HeartbeatEmitter(
                diagnostics_endpoint=settings.diagnostics_endpoint,
                service="nodus-edge",
                node_id=settings.node_id,
                get_stats=self.pipeline.get_stats,
            )
            self.heartbeat_emitter.start()

        # Start REM check-in
        self._start_rem_checkin()

        # Start worker thread
        self._worker_thread = Thread(target=self._worker_loop, daemon=True)
        self._worker_thread.start()

        # Start encrypted call scanner thread
        self._encrypted_scanner_thread = Thread(
            target=self._encrypted_scanner_loop, daemon=True
        )
        self._encrypted_scanner_thread.start()
        logger.info("Encrypted call scanner started", interval=self.ENCRYPTED_SCAN_INTERVAL)

        # Set up watcher
        self.watcher = SDRTrunkWatcher(
            on_new_recording=self._on_new_p25_file,
        )

        # Backfill existing files if requested
        if backfill or once:
            logger.info("Processing existing files...")
            count = self.watcher.scan_existing(self._on_new_p25_file)
            logger.info("Backfill complete", files_queued=count)

            # Wait for queue to drain
            self._work_queue.join()

            if once:
                logger.info("One-shot mode complete, exiting")
                self._print_stats()
                return

        # Start watching
        self.watcher.start()
        logger.info("Watching for new recordings...")

        # Wait for shutdown
        try:
            while not self._shutdown_event.is_set():
                time.sleep(1)
        except KeyboardInterrupt:
            pass

        self.stop()

    def _start_p25_tr_mode(self) -> None:
        """Start in P25 mode with Trunk Recorder as source."""
        logger.info(
            "Starting Nodus Edge daemon (P25/Trunk Recorder mode)",
            node_id=settings.node_id,
            tr_capture_dir=str(settings.tr_capture_path),
            output_dir=str(settings.output_path),
            transcription_enabled=settings.transcription_enabled,
        )

        # Start heartbeat emitter if diagnostics endpoint configured
        if settings.diagnostics_endpoint:
            self.heartbeat_emitter = HeartbeatEmitter(
                diagnostics_endpoint=settings.diagnostics_endpoint,
                service="nodus-edge-p25",
                node_id=settings.node_id,
                get_stats=self.pipeline.get_stats,
            )
            self.heartbeat_emitter.start()

        # Start REM check-in
        self._start_rem_checkin()

        # Start worker thread
        self._worker_thread = Thread(target=self._worker_loop, daemon=True)
        self._worker_thread.start()

        # No encrypted scanner needed - TR reports encryption in JSON directly

        # Set up TR watcher
        self.tr_watcher = TRWatcher(
            recordings_dir=settings.tr_capture_path,
            poll_interval=settings.poll_interval_seconds,
            settle_time=settings.tr_settle_time_seconds,
        )

        # Start watching (blocking via run loop in main thread)
        logger.info("Watching for Trunk Recorder output...")

        try:
            self.tr_watcher.run(callback=self._on_tr_call)
        except KeyboardInterrupt:
            pass

        self.stop()

    def _start_fm_mode(self) -> None:
        """Start in FM mode (frequency scanning)."""
        # Resolve frequency list
        all_frequencies = settings.fm_core_frequencies or settings.fm_frequencies
        if not all_frequencies:
            raise ValueError(
                "No FM frequencies configured. Set NODUS_EDGE_FM_CORE_FREQUENCIES "
                "or NODUS_EDGE_FM_FREQUENCIES."
            )

        # Determine scanner backend
        use_airband = settings.fm_scanner_backend == "airband"
        use_adaptive = bool(settings.fm_core_frequencies) and not use_airband

        if use_airband:
            logger.info(
                "Starting Nodus Edge daemon (FM airband mode)",
                node_id=settings.node_id,
                channels=len(all_frequencies),
                output_dir=str(settings.output_path),
                transcription_enabled=settings.transcription_enabled,
            )
        elif use_adaptive:
            logger.info(
                "Starting Nodus Edge daemon (FM adaptive mode)",
                node_id=settings.node_id,
                core_frequencies=[f/1_000_000 for f in settings.fm_core_frequencies],
                candidate_count=len(settings.fm_candidate_frequencies),
                output_dir=str(settings.output_path),
                transcription_enabled=settings.transcription_enabled,
            )
        else:
            logger.info(
                "Starting Nodus Edge daemon (FM mode)",
                node_id=settings.node_id,
                frequencies=[f/1_000_000 for f in settings.fm_frequencies],
                output_dir=str(settings.output_path),
                transcription_enabled=settings.transcription_enabled,
            )

        # Start worker thread
        self._worker_thread = Thread(target=self._worker_loop, daemon=True)
        self._worker_thread.start()

        # Set up FM scanner
        if use_airband:
            try:
                # Merge core + candidate frequencies for simultaneous monitoring
                combined = list(set(
                    settings.fm_core_frequencies
                    + settings.fm_candidate_frequencies
                    + settings.fm_frequencies
                ))
                self.fm_scanner = AirbandScanner(
                    frequencies=combined,
                    on_segment=self._on_new_fm_segment,
                )
                logger.info(
                    "RTLSDR-Airband scanner initialized",
                    channels=len(combined),
                    center_mhz=self.fm_scanner._center_freq_hz / 1_000_000,
                )
            except FileNotFoundError as e:
                logger.warning(
                    "RTLSDR-Airband binary not found, falling back to legacy scanner",
                    error=str(e),
                )
                use_airband = False
                use_adaptive = bool(settings.fm_core_frequencies)

        if not use_airband:
            if use_adaptive:
                self.fm_scanner = AdaptiveFMScanner(
                    on_segment=self._on_new_fm_segment,
                    core_frequencies=settings.fm_core_frequencies,
                    candidate_frequencies=settings.fm_candidate_frequencies,
                )
                logger.info(
                    "Adaptive FM scanner initialized",
                    core=len(settings.fm_core_frequencies),
                    candidates=len(settings.fm_candidate_frequencies),
                    max_active=settings.fm_max_active_frequencies,
                    demotion_timeout_min=settings.fm_demotion_timeout_minutes,
                )
            else:
                self.fm_scanner = FMScanner(
                    frequencies=settings.fm_frequencies,
                    on_segment=self._on_new_fm_segment,
                )

        # Report spectrum coverage to Gateway for metro coordination
        if settings.gateway_url and settings.metro:
            self.coverage_reporter = CoverageReporter(
                gateway_url=settings.gateway_url,
                node_id=settings.node_id,
                metro=settings.metro,
                mode="fm",
                auth_token=settings.synapse_auth_token,
            )
            self.coverage_reporter.report(
                core_frequencies=settings.fm_core_frequencies,
                candidate_frequencies=settings.fm_candidate_frequencies,
            )

        # Start heartbeat emitter after scanner creation so we can include
        # scanner capture stats (watchdog restarts, segments/hour, etc.)
        if settings.diagnostics_endpoint:
            scanner = self.fm_scanner
            coverage_reporter = self.coverage_reporter

            def combined_stats():
                stats = self.pipeline.get_stats()
                stats["metro"] = settings.metro or ""
                if hasattr(scanner, 'get_capture_stats'):
                    stats["scanner"] = scanner.get_capture_stats()
                if coverage_reporter and coverage_reporter.coverage_hash:
                    stats["coverage_hash"] = coverage_reporter.coverage_hash
                return stats

            self.heartbeat_emitter = HeartbeatEmitter(
                diagnostics_endpoint=settings.diagnostics_endpoint,
                service="nodus-edge-fm",
                node_id=settings.node_id,
                get_stats=combined_stats,
                auth_token=settings.synapse_auth_token,
            )
            self.heartbeat_emitter.start()

        # Start REM check-in (with frequency reporting)
        def _get_frequencies():
            return list(set(
                (settings.fm_core_frequencies or [])
                + (settings.fm_candidate_frequencies or [])
                + (settings.fm_frequencies or [])
            ))
        self._start_rem_checkin(get_frequencies=_get_frequencies)

        # Start scanning
        self.fm_scanner.start()
        logger.info("FM scanner started")

        # Start health + audio server (serves /health, /stats, /audio/{filename})
        scanner_ref = self.fm_scanner

        def health_stats():
            stats = self.pipeline.get_stats()
            if hasattr(scanner_ref, 'get_capture_stats'):
                stats["scanner"] = scanner_ref.get_capture_stats()
            return stats

        self.health_server = HealthServer(
            port=8082,
            node_id=settings.node_id,
            get_stats=health_stats,
            audio_dir=settings.fm_capture_path,
            operator_cache=getattr(self.pipeline, 'operator_cache', None),
            audit_log=audit_log,
            synapse_publisher=self.pipeline.synapse_publisher,
            scanner=scanner_ref,
        )
        self.health_server.start()

        # Run startup validation
        repeater_db = self.pipeline._repeater_db
        startup_warnings = validate_startup_config(
            repeater_db_loaded=repeater_db.is_loaded() if hasattr(repeater_db, 'is_loaded') else bool(repeater_db.get_all_frequencies()),
            repeater_count=len(repeater_db.get_all_frequencies()),
            frequencies=all_frequencies,
            synapse_endpoint=settings.synapse_endpoint,
            node_id=settings.node_id,
            metro=settings.metro,
        )
        if startup_warnings:
            for w in startup_warnings:
                logger.warning("Startup validation", code=w.code, message=w.message, severity=w.severity)

        # Start edge-thread grouper and keyword scanner
        self.keyword_scanner = KeywordScanner.from_yaml(settings.watchlist_path)
        self.thread_grouper = ThreadGrouper(
            db_path=settings.thread_db_path,
            gap_seconds=settings.thread_gap_seconds,
            prune_hours=settings.thread_prune_hours,
        )

        # Wire keyword scanner: scan each segment immediately
        def _on_segment_for_keywords(segment_data):
            # Get thread_id for context (find open thread on same freq)
            rf = segment_data.get("rf_channel", {})
            freq = rf.get("frequency_hz", 0)
            open_threads = self.thread_grouper.get_open_threads()
            thread_id = None
            for t in open_threads:
                if t["frequency_hz"] == freq:
                    thread_id = t["thread_id"]
                    break
            matches = self.keyword_scanner.scan_segment(
                segment_data, thread_id=thread_id, frequency_hz=freq,
            )
            # Record keywords on the thread
            if matches and thread_id:
                labels = [m.label for m in matches]
                self.thread_grouper.update_thread_keywords(thread_id, labels)

        self.pipeline.register_segment_callback(_on_segment_for_keywords)

        # Wire keyword scanner to thread close (multi-segment patterns)
        def _on_thread_close_keywords(thread_info):
            matches = self.keyword_scanner.scan_thread(thread_info)
            if matches:
                labels = [m.label for m in matches]
                self.thread_grouper.update_thread_keywords(
                    thread_info["thread_id"], labels,
                )

        self.thread_grouper.on_thread_close(_on_thread_close_keywords)

        # Register thread grouper as a segment callback
        self.pipeline.register_segment_callback(self.thread_grouper.on_segment)

        self.thread_grouper.start()
        logger.info(
            "Edge-thread grouper active",
            gap_seconds=settings.thread_gap_seconds,
            watchlist=settings.watchlist_path,
        )

        # Start connectivity probe (internet reachability)
        probe_url = settings.connectivity_probe_url
        if not probe_url and settings.synapse_endpoint:
            probe_url = settings.synapse_endpoint.rstrip("/") + "/health"
        if probe_url:
            self.connectivity_probe = ConnectivityProbe(
                probe_url=probe_url,
                interval_sec=settings.connectivity_probe_interval_sec,
                fail_threshold=settings.connectivity_fail_threshold,
            )
            self.connectivity_probe.start()

        # Start edge dashboard if enabled
        if settings.dashboard_enabled:
            store = SegmentStore(max_segments=settings.dashboard_max_segments)
            self.pipeline.register_segment_callback(store.add_segment)

            # Resolve bundled repeaters path
            bundled_repeaters = None
            try:
                from .data.ham_data import get_repeater_db
                rdb = get_repeater_db()
                if hasattr(rdb, '_data_path') and rdb._data_path:
                    bundled_repeaters = Path(rdb._data_path)
            except Exception:
                pass

            cache = SyncCache(
                gateway_url=settings.synapse_endpoint,
                auth_token=settings.synapse_auth_token,
                bundled_repeaters_path=bundled_repeaters,
            )

            # Background sync on startup if endpoint configured
            if cache.can_sync and not cache.has_repeaters:
                try:
                    cache.sync()
                except Exception as e:
                    logger.debug("Initial dashboard sync failed", error=str(e))

            # Wire signal data into coverage reporter for quality-aware suggestions
            if self.coverage_reporter:
                self.coverage_reporter.get_signal_db = store.get_avg_signal_db

            start_dashboard(
                store=store,
                cache=cache,
                port=settings.dashboard_port,
                node_id=settings.node_id,
                health_port=8082,
                timezone=settings.timezone,
                metro=settings.metro,
                dashboard_token=settings.dashboard_token,
                channel_frequencies=getattr(self.fm_scanner, 'frequencies', []),
                squelch_snr_db=settings.fm_airband_squelch_snr_db,
                env_path=Path("/app/.env"),
                startup_warnings=startup_warnings,
                get_segment_warnings=self.pipeline.get_segment_warning_counts,
                get_pipeline_stats=self.pipeline.get_stats,
                rem_checkin=self.rem_checkin,
            )

        # Wait for shutdown
        try:
            while not self._shutdown_event.is_set():
                time.sleep(1)
        except KeyboardInterrupt:
            pass

        self.stop()

    def _start_aprs_mode(self) -> None:
        """Start in APRS mode (Direwolf packet decoding)."""
        from .ingestion.aprs_decoder import APRSDecoder

        logger.info(
            "Starting Nodus Edge daemon (APRS mode)",
            node_id=settings.node_id,
            frequency_mhz=settings.aprs_frequency_hz / 1_000_000,
            device_index=settings.aprs_device_index,
        )

        # Start heartbeat emitter
        if settings.diagnostics_endpoint:
            self.heartbeat_emitter = HeartbeatEmitter(
                diagnostics_endpoint=settings.diagnostics_endpoint,
                service="nodus-edge-aprs",
                node_id=settings.node_id,
                get_stats=self.pipeline.get_stats,
                auth_token=settings.synapse_auth_token,
            )
            self.heartbeat_emitter.start()

        # Start REM check-in
        self._start_rem_checkin(
            get_frequencies=lambda: [settings.aprs_frequency_hz],
        )

        # Start health server
        self.health_server = HealthServer(
            port=8082,
            node_id=settings.node_id,
            get_stats=self.pipeline.get_stats,
        )
        self.health_server.start()

        # Start APRS decoder (rtl_fm | direwolf pipeline)
        self.aprs_decoder = APRSDecoder(
            on_packet=self.pipeline.process_packet,
            frequency_hz=settings.aprs_frequency_hz,
            device_index=settings.aprs_device_index,
            gain=settings.aprs_gain,
            sample_rate=settings.aprs_sample_rate,
        )
        self.aprs_decoder.start()

        # Start dashboard if enabled
        if settings.dashboard_enabled:
            store = SegmentStore(max_segments=settings.dashboard_max_segments)
            self.pipeline.register_segment_callback(store.add_segment)
            cache = SyncCache()
            start_dashboard(
                store=store,
                cache=cache,
                port=settings.dashboard_port,
                node_id=settings.node_id,
            )

        # Wait for shutdown
        try:
            while not self._shutdown_event.is_set():
                time.sleep(1)
        except KeyboardInterrupt:
            pass

        self.stop()

    def _start_hf_mode(self) -> None:
        """Start in HF mode (USB audio capture + optional CAT)."""
        logger.info(
            "Starting Nodus Edge daemon (HF mode)",
            node_id=settings.node_id,
            station_callsign=settings.hf_station_callsign,
            cat_protocol=settings.hf_cat_protocol,
            audio_device=settings.hf_audio_device,
        )

        # Start heartbeat emitter
        if settings.diagnostics_endpoint:
            self.heartbeat_emitter = HeartbeatEmitter(
                diagnostics_endpoint=settings.diagnostics_endpoint,
                service="nodus-edge-hf",
                node_id=settings.node_id,
                get_stats=self.pipeline.get_stats,
                auth_token=settings.synapse_auth_token,
            )
            self.heartbeat_emitter.start()

        # Start REM check-in
        self._start_rem_checkin()

        # Start health server
        self.health_server = HealthServer(get_stats=self.pipeline.get_stats)
        self.health_server.start()

        # Start the HF pipeline (manages audio capture + CAT internally)
        self.pipeline.start()

        # Start dashboard if enabled
        if settings.dashboard_enabled:
            segment_store = SegmentStore(max_segments=settings.dashboard_max_segments)
            self.pipeline.set_segment_callback(segment_store.add_segment)
            start_dashboard(
                segment_store=segment_store,
                port=settings.dashboard_port,
            )

        # Wait for shutdown
        try:
            while not self._shutdown_event.is_set():
                time.sleep(1)
        except KeyboardInterrupt:
            pass

        self.stop()

    def stop(self) -> None:
        """Stop the daemon gracefully."""
        logger.info(f"Stopping Nodus Edge daemon ({self.mode} mode)...")
        self._shutdown_event.set()

        if self.health_server:
            self.health_server.stop()

        if self.heartbeat_emitter:
            self.heartbeat_emitter.stop()

        if self.rem_checkin:
            self.rem_checkin.stop()

        if self.mode == "p25" and hasattr(self, 'watcher') and self.watcher:
            self.watcher.stop()
        # TR watcher runs in the main thread via run(), stops on KeyboardInterrupt
        elif self.mode == "fm" and hasattr(self, 'fm_scanner') and self.fm_scanner:
            self.fm_scanner.stop()
        elif self.mode == "hf" and hasattr(self.pipeline, 'stop'):
            self.pipeline.stop()
        elif self.mode == "aprs" and hasattr(self, 'aprs_decoder'):
            self.aprs_decoder.stop()

        # Stop edge-thread grouper and connectivity probe
        if self.thread_grouper:
            self.thread_grouper.stop()
        if self.connectivity_probe:
            self.connectivity_probe.stop()

        # Shut down pipeline background resources (shadow whisper thread pool)
        if hasattr(self, 'pipeline') and hasattr(self.pipeline, 'shutdown'):
            self.pipeline.shutdown()

        # Wait for worker to drain queue (with timeout to prevent hang)
        if self._worker_thread and self._worker_thread.is_alive():
            self._worker_thread.join(timeout=10)
            if self._worker_thread.is_alive():
                logger.warning("Worker thread did not finish within 10s")

        self._print_stats()
        logger.info("Nodus Edge daemon stopped")

    def _on_new_p25_file(self, path: Path) -> None:
        """Callback when new P25 recording is detected."""
        try:
            self._work_queue.put(("p25", path, None), timeout=5)
        except Full:
            self._segments_dropped_count += 1
            logger.error("Work queue full, dropping recording", path=str(path),
                         dropped_count=self._segments_dropped_count)

    def _on_tr_call(self, json_path: Path, call_data: TRCallJSON) -> None:
        """Callback when Trunk Recorder emits a new call."""
        try:
            self._work_queue.put(("tr", json_path, call_data), timeout=5)
        except Full:
            self._segments_dropped_count += 1
            logger.error("Work queue full, dropping TR call", path=str(json_path),
                         dropped_count=self._segments_dropped_count)

    def _on_new_fm_segment(self, path: Path, frequency_hz: int, signal_db: float = None) -> None:
        """Callback when new FM segment is captured."""
        try:
            self._work_queue.put(("fm", path, (frequency_hz, signal_db)), timeout=5)
        except Full:
            self._segments_dropped_count += 1
            logger.error("Work queue full, dropping FM segment", path=str(path),
                         frequency_hz=frequency_hz, dropped_count=self._segments_dropped_count)

    def _worker_loop(self) -> None:
        """Worker thread that processes files from the queue."""
        while not self._shutdown_event.is_set():
            try:
                item = self._work_queue.get(timeout=1.0)
            except Empty:
                continue

            try:
                mode, path, extra = item
                if mode == "p25":
                    self.pipeline.process_recording(path)
                elif mode == "fm":
                    if isinstance(extra, tuple):
                        freq_hz, sig_db = extra
                    else:
                        freq_hz, sig_db = extra, None
                    self.pipeline.process_recording(path, frequency_hz=freq_hz, signal_db=sig_db)
                elif mode == "tr":
                    json_path = path
                    call_data = extra  # TRCallJSON
                    audio_path = self.tr_watcher.get_audio_path(json_path)
                    if audio_path:
                        self.pipeline.process_tr_recording(json_path, audio_path, call_data)
                    else:
                        logger.warning("No audio file for TR call", json_path=str(json_path))
            except Exception as e:
                logger.error(
                    "Error processing recording",
                    path=str(path),
                    mode=mode,
                    error=str(e),
                )
            finally:
                self._work_queue.task_done()

    def _encrypted_scanner_loop(self) -> None:
        """
        Background thread that periodically scans event logs for encrypted calls.

        Encrypted calls (like OPD) don't create audio recordings, so we need
        to extract their metadata directly from the event logs.
        """
        # Wait a bit before first scan to let the system stabilize
        for _ in range(5):
            if self._shutdown_event.is_set():
                return
            time.sleep(1)

        while not self._shutdown_event.is_set():
            try:
                # Determine scan window - only look at calls since last scan
                # Use local time (datetime.now()) since SDRTrunk logs use local time
                # On first run, look back 5 minutes
                if self._last_encrypted_scan is None:
                    since = datetime.now() - timedelta(minutes=5)
                else:
                    # Look back a bit further to catch any stragglers
                    since = self._last_encrypted_scan - timedelta(seconds=10)

                # Run the scan
                count = self.pipeline.scan_event_logs_for_encrypted(
                    since_timestamp=since
                )

                # Update last scan timestamp (use local time)
                self._last_encrypted_scan = datetime.now()

                if count > 0:
                    logger.debug(
                        "Encrypted calls processed",
                        count=count,
                        since=since.isoformat(),
                    )

            except Exception as e:
                logger.error("Error in encrypted call scanner", error=str(e))

            # Wait for next scan interval
            for _ in range(self.ENCRYPTED_SCAN_INTERVAL):
                if self._shutdown_event.is_set():
                    return
                time.sleep(1)

    def _print_stats(self) -> None:
        """Print processing statistics."""
        stats = self.pipeline.get_stats()
        log_kwargs = {
            "processed": stats.get("processed_count", 0),
            "transcribed": stats.get("transcribed_count", 0),
            "errors": stats.get("error_count", 0),
        }
        if "encrypted_count" in stats:
            log_kwargs["encrypted"] = stats["encrypted_count"]
        if "filtered_count" in stats:
            log_kwargs["filtered"] = stats["filtered_count"]
        if self._segments_dropped_count > 0:
            log_kwargs["dropped"] = self._segments_dropped_count
        logger.info("Processing statistics", **log_kwargs)


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Nodus Edge - Radio Ingestion for Public Safety Intelligence",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  nodus-edge                      Run daemon in P25 mode (default)
  nodus-edge --mode fm            Run daemon in FM ham radio mode
  nodus-edge --backfill           Process existing P25 files, then watch
  nodus-edge --once               Process existing P25 files and exit

Environment Variables:
  NODUS_EDGE_MODE                  Radio mode: p25 (default) or fm
  NODUS_EDGE_NODE_ID               Node identifier (default: hostname)
  NODUS_EDGE_RECORDINGS_DIR        Recordings directory (P25 mode)
  NODUS_EDGE_OUTPUT_DIR            Output directory for segments
  NODUS_EDGE_TRANSCRIPTION_ENABLED Enable Whisper transcription (default: true)
  NODUS_EDGE_WHISPER_API_URL       Whisper API URL

FM Mode Settings:
  NODUS_EDGE_FM_FREQUENCIES        Frequencies to scan (Hz), e.g., [146940000,147120000]
  NODUS_EDGE_FM_DWELL_SECONDS      Dwell time per frequency (default: 3.0)
  NODUS_EDGE_FM_SQUELCH_THRESHOLD  Squelch level 0-100 (default: 50)
        """,
    )

    parser.add_argument(
        "--mode",
        choices=["p25", "fm", "hf", "aprs"],
        default=None,
        help="Radio mode: p25, fm (ham radio), hf, or aprs (packet)",
    )
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Process existing files before watching for new ones (P25 mode only)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Process existing files and exit (P25 mode only)",
    )
    parser.add_argument(
        "--recordings-dir",
        type=str,
        help="Override recordings directory",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        help="Override output directory",
    )
    parser.add_argument(
        "--no-transcribe",
        action="store_true",
        help="Disable transcription",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default=None,
        help="Override log level",
    )

    args = parser.parse_args()

    # Apply overrides
    if args.mode:
        settings.mode = args.mode
    if args.recordings_dir:
        settings.recordings_dir = args.recordings_dir
    if args.output_dir:
        settings.output_dir = args.output_dir
    if args.no_transcribe:
        settings.transcription_enabled = False
    if args.log_level:
        settings.log_level = args.log_level

    configure_logging()

    # Set up signal handlers
    daemon = EdgeDaemon()

    def signal_handler(signum, frame):
        logger.info("Received shutdown signal")
        daemon.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        daemon.start(backfill=args.backfill, once=args.once)
    except Exception as e:
        logger.error("Daemon crashed", error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
