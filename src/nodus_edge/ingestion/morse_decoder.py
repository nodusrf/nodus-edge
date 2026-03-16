"""
Morse Code Detector and Decoder for FM audio segments.

Detects CW (Continuous Wave) Morse code tones in 16 kHz mono WAV files
and decodes them to text. Designed for ham repeater CW identification
beacons (FCC-required every 10 minutes).

Algorithm:
1. Goertzel scan 400-1200 Hz to find dominant CW tone
2. Bandpass filter around detected tone
3. Envelope extraction → binary on/off signal
4. Adaptive timing analysis → dit/dah classification
5. ITU Morse tree lookup → text
"""

import math
import struct
import wave
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple

import structlog

logger = structlog.get_logger(__name__)

# ITU Morse code tree — binary trie indexed by dit=0, dah=1
# Each entry: (character_at_this_node, left_child_dit, right_child_dah)
MORSE_TABLE = {
    ".-": "A",
    "-...": "B",
    "-.-.": "C",
    "-..": "D",
    ".": "E",
    "..-.": "F",
    "--.": "G",
    "....": "H",
    "..": "I",
    ".---": "J",
    "-.-": "K",
    ".-..": "L",
    "--": "M",
    "-.": "N",
    "---": "O",
    ".--.": "P",
    "--.-": "Q",
    ".-.": "R",
    "...": "S",
    "-": "T",
    "..-": "U",
    "...-": "V",
    ".--": "W",
    "-..-": "X",
    "-.--": "Y",
    "--..": "Z",
    "-----": "0",
    ".----": "1",
    "..---": "2",
    "...--": "3",
    "....-": "4",
    ".....": "5",
    "-....": "6",
    "--...": "7",
    "---..": "8",
    "----.": "9",
    ".-.-.-": ".",
    "--..--": ",",
    "..--..": "?",
    ".----.": "'",
    "-.-.--": "!",
    "-..-.": "/",
    "-.--.": "(",
    "-.--.-": ")",
    ".-...": "&",
    "---...": ":",
    "-.-.-.": ";",
    "-...-": "=",
    ".-.-.": "+",
    "-....-": "-",
    "..--.-": "_",
    ".-..-.": '"',
    "...-..-": "$",
    ".--.-.": "@",
}


@dataclass
class MorseResult:
    """Result of Morse code detection and decoding."""

    detected: bool
    text: str
    confidence: float  # 0-1
    tone_frequency_hz: float
    wpm: float  # estimated words per minute


_EMPTY_RESULT = MorseResult(
    detected=False, text="", confidence=0.0, tone_frequency_hz=0.0, wpm=0.0
)


def detect_and_decode_morse(
    wav_path: Path,
    min_snr_db: float = 10.0,
    tone_low_hz: int = 400,
    tone_high_hz: int = 1200,
) -> MorseResult:
    """
    Detect and decode Morse code from a 16 kHz mono WAV file.

    Args:
        wav_path: Path to WAV file (16 kHz, mono, 16-bit PCM)
        min_snr_db: Minimum tone SNR to consider Morse present
        tone_low_hz: Low end of CW tone search range
        tone_high_hz: High end of CW tone search range

    Returns:
        MorseResult with detection status and decoded text
    """
    try:
        samples, sample_rate = _read_wav(wav_path)
    except Exception as e:
        logger.debug("Morse: failed to read WAV", error=str(e), path=wav_path.name)
        return _EMPTY_RESULT

    if len(samples) < sample_rate * 0.3:
        # Less than 300ms — too short for any meaningful Morse
        return _EMPTY_RESULT

    # Step 1: Find dominant tone via Goertzel scan
    tone_freq, tone_snr = _find_dominant_tone(
        samples, sample_rate, tone_low_hz, tone_high_hz, step_hz=25
    )

    if tone_snr < min_snr_db:
        return _EMPTY_RESULT

    # Step 2: Bandpass filter around detected tone
    filtered = _bandpass_filter(samples, sample_rate, tone_freq, bandwidth_hz=100)

    # Step 3: Envelope extraction
    envelope = _extract_envelope(filtered, sample_rate, cutoff_hz=30)

    # Step 4: Binary thresholding
    on_off = _threshold_envelope(envelope)

    # Step 5: Extract timing events
    events = _extract_timing_events(on_off, sample_rate)
    if not events:
        return _EMPTY_RESULT

    # Step 6: Classify dit/dah and decode
    text, unit_time = _decode_events(events)
    if not text or not text.strip():
        return _EMPTY_RESULT

    text = text.strip()

    # Estimate WPM: "PARIS" = 50 units, so WPM = 60 / (50 * unit_time)
    wpm = 60.0 / (50.0 * unit_time) if unit_time > 0 else 0.0

    # Confidence based on SNR (higher SNR = cleaner decode)
    confidence = min(1.0, (tone_snr - min_snr_db) / 20.0)
    # Penalize very short decoded text (likely noise)
    if len(text) < 3:
        confidence *= 0.5

    logger.info(
        "Morse decoded",
        text=text,
        tone_hz=round(tone_freq, 1),
        snr_db=round(tone_snr, 1),
        wpm=round(wpm, 1),
        confidence=round(confidence, 2),
        path=wav_path.name,
    )

    return MorseResult(
        detected=True,
        text=text,
        confidence=confidence,
        tone_frequency_hz=tone_freq,
        wpm=wpm,
    )


def _read_wav(wav_path: Path) -> Tuple[List[float], int]:
    """Read WAV file and return normalized float samples [-1, 1] and sample rate."""
    with wave.open(str(wav_path), "rb") as wf:
        n_channels = wf.getnchannels()
        sample_width = wf.getsampwidth()
        sample_rate = wf.getframerate()
        n_frames = wf.getnframes()

        raw = wf.readframes(n_frames)

    if sample_width == 2:
        fmt = f"<{n_frames * n_channels}h"
        int_samples = struct.unpack(fmt, raw)
        scale = 32768.0
    elif sample_width == 1:
        int_samples = [b - 128 for b in raw]
        scale = 128.0
    else:
        raise ValueError(f"Unsupported sample width: {sample_width}")

    # Take first channel if stereo
    if n_channels > 1:
        int_samples = int_samples[::n_channels]

    samples = [s / scale for s in int_samples]
    return samples, sample_rate


def _goertzel_power(samples: List[float], sample_rate: int, target_freq: float) -> float:
    """
    Compute power at a specific frequency using Goertzel algorithm.
    More efficient than FFT when checking a small number of frequencies.
    """
    n = len(samples)
    k = round(target_freq * n / sample_rate)
    w = 2.0 * math.pi * k / n
    coeff = 2.0 * math.cos(w)

    s0 = 0.0
    s1 = 0.0
    s2 = 0.0

    for sample in samples:
        s0 = sample + coeff * s1 - s2
        s2 = s1
        s1 = s0

    power = s1 * s1 + s2 * s2 - coeff * s1 * s2
    return power / (n * n)


def _find_dominant_tone(
    samples: List[float],
    sample_rate: int,
    low_hz: int,
    high_hz: int,
    step_hz: int = 25,
) -> Tuple[float, float]:
    """
    Scan frequency range to find the dominant tone and its SNR.
    Returns (frequency_hz, snr_db).
    """
    # Use a window of the audio (first 2 seconds or full if shorter)
    window_samples = min(len(samples), sample_rate * 2)
    window = samples[:window_samples]

    freqs = list(range(low_hz, high_hz + 1, step_hz))
    powers = []

    for freq in freqs:
        p = _goertzel_power(window, sample_rate, float(freq))
        powers.append(p)

    if not powers:
        return 0.0, 0.0

    # Find peak
    max_idx = 0
    max_power = powers[0]
    for i, p in enumerate(powers):
        if p > max_power:
            max_power = p
            max_idx = i

    if max_power <= 0:
        return 0.0, 0.0

    # Estimate noise floor as median power (excluding bins near peak)
    peak_freq = freqs[max_idx]
    noise_powers = []
    for i, p in enumerate(powers):
        if abs(freqs[i] - peak_freq) > 100:  # Exclude ±100 Hz around peak
            noise_powers.append(p)

    if not noise_powers:
        # All bins are near the peak — can't estimate noise
        return float(peak_freq), 20.0  # Assume decent SNR

    noise_powers.sort()
    median_noise = noise_powers[len(noise_powers) // 2]

    if median_noise <= 0:
        snr_db = 40.0  # Very clean signal
    else:
        snr_db = 10.0 * math.log10(max_power / median_noise)

    # Refine peak frequency with quadratic interpolation
    if 0 < max_idx < len(powers) - 1:
        alpha = powers[max_idx - 1]
        beta = powers[max_idx]
        gamma = powers[max_idx + 1]
        denom = alpha - 2.0 * beta + gamma
        if abs(denom) > 1e-10:
            correction = 0.5 * (alpha - gamma) / denom
            refined_freq = freqs[max_idx] + correction * step_hz
            return refined_freq, snr_db

    return float(peak_freq), snr_db


def _bandpass_filter(
    samples: List[float],
    sample_rate: int,
    center_freq: float,
    bandwidth_hz: float = 100,
) -> List[float]:
    """
    Simple 2nd-order IIR bandpass filter (biquad).
    """
    w0 = 2.0 * math.pi * center_freq / sample_rate
    q = center_freq / bandwidth_hz
    alpha = math.sin(w0) / (2.0 * q)

    b0 = alpha
    b1 = 0.0
    b2 = -alpha
    a0 = 1.0 + alpha
    a1 = -2.0 * math.cos(w0)
    a2 = 1.0 - alpha

    # Normalize
    b0 /= a0
    b1 /= a0
    b2 /= a0
    a1 /= a0
    a2 /= a0

    out = [0.0] * len(samples)
    x1 = x2 = y1 = y2 = 0.0

    for i, x0 in enumerate(samples):
        y0 = b0 * x0 + b1 * x1 + b2 * x2 - a1 * y1 - a2 * y2
        out[i] = y0
        x2 = x1
        x1 = x0
        y2 = y1
        y1 = y0

    return out


def _extract_envelope(
    samples: List[float],
    sample_rate: int,
    cutoff_hz: float = 30,
) -> List[float]:
    """
    Extract amplitude envelope: rectify + single-pole low-pass filter.
    """
    # Rectify
    rectified = [abs(s) for s in samples]

    # Single-pole low-pass (exponential moving average)
    rc = 1.0 / (2.0 * math.pi * cutoff_hz)
    dt = 1.0 / sample_rate
    alpha = dt / (rc + dt)

    envelope = [0.0] * len(rectified)
    envelope[0] = rectified[0]
    for i in range(1, len(rectified)):
        envelope[i] = alpha * rectified[i] + (1.0 - alpha) * envelope[i - 1]

    return envelope


def _threshold_envelope(envelope: List[float]) -> List[int]:
    """
    Convert envelope to binary on/off using adaptive threshold.
    Threshold = midpoint between mean of top 20% and mean of bottom 50%.
    """
    if not envelope:
        return []

    sorted_env = sorted(envelope)
    n = len(sorted_env)

    # Bottom 50% = noise floor estimate
    bottom_end = n // 2
    noise_mean = sum(sorted_env[:bottom_end]) / max(bottom_end, 1)

    # Top 20% = signal level estimate
    top_start = int(n * 0.8)
    top_vals = sorted_env[top_start:]
    signal_mean = sum(top_vals) / max(len(top_vals), 1)

    if signal_mean <= noise_mean * 1.5:
        # No clear signal/noise separation
        return [0] * n

    threshold = (noise_mean + signal_mean) / 2.0

    return [1 if s > threshold else 0 for s in envelope]


@dataclass
class _TimingEvent:
    """A single on or off period."""

    is_on: bool
    duration_seconds: float


def _extract_timing_events(
    on_off: List[int], sample_rate: int
) -> List[_TimingEvent]:
    """Extract timing events (runs of on/off) from binary signal."""
    if not on_off:
        return []

    events: List[_TimingEvent] = []
    current_state = on_off[0]
    run_start = 0

    for i in range(1, len(on_off)):
        if on_off[i] != current_state:
            duration = (i - run_start) / sample_rate
            # Filter out very short glitches (< 10ms)
            if duration >= 0.010:
                events.append(_TimingEvent(is_on=bool(current_state), duration_seconds=duration))
            current_state = on_off[i]
            run_start = i

    # Final run
    duration = (len(on_off) - run_start) / sample_rate
    if duration >= 0.010:
        events.append(_TimingEvent(is_on=bool(current_state), duration_seconds=duration))

    return events


def _estimate_unit_time(on_durations: List[float]) -> float:
    """
    Estimate Morse unit time from ON durations.

    Dits ≈ 1 unit, dahs ≈ 3 units. The shortest cluster of ON durations
    should be dits, giving us the unit time.

    Typical CW IDs are 15-25 WPM. At 20 WPM, unit = 60ms.
    """
    if not on_durations:
        return 0.06  # Default 20 WPM

    sorted_durs = sorted(on_durations)

    # Simple approach: find the gap between dit and dah clusters
    # If there's a clear bimodal distribution, the gap is between
    # the two clusters. Unit time = mean of shorter cluster.

    if len(sorted_durs) == 1:
        # Single element — could be dit or dah, assume dit
        return sorted_durs[0]

    # Look for the largest ratio gap between consecutive sorted durations
    max_ratio = 0.0
    split_idx = 0
    for i in range(len(sorted_durs) - 1):
        if sorted_durs[i] > 0:
            ratio = sorted_durs[i + 1] / sorted_durs[i]
            if ratio > max_ratio:
                max_ratio = ratio
                split_idx = i + 1

    if max_ratio >= 1.8:
        # Clear bimodal split — short cluster = dits
        dit_cluster = sorted_durs[:split_idx]
        unit_time = sum(dit_cluster) / len(dit_cluster)
    else:
        # No clear split — all similar durations, assume all dits
        unit_time = sum(sorted_durs) / len(sorted_durs)

    # Sanity clamp: 5 WPM (240ms unit) to 40 WPM (30ms unit)
    unit_time = max(0.030, min(0.240, unit_time))

    return unit_time


def _decode_events(events: List[_TimingEvent]) -> Tuple[str, float]:
    """
    Decode timing events into text using Morse code lookup.

    Returns (decoded_text, unit_time_seconds).
    """
    # Collect ON durations for unit time estimation
    on_durations = [e.duration_seconds for e in events if e.is_on]
    if not on_durations:
        return "", 0.0

    unit_time = _estimate_unit_time(on_durations)

    # Classify each event
    morse_chars: List[str] = []
    current_char = ""

    for event in events:
        if event.is_on:
            # dit or dah
            if event.duration_seconds < unit_time * 2.0:
                current_char += "."
            else:
                current_char += "-"
        else:
            # Gap classification
            gap_units = event.duration_seconds / unit_time
            if gap_units >= 5.0:
                # Word space
                if current_char:
                    char = MORSE_TABLE.get(current_char, "")
                    if char:
                        morse_chars.append(char)
                    current_char = ""
                morse_chars.append(" ")
            elif gap_units >= 2.0:
                # Character space
                if current_char:
                    char = MORSE_TABLE.get(current_char, "")
                    if char:
                        morse_chars.append(char)
                    current_char = ""
            # else: element space (within character), do nothing

    # Flush last character
    if current_char:
        char = MORSE_TABLE.get(current_char, "")
        if char:
            morse_chars.append(char)

    text = "".join(morse_chars)

    # Clean up multiple spaces
    while "  " in text:
        text = text.replace("  ", " ")

    return text, unit_time
