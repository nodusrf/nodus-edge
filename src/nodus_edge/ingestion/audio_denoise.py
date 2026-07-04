"""
Audio denoising for radio recordings.

Applies spectral gating to reduce static and broadband noise
before Whisper transcription. Uses the noisereduce library which
estimates the noise profile from the signal itself (no reference
recording needed).

Runs after RMS normalization, before Whisper submission.
"""

import array
import io
import wave
from typing import Optional

import numpy as np
import structlog

logger = structlog.get_logger(__name__)

# Lazy import — noisereduce pulls in scipy which is heavy.
# Fail at call time, not import time, so the flag can stay off
# on nodes that don't have it installed.
_noisereduce = None


def _get_noisereduce():
    global _noisereduce
    if _noisereduce is None:
        try:
            import noisereduce as nr
            _noisereduce = nr
        except ImportError:
            raise ImportError(
                "noisereduce is required for audio denoising. "
                "Install with: pip install noisereduce"
            )
    return _noisereduce


def denoise_audio(wav_bytes: bytes, prop_decrease: float = 0.75) -> bytes:
    """Denoise a 16-bit mono WAV using spectral gating.

    Args:
        wav_bytes: Raw WAV file bytes (16-bit PCM, mono expected).
        prop_decrease: How aggressively to reduce noise (0.0-1.0).
            0.75 = remove 75% of estimated noise energy.
            Lower values preserve more of the original signal.

    Returns:
        Denoised WAV file bytes, same format as input.
        Returns original bytes unchanged on error.
    """
    nr = _get_noisereduce()

    try:
        with io.BytesIO(wav_bytes) as inp:
            with wave.open(inp, "rb") as wav_in:
                params = wav_in.getparams()
                raw = wav_in.readframes(params.nframes)
                sample_rate = params.framerate

        samples = array.array("h")
        samples.frombytes(raw)

        if len(samples) < sample_rate // 2:
            # Too short to estimate noise profile reliably
            return wav_bytes

        # Convert to float32 [-1, 1] for noisereduce
        audio = np.array(samples, dtype=np.float32) / 32768.0

        # Stationary noise reduction — good for radio hiss/static
        denoised = nr.reduce_noise(
            y=audio,
            sr=sample_rate,
            stationary=True,
            prop_decrease=prop_decrease,
        )

        # Convert back to 16-bit PCM
        denoised_int = np.clip(denoised * 32768.0, -32768, 32767).astype(np.int16)
        output_samples = array.array("h")
        output_samples.frombytes(denoised_int.tobytes())

        output = io.BytesIO()
        with wave.open(output, "wb") as wav_out:
            wav_out.setparams(params)
            wav_out.writeframes(output_samples.tobytes())

        return output.getvalue()

    except Exception as e:
        logger.warning("Audio denoise failed, using original", error=str(e))
        return wav_bytes
