"""
FM Ham Radio recording parser for Nodus Edge.

Parses FM recording filenames and extracts ham radio callsigns
from transcript text.
"""

import re
import wave
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List

import structlog

from ..schema import FMRFChannel, AudioMetadata

logger = structlog.get_logger(__name__)


class FMRecordingParser:
    """
    Parser for FM ham radio recordings.

    Extracts metadata from recording filenames and callsigns from transcripts.
    """

    # Pattern for FM recordings from fm_scanner
    # Format: YYYYMMDD_HHMMSS_FREQHz.wav
    FM_RECORDING_PATTERN = re.compile(
        r'^(\d{8})_(\d{6})_(\d+)Hz\.wav$'
    )

    # Callsign patterns for ham radio
    # W, K, N: optional second letter before digit
    # Examples: W1ABC, KD9XYZ, N0CALL, WA3DEF
    US_CALLSIGN_PATTERN = re.compile(
        r'\b([WKN][A-Z]?\d[A-Z]{1,3})\b',
        re.IGNORECASE
    )

    # A prefix: REQUIRES second letter (AA-AL are valid US amateur prefixes)
    # FCC does not issue bare-A prefix callsigns — prevents "AT9ON" false positives
    US_CALLSIGN_A_PREFIX = re.compile(
        r'\b(A[A-L]\d[A-Z]{1,3})\b',
        re.IGNORECASE
    )

    # Canadian callsigns: VE/VA/VO/VY + digit + 1-3 letters
    # Examples: VE3ABC, VA7XYZ
    CANADIAN_CALLSIGN_PATTERN = re.compile(
        r'\b(V[AEOY]\d[A-Z]{1,3})\b',
        re.IGNORECASE
    )

    # Mexican callsigns: XE/XF + digit + 1-3 letters
    MEXICAN_CALLSIGN_PATTERN = re.compile(
        r'\b(X[EF]\d[A-Z]{1,3})\b',
        re.IGNORECASE
    )

    # Phonetic alphabet patterns for spoken callsigns
    # e.g., "whiskey one alpha bravo charlie" -> W1ABC
    # Includes NATO, APCO/police, and common informal phonetics hams use
    PHONETIC_ALPHABET = {
        # NATO phonetic alphabet
        'alpha': 'A', 'alfa': 'A',
        'bravo': 'B',
        'charlie': 'C',
        'delta': 'D',
        'echo': 'E',
        'foxtrot': 'F',
        'golf': 'G',
        'hotel': 'H',
        'india': 'I',
        'juliet': 'J', 'juliett': 'J',
        'kilo': 'K',
        'lima': 'L',
        'mike': 'M',
        'november': 'N',
        'oscar': 'O',
        'papa': 'P',
        'quebec': 'Q',
        'romeo': 'R',
        'sierra': 'S',
        'tango': 'T',
        'uniform': 'U',
        'victor': 'V',
        'whiskey': 'W', 'whisky': 'W',
        'xray': 'X', 'x-ray': 'X',
        'yankee': 'Y',
        'zulu': 'Z',
        # APCO / police / informal phonetics hams commonly use
        'adam': 'A',
        'baker': 'B',
        'david': 'D',
        'edward': 'E',
        'frank': 'F',
        'george': 'G',
        'henry': 'H',
        'ida': 'I',
        'john': 'J',
        'king': 'K',
        'lincoln': 'L',
        'mary': 'M',
        'nancy': 'N',
        'ocean': 'O',
        'paul': 'P',
        'queen': 'Q',
        'robert': 'R',
        'sam': 'S',
        'tom': 'T',
        'union': 'U',
        'victoria': 'V',
        'william': 'W',
    }

    # Multi-letter phonetics: single words that expand to 2+ callsign letters
    # "katie zero" = KT0, not K0 — hams use "katie" for the letter pair KT
    MULTI_LETTER_PHONETICS = {
        'katie': 'KT', 'katy': 'KT',
    }

    # Words that can start a phonetically-spelled callsign (W, K, N, A prefixes)
    CALLSIGN_START_PHONETICS = frozenset({
        'whiskey', 'whisky', 'william',   # W
        'kilo', 'king',                   # K
        'katie', 'katy',                  # KT (multi-letter)
        'november', 'nancy',              # N
        'alpha', 'alfa', 'adam',          # A
    })

    # Pattern for Whisper-split callsigns where the digit is separated
    # e.g., "KD zero NMD", "KD-Zero NMD", "W one ABC", "K 0 BVC"
    SPLIT_CALLSIGN_PATTERN = re.compile(
        r'\b([WKNA][A-Z]?)\s*[-]?\s*'
        r'(zero|oh|one|two|three|four|five|six|seven|eight|niner?|nine|\d)\s+'
        r'([A-Z]{1,3})\b',
        re.IGNORECASE,
    )

    # Numbers spoken as words
    PHONETIC_NUMBERS = {
        'zero': '0', 'oh': '0',
        'one': '1',
        'two': '2',
        'three': '3',
        'four': '4',
        'five': '5',
        'six': '6',
        'seven': '7',
        'eight': '8',
        'niner': '9', 'nine': '9',
    }

    def parse_fm_recording(self, filepath: Path) -> Optional[Dict[str, Any]]:
        """
        Parse an FM recording filename.

        Returns metadata dict or None if not a valid FM recording filename.
        """
        match = self.FM_RECORDING_PATTERN.match(filepath.name)
        if not match:
            logger.debug("Filename does not match FM pattern", filename=filepath.name)
            return None

        date_str = match.group(1)
        time_str = match.group(2)
        frequency_hz = int(match.group(3))

        try:
            dt = datetime.strptime(f"{date_str}{time_str}", "%Y%m%d%H%M%S")
        except ValueError:
            logger.warning("Invalid timestamp in filename", path=str(filepath))
            return None

        # Get file stats
        try:
            stat = filepath.stat()
            file_size = stat.st_size
        except OSError:
            file_size = None

        # Try to get duration from WAV file
        duration_seconds = None
        sample_rate_hz = 16000  # Default
        try:
            with wave.open(str(filepath), 'rb') as wav:
                frames = wav.getnframes()
                sample_rate_hz = wav.getframerate()
                duration_seconds = frames / sample_rate_hz
        except Exception as e:
            logger.debug("Could not read WAV metadata", error=str(e))

        return {
            "filename": filepath.name,
            "filepath": str(filepath.absolute()),
            "timestamp": dt,
            "frequency_hz": frequency_hz,
            "file_size_bytes": file_size,
            "duration_seconds": duration_seconds,
            "sample_rate_hz": sample_rate_hz,
            "recording_type": "fm",
        }

    def extract_callsigns(self, text: str) -> List[str]:
        """
        Extract ham radio callsigns from transcript text.

        Handles both literal callsigns (W1ABC) and phonetic spelling
        (whiskey one alpha bravo charlie).

        Returns list of unique callsigns in uppercase.
        """
        if not text:
            return []

        callsigns = set()

        # Extract literal callsigns
        for pattern in [
            self.US_CALLSIGN_PATTERN,
            self.US_CALLSIGN_A_PREFIX,
            self.CANADIAN_CALLSIGN_PATTERN,
            self.MEXICAN_CALLSIGN_PATTERN,
        ]:
            matches = pattern.findall(text)
            for match in matches:
                callsign = match.upper()
                if self._is_valid_callsign(callsign):
                    callsigns.add(callsign)

        # Try to extract phonetically spelled callsigns
        phonetic_callsigns = self._extract_phonetic_callsigns(text)
        callsigns.update(phonetic_callsigns)

        # Try to extract Whisper-split callsigns (e.g., "KD zero NMD")
        split_callsigns = self._extract_split_callsigns(text)
        callsigns.update(split_callsigns)

        return sorted(list(callsigns))

    def _is_valid_callsign(self, callsign: str) -> bool:
        """
        Validate that a string looks like a real ham callsign.

        Filters out common false positives.
        """
        if len(callsign) < 4 or len(callsign) > 6:
            return False

        # Must start with valid prefix
        if callsign[0] not in 'WKNAXV':
            return False

        # Must contain at least one digit
        if not any(c.isdigit() for c in callsign):
            return False

        # A-prefix validation: FCC requires two-letter prefix AA-AL
        # Bare A + digit (e.g., A9XXX) is invalid, and AM-AZ are not amateur prefixes
        if callsign[0] == 'A' and len(callsign) >= 2 and callsign[1].isalpha():
            if callsign[1] > 'L':
                return False

        # Common false positives to filter
        false_positives = {
            'W1LL', 'K1LL', 'N0PE', 'N0NE', 'W0RD', 'K0RE',
            'W1TH', 'K1ND', 'W0RK', 'W1ND', 'K1NG',
        }
        if callsign in false_positives:
            return False

        return True

    def _extract_phonetic_callsigns(self, text: str) -> List[str]:
        """
        Extract callsigns spelled out phonetically.

        Examples:
            "whiskey one alpha bravo charlie" -> W1ABC
            "katie zero november whiskey juliet" -> KT0NWJ
        """
        callsigns = []
        text_lower = text.lower()
        words = text_lower.split()

        # Look for sequences that could be phonetic callsigns
        # Pattern: (prefix letters) (digit) (1-3 suffix letters)
        i = 0
        while i < len(words) - 2:
            if words[i] not in self.CALLSIGN_START_PHONETICS:
                i += 1
                continue

            callsign_parts = []

            # Build prefix: multi-letter phonetics (e.g., "katie" → KT)
            # or single-letter with optional second letter
            multi = self.MULTI_LETTER_PHONETICS.get(words[i])
            if multi:
                callsign_parts.extend(list(multi))
                j = i + 1
            else:
                first_letter = self.PHONETIC_ALPHABET.get(words[i])
                if not first_letter:
                    i += 1
                    continue
                callsign_parts.append(first_letter)
                j = i + 1
                # Optional second letter (for 2-letter prefixes like AA, WA)
                if j < len(words) and words[j] in self.PHONETIC_ALPHABET:
                    callsign_parts.append(self.PHONETIC_ALPHABET[words[j]])
                    j += 1

            # Required digit
            if j >= len(words):
                i += 1
                continue
            number = self.PHONETIC_NUMBERS.get(words[j])
            if number is None and words[j].isdigit():
                number = words[j]
            if not number:
                i += 1
                continue
            callsign_parts.append(number)
            j += 1

            # 1-3 suffix letters
            suffix_count = 0
            while j < len(words) and suffix_count < 3:
                letter = self.PHONETIC_ALPHABET.get(words[j])
                if letter:
                    callsign_parts.append(letter)
                    suffix_count += 1
                    j += 1
                else:
                    break

            if suffix_count >= 1:
                callsign = ''.join(callsign_parts)
                if self._is_valid_callsign(callsign):
                    callsigns.append(callsign)

            i += 1

        return callsigns

    def _extract_split_callsigns(self, text: str) -> List[str]:
        """
        Extract callsigns split by Whisper transcription.

        Whisper often separates callsign parts with spaces or hyphens:
        "KD zero NMD" -> KD0NMD, "KD-Zero NMD" -> KD0NMD,
        "W one ABC" -> W1ABC, "K 0 BVC" -> K0BVC
        """
        callsigns = []
        for match in self.SPLIT_CALLSIGN_PATTERN.finditer(text):
            prefix = match.group(1).upper()
            digit_word = match.group(2).lower()
            suffix = match.group(3).upper()

            # Convert digit word to number
            digit = self.PHONETIC_NUMBERS.get(digit_word)
            if digit is None and digit_word.isdigit():
                digit = digit_word
            if digit is None:
                continue

            callsign = f"{prefix}{digit}{suffix}"
            if self._is_valid_callsign(callsign):
                callsigns.append(callsign)

        return callsigns

    def build_fm_rf_channel(
        self,
        metadata: Dict[str, Any],
        signal_strength_db: Optional[float] = None,
        ctcss_tone_hz: Optional[float] = None,
    ) -> FMRFChannel:
        """Build FMRFChannel from parsed metadata."""
        return FMRFChannel(
            frequency_hz=metadata.get("frequency_hz", 0),
            signal_strength_db=signal_strength_db,
            ctcss_tone_hz=ctcss_tone_hz,
            bandwidth_khz=12.5,  # Standard narrowband FM
        )

    def build_audio_metadata(self, metadata: Dict[str, Any]) -> AudioMetadata:
        """Build AudioMetadata from parsed metadata."""
        return AudioMetadata(
            filename=metadata.get("filename", "unknown"),
            filepath=metadata.get("filepath"),
            duration_seconds=metadata.get("duration_seconds"),
            file_size_bytes=metadata.get("file_size_bytes"),
            sample_rate_hz=metadata.get("sample_rate_hz", 16000),
            format="wav",
        )

    def format_frequency(self, frequency_hz: int) -> str:
        """Format frequency in human-readable form (e.g., 146.520 MHz)."""
        freq_mhz = frequency_hz / 1_000_000
        return f"{freq_mhz:.3f} MHz"
