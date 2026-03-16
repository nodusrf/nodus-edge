"""
Transcription quality gate for radio audio.

Replaces the reactive phrase-list approach with a principled quality score
derived from Whisper's own uncertainty signals. The key insight: Whisper
already knows when it's guessing — we just need to listen.

Three-component quality score:
- speech_confidence: Does Whisper think it heard speech? (1 - no_speech_prob)
- decode_confidence: Is Whisper confident in what it decoded? (exp(avg_logprob))
- text_quality: Is the decoded text structurally sound? (2 / compression_ratio)

Geometric mean ensures ALL three must be reasonable — if any signal collapses,
the score collapses.

Structural checks remain for deterministic impossibilities (empty text, CJK
characters, YouTube training-data artifacts).
"""

import re
from collections import Counter
from typing import Optional, Set, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from .schema import Transcription

from .config import settings


# --- Structural check patterns (deterministic, not heuristic) ---

# Pure punctuation / whitespace
_JUNK_PATTERN = re.compile(r"^[\s\W]+$")

# Non-Latin scripts that never appear in English radio comms
_FOREIGN_PATTERN = re.compile(
    r"[\u4e00-\u9fff"     # CJK Unified Ideographs
    r"\u3040-\u309f"      # Hiragana
    r"\u30a0-\u30ff"      # Katakana
    r"\uac00-\ud7af"      # Hangul
    r"\u0600-\u06ff]"     # Arabic
)

# Stable markers from Whisper's training data (YouTube signoffs).
# These are structural artifacts — Whisper's decoder generates them
# from specific internal states, not from audio content.
_FABRICATED_MARKERS = [
    "subscribe for more",
    "see you in the next one",
    "thanks for watching",
    "google chrome team",
    "the recording is now finished",
    "we shall be back with the second part",
    "dont forget to like",
    "hit that subscribe button",
    "closed captioning",
    "welcome back to my channel",
    "its about blast",
    "subtitles by the amara",
    "translated by",
    "captions by",
]


# --- Quality score ---

def compute_quality_score(
    max_no_speech_prob: Optional[float],
    min_confidence: Optional[float],
    max_compression_ratio: Optional[float],
) -> float:
    """
    Unified quality score derived from Whisper's own signals.

    Three components, each 0-1:
    - speech_confidence: Does Whisper think it heard speech?
    - decode_confidence: Is Whisper confident in what it decoded?
    - text_quality: Is the decoded text structurally sound?

    Returns: geometric mean — all three must be reasonable.
    """
    # 1. Speech confidence: inverse of no_speech_prob
    #    High no_speech_prob → Whisper thinks it's noise
    speech = 1.0 - (max_no_speech_prob if max_no_speech_prob is not None else 0.0)

    # 2. Decode confidence: from avg_logprob → exp()
    #    Already 0-1 via exp(avg_logprob) in whisper_client
    decode = min_confidence if min_confidence is not None else 0.0

    # 3. Text quality: inverse of compression ratio
    #    High compression = repetitive text = hallucination
    #    Typical real speech: 1.0-2.0. Hallucination: 2.5+
    cr = max_compression_ratio if max_compression_ratio is not None else 1.0
    text_q = min(1.0, 2.0 / max(cr, 0.1))

    # Geometric mean: if ANY signal is near zero, score collapses
    product = speech * decode * text_q
    if product <= 0:
        return 0.0
    return product ** (1.0 / 3.0)


# --- Structural checks ---

_CALLSIGN_RE = re.compile(r'\b[AKNW][A-Z]?\d[A-Z]{1,3}\b')

# Speech hesitation/filler words that inflate repetition scores in natural
# conversation. Excluded from word-level diversity calculation so "uh, uh, uh"
# in real speech doesn't tank the score, while "K0R, K0R, K0R" loops still fail.
# Only true fillers — NOT function words (the, is, and, etc.) which carry meaning.
_FILLER_WORDS = frozenset({
    "uh", "um", "ah", "oh", "er", "hm", "hmm", "uhh", "umm",
    "yeah", "yes", "no", "so", "well", "okay", "ok",
})


def _check_structural(
    text: str,
    initial_prompt: Optional[str] = None,
    duration_seconds: Optional[float] = None,
) -> Optional[str]:
    """Structural checks — things that are impossible in English radio."""
    if not text:
        return "empty"

    stripped = text.strip()
    if not stripped:
        return "empty"

    # Normalize for comparison
    cleaned = (stripped.rstrip(".!?,;:").strip().lower()
               .replace("\u2019", "").replace("\u2018", "")
               .replace("'", "").replace(",", ""))

    if len(cleaned) <= 1:
        return "too_short"

    if _JUNK_PATTERN.match(stripped):
        return "junk_chars"

    if _FOREIGN_PATTERN.search(stripped):
        return "foreign_chars"

    # Fabricated content markers (stable YouTube training-data artifacts)
    if len(cleaned) > 30:
        for marker in _FABRICATED_MARKERS:
            if marker in cleaned:
                return "fabricated_content"

    # Prompt-echo detection: Whisper echoing the initial_prompt callsigns
    # with no real speech content. Structurally impossible in real radio —
    # real conversations always contain words beyond callsigns.
    if initial_prompt:
        prompt_callsigns = set(_CALLSIGN_RE.findall(initial_prompt.upper()))
        text_callsigns = set(_CALLSIGN_RE.findall(stripped.upper()))

        if text_callsigns and text_callsigns.issubset(prompt_callsigns):
            # All callsigns in the text came from the prompt.
            # Check if there's any real speech left after removing callsigns.
            residual = _CALLSIGN_RE.sub('', stripped)
            residual_words = [w for w in re.findall(r'[a-zA-Z]{2,}', residual)]
            if len(residual_words) == 0:
                return "prompt_echo"

        # Full prompt text echo: Whisper regurgitating the initial_prompt verbatim.
        # If >=80% of text words appear in the prompt, it's an echo.
        prompt_words = set(re.findall(r'[a-zA-Z]{2,}', initial_prompt.lower()))
        text_words = re.findall(r'[a-zA-Z]{2,}', stripped.lower())
        if len(text_words) >= 3:
            from_prompt = sum(1 for w in text_words if w in prompt_words)
            if from_prompt / len(text_words) >= 0.8:
                return "prompt_echo"

    # Sentence repetition: same sentence/clause repeated 3+ times.
    # Catches "Beep. Beep. Beep." and similar structural impossibilities.
    sentences = [s.strip().rstrip(".!?,;:").strip().lower()
                 for s in re.split(r"[.!?]+", stripped) if s.strip()]
    if len(sentences) >= 3:
        sentence_counts = Counter(sentences)
        _, top_count = sentence_counts.most_common(1)[0]
        if top_count >= 3:
            return "sentence_repetition"

    # Repetition detection — hallucinated text loops on 1-3 tokens.
    # This is structurally impossible in real radio speech.
    # Must be in structural checks so it runs regardless of quality gate mode.

    # Comma-separated repetition: "K0Z, K0Z, K0Z" or "K0TV, NO1HAS, K0TV, NO1HAS"
    # Threshold: >=3 tokens with <=2 unique (was >=4)
    tokens = [t.strip().lower().rstrip(".!?,;:") for t in stripped.split(",") if t.strip()]
    if len(tokens) >= 3:
        unique_tokens = set(tokens)
        if len(unique_tokens) <= max(2, len(tokens) * 0.35):
            return "repetition"

    # Trailing repetition: unique prefix then same token repeated.
    # Catches "W7MSL, AT8P, K0Z, K0Z, K0Z, K0Z, K0Z" patterns.
    if len(tokens) >= 6:
        tail_start = max(len(tokens) // 3, 2)
        tail = tokens[tail_start:]
        if len(tail) >= 4 and len(set(tail)) <= 2:
            return "repetition"

    # Space-separated repetition: catches patterns not using commas.
    # Filter out common filler words so natural speech hesitation
    # ("uh, uh, uh") doesn't tank the diversity score.
    words = cleaned.split()
    if len(words) >= 6:
        content_words = [w for w in words if w not in _FILLER_WORDS]
        # Use content words if enough exist; otherwise fall back to all words
        # (prevents filler-only texts from slipping through)
        check_words = content_words if len(content_words) >= 4 else words

        word_counts = Counter(check_words)
        _, top_count = word_counts.most_common(1)[0]
        # Single word dominates (>70%)
        if top_count >= len(check_words) * 0.7:
            return "repetition"
        # Very low diversity (< 25% unique)
        if len(set(check_words)) / len(check_words) < 0.25:
            return "repetition"

    # Content density: too many callsigns per second of audio.
    # Real conversations have ~0.5 callsigns/sec max; hallucinations pack many more.
    if duration_seconds is not None and duration_seconds > 0:
        callsigns = _CALLSIGN_RE.findall(stripped.upper())
        if len(callsigns) >= 3:
            if len(callsigns) * 1.0 > duration_seconds * 2.0:
                return "content_density"

    return None


# --- Tail loop truncation ---

def truncate_tail_loop(text: str) -> Tuple[str, bool]:
    """Detect and truncate tail repetition loops in transcription text.

    Whisper sometimes starts with valid speech then enters a loop,
    repeating a phrase dozens of times. This preserves the valid prefix
    and one instance of the repeated phrase, discarding the loop.

    Handles two patterns:
    1. Sentence-level loops: "Good speech. Beep. Beep. Beep. Beep."
    2. Comma-separated loops: "Real speech, W0F, W0F, W0F, W0F"

    Returns (truncated_text, was_truncated).
    """
    if not text or len(text) < 20:
        return text, False

    result = text
    was_truncated = False

    # Pass 1: Sentence-level loops (split by .!?)
    result, truncated_1 = _truncate_sentence_loop(result)
    was_truncated = was_truncated or truncated_1

    # Pass 2: Comma-separated loops
    result, truncated_2 = _truncate_comma_loop(result)
    was_truncated = was_truncated or truncated_2

    # Pass 3: Space-separated word loops (e.g., "W-0-W-Y-V W-0-W-Y-V W-0-W-Y-V")
    result, truncated_3 = _truncate_space_loop(result)
    was_truncated = was_truncated or truncated_3

    return result, was_truncated


def _truncate_sentence_loop(text: str) -> Tuple[str, bool]:
    """Truncate sentence-level tail loops (split by .!?)."""
    parts = re.split(r'(?<=[.!?])\s+', text.strip())
    if len(parts) < 4:
        return text, False

    normalized = [p.strip().rstrip(".!?,;:").strip().lower() for p in parts]

    run_start = None
    run_count = 1
    for i in range(1, len(normalized)):
        if normalized[i] == normalized[i - 1] and normalized[i]:
            run_count += 1
            if run_count >= 3 and run_start is None:
                run_start = i - run_count + 1
                break
        else:
            run_count = 1

    if run_start is None:
        return text, False

    kept = parts[:run_start + 1]
    return " ".join(kept), True


def _truncate_comma_loop(text: str) -> Tuple[str, bool]:
    """Truncate comma-separated tail loops.

    Catches patterns like "Real speech, W0F, W0F, W0F, W0F" where
    Whisper starts with valid content then loops on a comma-separated token.
    """
    tokens = [t.strip() for t in text.split(",") if t.strip()]
    if len(tokens) < 4:
        return text, False

    normalized = [t.rstrip(".!?,;:").strip().lower() for t in tokens]

    run_start = None
    run_count = 1
    for i in range(1, len(normalized)):
        if normalized[i] == normalized[i - 1] and normalized[i]:
            run_count += 1
            if run_count >= 3 and run_start is None:
                run_start = i - run_count + 1
                break
        else:
            run_count = 1

    if run_start is None:
        return text, False

    # Keep everything up to and including the first instance of the repeat
    kept = tokens[:run_start + 1]
    return ", ".join(kept), True


def _truncate_space_loop(text: str) -> Tuple[str, bool]:
    """Truncate space-separated tail loops.

    Catches patterns like "valid speech KX-0U W-0-W-Y-V Repeater W-0-W-Y-V
    W-0-W-Y-V W-0-W-Y-V" where Whisper loops on a space-separated token
    (typically a callsign from CW ID bleed).
    """
    words = text.split()
    if len(words) < 4:
        return text, False

    normalized = [w.rstrip(".!?,;:").lower() for w in words]

    # Walk backwards counting consecutive identical tokens
    tail_tok = normalized[-1]
    if not tail_tok:
        return text, False

    trail = 0
    for i in range(len(normalized) - 1, -1, -1):
        if normalized[i] == tail_tok:
            trail += 1
        else:
            break

    if trail < 3:
        return text, False

    # Keep everything up to and including the first instance of the repeat
    run_start = len(words) - trail
    kept = words[:run_start + 1]
    return " ".join(kept), True


# --- Public API ---

def evaluate_transcription(
    transcription: "Transcription",
    initial_prompt: Optional[str] = None,
) -> Tuple[bool, float, str]:
    """
    Evaluate transcription quality using Whisper's own signals.

    When quality_gate_primary is True, uses the quality score as the
    primary filter. Otherwise falls back to the legacy hallucination check.

    In shadow mode (quality_gate_enabled=True, quality_gate_primary=False),
    computes the quality score for audit logging but doesn't use it for
    filtering decisions.

    Args:
        transcription: The transcription to evaluate.
        initial_prompt: The Whisper initial_prompt used for this transcription,
            if any. Used for prompt-echo detection.

    Returns:
        (passes: bool, quality_score: float, reason: str)
    """
    text = transcription.text if transcription.text else ""

    # Gate A: Structural impossibility (deterministic)
    structural_result = _check_structural(
        text,
        initial_prompt=initial_prompt,
        duration_seconds=transcription.duration_seconds,
    )
    if structural_result:
        return False, 0.0, structural_result

    # Gate B: Hard confidence floor — Whisper's own uncertainty signal.
    # FM audio normalization makes no_speech_prob useless (~10^-11),
    # but min_confidence cleanly separates noise (max 0.391) from speech (min 0.509).
    if transcription.min_confidence is not None:
        if transcription.min_confidence < settings.fm_min_confidence:
            return False, 0.0, f"low_confidence ({transcription.min_confidence:.3f})"

    # Compute quality score (always, for audit logging)
    score = compute_quality_score(
        max_no_speech_prob=transcription.max_no_speech_prob,
        min_confidence=transcription.min_confidence,
        max_compression_ratio=transcription.max_compression_ratio,
    )

    if settings.quality_gate_primary:
        # Gate B: Quality score (model's own uncertainty)
        if score < settings.quality_score_threshold:
            return False, score, f"low_quality_score ({score:.3f})"
        return True, score, ""

    # Legacy mode: use the old hallucination check
    is_hallucination, reason = _legacy_hallucination_check(text)
    if is_hallucination:
        return False, score, reason

    return True, score, ""


# --- Legacy hallucination filter (kept for shadow mode comparison) ---

# Known Whisper hallucination phrases — phantom text generated from silence/noise.
WHISPER_HALLUCINATIONS: Set[str] = {
    # YouTube / podcast signoffs
    "thanks for watching",
    "thank you for watching",
    "subscribe to my channel",
    "please subscribe",
    "like and subscribe",
    "please like and subscribe",
    "dont forget to subscribe",
    "hit the bell icon",
    "leave a comment below",
    "thanks for watching guys",
    "thats all for today",
    "thank you for listening",
    "thanks for listening",
    "see you next time",
    "see you in the next video",
    "peace out",
    # Subtitle / caption artifacts
    "subtitles by the amara.org community",
    "amara.org",
    "translated by",
    "subtitles by",
    "captions by",
    # Generic filler
    "goodbye",
    "bye bye",
    "bye",
    "thank you",
    "thanks",
    "you",
    "the end",
    "so",
    "ugh",
    "hmm",
    "oh",
    "ah",
    "okay",
    "ok",
    "yeah",
    "yes",
    "no",
    "what",
    "huh",
    "right",
    "im sorry",
    "sorry",
    # Repetitive filler Whisper generates on carrier noise
    "i dont know",
    "i dont know what to do",
    "i dont know how to do this",
    "i dont know what to say",
    "i dont know what youre talking about",
    "ill see you next time",
    "and ill see you next time",
    "ill be right back",
    "ill be back",
    "ill see you later",
    "ill see you soon",
    "were going to be right back",
    "were going to take a break",
    "lets get started",
    "hello",
    "hello everyone",
    "hey guys",
    "hi everyone",
    "good morning",
    "good evening",
    "good night",
    "good afternoon",
    # Broadcast / filler
    "well be right back",
    "well be back",
    "stay tuned",
    # Misinterpreted audio artifacts
    "silence",
    "music",
    "applause",
    "laughter",
    "birds chirp",
    "birds",
    # CW / courtesy tone artifacts
    "beep",
    "boop",
    "beep beep",
    "boop boop",
    # Additional signoffs / greetings Whisper fabricates
    "well see you next time",
    "well see you in the next video",
    "well see you in the next one",
    "welcome back to my channel",
    "hello everyone welcome back to my channel",
    "hello everybody",
    "hello everyone welcome back",
    "welcome back",
    "youre welcome",
    "im welcome",
    # Repetitive filler variants
    "its about blast",
    "you can use blast",
}


def _legacy_hallucination_check(text: str) -> Tuple[bool, str]:
    """
    Legacy hallucination detection via phrase matching.

    Kept for shadow mode comparison during quality gate transition.
    Will be removed once quality gate is proven in production.
    """
    stripped = text.strip()
    cleaned = (stripped.rstrip(".!?,;:").strip().lower()
               .replace("\u2019", "").replace("\u2018", "")
               .replace("'", "").replace(",", ""))

    # Exact match against known hallucination phrases
    if cleaned in WHISPER_HALLUCINATIONS:
        return True, "known_phrase"

    # Composite hallucination: every sentence/clause is a known phrase
    sentences = [s.strip().rstrip(".!?,;:").strip().lower()
                 .replace("\u2019", "").replace("\u2018", "")
                 .replace("'", "").replace(",", "")
                 for s in re.split(r"[.!?]+", stripped) if s.strip()]
    clauses = [re.sub(r"^(but|and|or|so|well|then)\s+", "", s.strip()
               .rstrip(".!?,;:").strip().lower()
               .replace("\u2019", "").replace("\u2018", "")
               .replace("'", "").replace(",", ""))
               for s in re.split(r"[.!?,;]+", stripped) if s.strip()]

    if len(sentences) >= 2 and all(s in WHISPER_HALLUCINATIONS for s in sentences):
        return True, "known_phrase_composite"
    if len(clauses) >= 2 and all(c in WHISPER_HALLUCINATIONS for c in clauses):
        return True, "known_phrase_composite"

    for fragments in (sentences, clauses):
        if len(fragments) >= 3:
            known_count = sum(1 for s in fragments if s in WHISPER_HALLUCINATIONS)
            if known_count >= len(fragments) * 0.8:
                return True, "known_phrase_composite"

    # Sentence/clause-level repetition
    for fragments in (sentences, clauses):
        if len(fragments) >= 3:
            fragment_counts = Counter(fragments)
            _, top_count = fragment_counts.most_common(1)[0]
            if top_count >= 3:
                return True, "sentence_repetition"

    # Tone words
    _TONE_WORDS = {"beep", "boop", "beeps", "boops", "ding", "dong", "bip", "bop"}
    words = cleaned.split()
    if len(words) >= 3:
        tone_count = sum(1 for w in words if w in _TONE_WORDS)
        if tone_count >= len(words) * 0.8:
            return True, "tone_pattern"

    # Word spam
    if len(words) >= 6:
        word_counts = Counter(words)
        _, most_common_count = word_counts.most_common(1)[0]
        if most_common_count >= len(words) * 0.8:
            return True, "word_spam"

    # Token diversity — catches repetitive patterns with few unique tokens
    # Real speech has diverse vocabulary; hallucinations loop on a few tokens
    if len(words) >= 8:
        unique_ratio = len(set(words)) / len(words)
        if unique_ratio < 0.25:
            return True, "low_diversity"

    # Stutter pattern
    if re.match(r"^[a-z](-[a-z]){3,}$", cleaned):
        return True, "stutter_pattern"

    # Prompt-echo detection
    _CALLSIGN_RE = re.compile(r'\b[AKNW][A-Z]?\d[A-Z]{0,3}\b')
    callsign_tokens = _CALLSIGN_RE.findall(stripped.upper())
    if len(callsign_tokens) >= 2:
        residual = _CALLSIGN_RE.sub('', stripped)
        _NOISE_FILLER = {'callsigns', 'callsign', 'birds', 'siren', 'sirens',
                         'bell', 'bells', 'rings', 'rip', 'knock', 'broadf',
                         'trekedabc', 'sirency'}
        residual_words = [w for w in re.findall(r'[a-zA-Z]{2,}', residual)
                          if w.lower() not in _NOISE_FILLER]
        total_words = len(words)
        if total_words > 0 and len(residual_words) / total_words <= 0.3:
            return True, "prompt_echo"

    return False, ""


def is_whisper_hallucination(text: str) -> Tuple[bool, str]:
    """
    Check if transcription text is a known Whisper hallucination.

    Legacy API — delegates to structural checks + legacy phrase matching.
    Kept for backward compatibility during quality gate transition.

    Returns:
        Tuple of (is_hallucination, reason).
    """
    # Structural checks first
    structural = _check_structural(text)
    if structural:
        return True, structural

    return _legacy_hallucination_check(text)
