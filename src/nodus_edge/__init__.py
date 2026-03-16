"""
Nodus Edge

Distributed radio ingestion for public safety intelligence.
Nodus Edge captures radio signals, transcribes audio, and outputs structured segments.
It does not interpret, alert, or decide.
"""

import os

__version__ = os.environ.get("NODUS_VERSION", "1.0.1")
