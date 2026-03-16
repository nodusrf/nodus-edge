"""
Trunk Recorder input schema types.

Defines the JSON structure that Trunk Recorder writes alongside each WAV recording.
Adapted for Nodus Edge.
"""

from typing import Optional, List
from pydantic import BaseModel, Field


class TRSourceEntry(BaseModel):
    """Entry in TR's srcList array."""
    src: int
    time: int
    pos: float
    emergency: Optional[int] = 0
    signal_system: Optional[str] = ""
    tag: Optional[str] = ""


class TRFreqEntry(BaseModel):
    """Entry in TR's freqList array."""
    freq: int
    time: int
    pos: float
    len: float
    error_count: int = 0
    spike_count: int = 0


class TRCallEvent(BaseModel):
    """Entry in TR's call_events array (NodusNet fork enhancement)."""
    timestamp: int
    message_type: int
    opcode: int
    source: int
    freq: int
    encrypted: int = 0
    emergency: int = 0
    priority: int = 0
    tdma_slot: int = 0


class TRCallJSON(BaseModel):
    """
    Trunk Recorder call JSON structure.

    This represents the JSON file TR writes alongside each WAV recording.
    """
    freq: int
    freq_error: Optional[int] = 0
    signal: Optional[int] = None
    noise: Optional[int] = None
    source_num: Optional[int] = None
    recorder_num: Optional[int] = None
    tdma_slot: int = 0
    phase2_tdma: int = 0
    start_time: int
    stop_time: int
    emergency: int = 0
    priority: int = 0
    mode: int = 0
    duplex: int = 0
    encrypted: int = 0
    call_length: int = 0
    talkgroup: int
    talkgroup_tag: Optional[str] = ""
    talkgroup_description: Optional[str] = ""
    talkgroup_group_tag: Optional[str] = ""
    talkgroup_group: Optional[str] = ""
    audio_type: Optional[str] = "digital"
    short_name: Optional[str] = ""

    # Enhanced fields (from nodus fork)
    message_type: Optional[int] = None
    opcode: Optional[int] = None
    call_events: List[TRCallEvent] = Field(default_factory=list)

    # Standard TR arrays
    freqList: List[TRFreqEntry] = Field(default_factory=list)
    srcList: List[TRSourceEntry] = Field(default_factory=list)
    patched_talkgroups: List[int] = Field(default_factory=list)

    class Config:
        extra = "ignore"


# MessageType enum values (from TR parser.h)
MESSAGE_TYPES = {
    0: "GRANT",
    1: "STATUS",
    2: "UPDATE",
    3: "CONTROL_CHANNEL",
    4: "REGISTRATION",
    5: "DEREGISTRATION",
    6: "AFFILIATION",
    7: "SYSID",
    8: "ACKNOWLEDGE",
    9: "LOCATION",
    10: "PATCH_ADD",
    11: "PATCH_DELETE",
    12: "DATA_GRANT",
    13: "UU_ANS_REQ",
    14: "UU_V_GRANT",
    15: "UU_V_UPDATE",
    16: "INVALID_CC_MESSAGE",
    17: "TDULC",
    99: "UNKNOWN",
}


def message_type_to_string(msg_type: Optional[int]) -> Optional[str]:
    """Convert TR message_type int to string."""
    if msg_type is None:
        return None
    return MESSAGE_TYPES.get(msg_type, f"UNKNOWN_{msg_type}")
