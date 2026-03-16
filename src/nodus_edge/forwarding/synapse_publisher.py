"""
Synapse publisher for Nodus Edge.

Sends TranscriptSegment.v1 to the Synapse service via HTTP POST.
"""

import httpx
import structlog

from ..config import settings
from ..schema import TranscriptSegmentV1, FMTranscriptSegmentV1, HFTranscriptSegmentV1, APRSPacketSegmentV1

logger = structlog.get_logger(__name__)


class SynapsePublisher:
    """
    Publisher that sends segments to Synapse via HTTP.

    Segments are POSTed to the Synapse /v1/segments endpoint.
    Local file emission continues regardless of Synapse availability.
    """

    def __init__(
        self,
        endpoint: str | None = None,
        timeout: float | None = None,
        auth_token: str | None = None,
    ):
        self.endpoint = endpoint or settings.synapse_endpoint
        self.timeout = timeout or settings.synapse_timeout_seconds
        self.auth_token = auth_token or settings.synapse_auth_token

        # Runtime pause flag (session-level, not persisted)
        self._paused = False

        # REM check-in reference (set by main.py after startup)
        self.rem_checkin = None

        # Statistics
        self._published_count = 0
        self._failure_count = 0

    def _auth_headers(self) -> dict:
        """Build auth headers for segment POST.

        Sends static auth token in Authorization (Gateway device auth) and
        compliance token in X-Compliance-Token (Synapse version gate).
        Both are needed when segments are proxied through Gateway.
        """
        headers: dict[str, str] = {}
        if self.auth_token:
            headers["Authorization"] = f"Bearer {self.auth_token}"
        if self.rem_checkin and self.rem_checkin.compliance_token:
            headers["X-Compliance-Token"] = self.rem_checkin.compliance_token
        return headers

    @property
    def enabled(self) -> bool:
        """Check if Synapse publishing is enabled (and not paused)."""
        return self.endpoint is not None and not self._paused

    def pause(self):
        """Pause publishing (session-level disconnect)."""
        self._paused = True
        logger.info("Synapse publishing paused (disconnected from NodusNet)")

    def unpause(self):
        """Resume publishing (reconnect to NodusNet)."""
        self._paused = False
        logger.info("Synapse publishing resumed (reconnected to NodusNet)")

    async def publish_async(self, segment: TranscriptSegmentV1) -> bool:
        """
        Publish segment to Synapse asynchronously.

        Returns True on success, False on failure.
        """
        if not self.endpoint:
            return False

        url = f"{self.endpoint.rstrip('/')}/v1/segments"

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    url,
                    json=segment.model_dump(mode="json"),
                    headers=self._auth_headers(),
                )
                response.raise_for_status()

                self._published_count += 1
                logger.debug(
                    "Segment published to Synapse",
                    segment_id=str(segment.segment_id),
                    endpoint=self.endpoint,
                )
                return True

        except httpx.ConnectError:
            self._failure_count += 1
            logger.warning(
                "Cannot reach Synapse",
                endpoint=self.endpoint,
                segment_id=str(segment.segment_id),
            )
            return False

        except httpx.HTTPStatusError as e:
            self._failure_count += 1
            logger.error(
                "Synapse rejected segment",
                status_code=e.response.status_code,
                segment_id=str(segment.segment_id),
            )
            return False

        except Exception as e:
            self._failure_count += 1
            logger.error(
                "Synapse publish error",
                error=str(e),
                segment_id=str(segment.segment_id),
            )
            return False

    def publish(self, segment: TranscriptSegmentV1) -> bool:
        """
        Publish segment to Synapse synchronously.

        Returns True on success, False on failure.
        """
        if not self.endpoint:
            return False

        url = f"{self.endpoint.rstrip('/')}/v1/segments"

        try:
            with httpx.Client(timeout=self.timeout) as client:
                response = client.post(
                    url,
                    json=segment.model_dump(mode="json"),
                    headers=self._auth_headers(),
                )
                response.raise_for_status()

                self._published_count += 1
                logger.debug(
                    "Segment published to Synapse",
                    segment_id=str(segment.segment_id),
                    endpoint=self.endpoint,
                )
                return True

        except httpx.ConnectError:
            self._failure_count += 1
            logger.warning(
                "Cannot reach Synapse",
                endpoint=self.endpoint,
                segment_id=str(segment.segment_id),
            )
            return False

        except httpx.HTTPStatusError as e:
            self._failure_count += 1
            logger.error(
                "Synapse rejected segment",
                status_code=e.response.status_code,
                segment_id=str(segment.segment_id),
            )
            return False

        except Exception as e:
            self._failure_count += 1
            logger.error(
                "Synapse publish error",
                error=str(e),
                segment_id=str(segment.segment_id),
            )
            return False

    def publish_fm(self, segment: FMTranscriptSegmentV1) -> bool:
        """
        Publish FM segment to Synapse synchronously.

        Returns True on success, False on failure.
        """
        if not self.endpoint:
            return False

        url = f"{self.endpoint.rstrip('/')}/v1/segments"

        try:
            with httpx.Client(timeout=self.timeout) as client:
                response = client.post(
                    url,
                    json=segment.model_dump(mode="json"),
                    headers=self._auth_headers(),
                )
                response.raise_for_status()

                self._published_count += 1
                logger.debug(
                    "FM segment published to Synapse",
                    segment_id=str(segment.segment_id),
                    frequency_hz=segment.rf_channel.frequency_hz,
                    endpoint=self.endpoint,
                )
                return True

        except httpx.ConnectError:
            self._failure_count += 1
            logger.warning(
                "Cannot reach Synapse",
                endpoint=self.endpoint,
                segment_id=str(segment.segment_id),
            )
            return False

        except httpx.HTTPStatusError as e:
            self._failure_count += 1
            logger.error(
                "Synapse rejected FM segment",
                status_code=e.response.status_code,
                segment_id=str(segment.segment_id),
            )
            return False

        except Exception as e:
            self._failure_count += 1
            logger.error(
                "Synapse publish error",
                error=str(e),
                segment_id=str(segment.segment_id),
            )
            return False

    def publish_hf(self, segment: HFTranscriptSegmentV1) -> bool:
        """
        Publish HF segment to Synapse synchronously.

        Returns True on success, False on failure.
        """
        if not self.endpoint:
            return False

        url = f"{self.endpoint.rstrip('/')}/v1/segments"

        try:
            with httpx.Client(timeout=self.timeout) as client:
                response = client.post(
                    url,
                    json=segment.model_dump(mode="json"),
                    headers=self._auth_headers(),
                )
                response.raise_for_status()

                self._published_count += 1
                logger.debug(
                    "HF segment published to Synapse",
                    segment_id=str(segment.segment_id),
                    frequency_hz=segment.rf_channel.frequency_hz,
                    station_callsign=segment.station_callsign,
                    endpoint=self.endpoint,
                )
                return True

        except httpx.ConnectError:
            self._failure_count += 1
            logger.warning(
                "Cannot reach Synapse",
                endpoint=self.endpoint,
                segment_id=str(segment.segment_id),
            )
            return False

        except httpx.HTTPStatusError as e:
            self._failure_count += 1
            logger.error(
                "Synapse rejected HF segment",
                status_code=e.response.status_code,
                segment_id=str(segment.segment_id),
            )
            return False

        except Exception as e:
            self._failure_count += 1
            logger.error(
                "Synapse publish error",
                error=str(e),
                segment_id=str(segment.segment_id),
            )
            return False

    def publish_aprs(self, segment: APRSPacketSegmentV1) -> bool:
        """Publish APRS segment to Synapse synchronously."""
        if not self.endpoint:
            return False

        url = f"{self.endpoint.rstrip('/')}/v1/segments"

        try:
            with httpx.Client(timeout=self.timeout) as client:
                response = client.post(
                    url,
                    json=segment.model_dump(mode="json"),
                    headers=self._auth_headers(),
                )
                response.raise_for_status()

                self._published_count += 1
                logger.debug(
                    "APRS segment published to Synapse",
                    segment_id=str(segment.segment_id),
                    from_callsign=segment.from_callsign,
                    packet_type=segment.packet_type,
                    endpoint=self.endpoint,
                )
                return True

        except httpx.ConnectError:
            self._failure_count += 1
            logger.warning(
                "Cannot reach Synapse",
                endpoint=self.endpoint,
                segment_id=str(segment.segment_id),
            )
            return False

        except httpx.HTTPStatusError as e:
            self._failure_count += 1
            logger.error(
                "Synapse rejected APRS segment",
                status_code=e.response.status_code,
                segment_id=str(segment.segment_id),
            )
            return False

        except Exception as e:
            self._failure_count += 1
            logger.error(
                "Synapse publish error",
                error=str(e),
                segment_id=str(segment.segment_id),
            )
            return False

    def get_stats(self) -> dict:
        """Get publisher statistics."""
        return {
            "enabled": self.enabled,
            "configured": self.endpoint is not None,
            "paused": self._paused,
            "endpoint": self.endpoint,
            "published_count": self._published_count,
            "failure_count": self._failure_count,
        }
