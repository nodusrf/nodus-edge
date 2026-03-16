"""
Nodus Edge - ORION System Configuration Lookup

This module provides fast talkgroup and site lookups for the ORION P25 system.
It is used by Edge to enrich raw observations with metadata before emission.

Usage:
    from nodus_edge.orion_lookup import ORIONLookup

    lookup = ORIONLookup()
    tg_info = lookup.get_talkgroup(443)
    # Returns: TalkgroupInfo(alpha_tag='OFD Dispatch', description='Dispatch',
    #           tag='Fire Dispatch', category='Omaha Fire',
    #           mode='TE', encrypted=True)
"""

import json
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, Dict, Any

import structlog

logger = structlog.get_logger(__name__)


@dataclass
class TalkgroupInfo:
    """Structured talkgroup metadata for Edge observations."""
    tgid: int
    alpha_tag: str
    description: str
    tag: str  # e.g., "Fire Dispatch", "Law Tac"
    category: str  # e.g., "Omaha Fire", "Douglas County"
    mode: str  # D, DE, T, TE
    encrypted: bool

    def to_dict(self) -> Dict[str, Any]:
        return {
            'tgid': self.tgid,
            'alpha_tag': self.alpha_tag,
            'description': self.description,
            'tag': self.tag,
            'category': self.category,
            'mode': self.mode,
            'encrypted': self.encrypted
        }

    @property
    def is_dispatch(self) -> bool:
        """True if this is a dispatch channel (high priority for Cortex)."""
        return 'Dispatch' in self.tag

    @property
    def is_tactical(self) -> bool:
        """True if this is a tactical channel (active incident work)."""
        return 'Tac' in self.tag

    @property
    def service_type(self) -> str:
        """Returns: 'law', 'fire', 'ems', 'multi', or 'other'."""
        tag_lower = self.tag.lower()
        if 'law' in tag_lower:
            return 'law'
        elif 'fire' in tag_lower:
            return 'fire'
        elif 'ems' in tag_lower:
            return 'ems'
        elif 'multi' in tag_lower:
            return 'multi'
        else:
            return 'other'


@dataclass
class SiteInfo:
    """Site/tower information for geo-hinting."""
    site_id: int
    name: str
    county: str
    site_type: str  # 'site' or 'simulcast'
    control_channels: list

    def to_dict(self) -> Dict[str, Any]:
        return {
            'site_id': self.site_id,
            'name': self.name,
            'county': self.county,
            'type': self.site_type,
            'control_channels': self.control_channels
        }


class ORIONLookup:
    """
    Fast lookup service for ORION P25 system metadata.

    Edge uses this to enrich observations with:
    - Talkgroup identification (alpha tag, description, service type)
    - Encryption status
    - Site/geographic context
    - Category for downstream routing
    """

    # Mode to encryption mapping
    MODE_ENCRYPTED = {'DE', 'TE'}

    def __init__(self, config_dir: Optional[Path] = None):
        """
        Initialize the lookup service.

        Args:
            config_dir: Path to config directory. Defaults to ./config/orion/
        """
        if config_dir is None:
            config_dir = Path(__file__).parent / 'config' / 'orion'

        self.config_dir = Path(config_dir)
        self._talkgroups: Dict[int, TalkgroupInfo] = {}
        self._sites: Dict[int, SiteInfo] = {}
        self._system_info: Dict[str, Any] = {}

        self._load_config()

    def _load_config(self):
        """Load system and talkgroup configuration files."""
        # Load system config
        system_path = self.config_dir / 'system.json'
        if system_path.exists():
            with open(system_path) as f:
                data = json.load(f)
                self._system_info = data.get('system', {})

                # Parse sites
                for site_id_str, site_data in data.get('sites', {}).items():
                    site_id = int(site_id_str)
                    self._sites[site_id] = SiteInfo(
                        site_id=site_id,
                        name=site_data['name'],
                        county=site_data['county'],
                        site_type=site_data['type'],
                        control_channels=site_data.get('control_channels', [])
                    )

            logger.info(
                "Loaded ORION sites",
                count=len(self._sites),
                path=str(system_path),
            )
        else:
            logger.warning("ORION system config not found", path=str(system_path))

        # Load talkgroups
        tg_path = self.config_dir / 'talkgroups.json'
        if tg_path.exists():
            with open(tg_path) as f:
                data = json.load(f)

                for tgid_str, tg_array in data.get('talkgroups', {}).items():
                    tgid = int(tgid_str)
                    # Array format: [alpha_tag, description, tag, category, mode]
                    if len(tg_array) >= 5:
                        mode = tg_array[4]
                        self._talkgroups[tgid] = TalkgroupInfo(
                            tgid=tgid,
                            alpha_tag=tg_array[0],
                            description=tg_array[1],
                            tag=tg_array[2],
                            category=tg_array[3],
                            mode=mode,
                            encrypted=mode in self.MODE_ENCRYPTED
                        )

            logger.info(
                "Loaded ORION talkgroups",
                count=len(self._talkgroups),
                path=str(tg_path),
            )
        else:
            logger.warning("ORION talkgroups config not found", path=str(tg_path))

    def get_talkgroup(self, tgid: int) -> Optional[TalkgroupInfo]:
        """
        Look up talkgroup by ID.

        Args:
            tgid: Decimal talkgroup ID

        Returns:
            TalkgroupInfo if found, None otherwise
        """
        return self._talkgroups.get(tgid)

    def get_talkgroup_dict(self, tgid: int) -> Optional[Dict[str, Any]]:
        """
        Look up talkgroup and return as dictionary (for JSON serialization).

        Args:
            tgid: Decimal talkgroup ID

        Returns:
            Dict with talkgroup info, or None
        """
        tg = self.get_talkgroup(tgid)
        return tg.to_dict() if tg else None

    def get_site(self, site_id: int) -> Optional[SiteInfo]:
        """
        Look up site by ID.

        Args:
            site_id: Site ID from P25 control channel

        Returns:
            SiteInfo if found, None otherwise
        """
        return self._sites.get(site_id)

    def get_site_dict(self, site_id: int) -> Optional[Dict[str, Any]]:
        """Look up site and return as dictionary."""
        site = self.get_site(site_id)
        return site.to_dict() if site else None

    def is_encrypted(self, tgid: int) -> bool:
        """
        Quick check if a talkgroup is encrypted.

        Args:
            tgid: Decimal talkgroup ID

        Returns:
            True if encrypted (mode DE or TE), False otherwise.
            Returns False for unknown talkgroups.
        """
        tg = self.get_talkgroup(tgid)
        return tg.encrypted if tg else False

    def get_service_type(self, tgid: int) -> str:
        """
        Get service type for a talkgroup.

        Args:
            tgid: Decimal talkgroup ID

        Returns:
            'law', 'fire', 'ems', 'multi', 'other', or 'unknown'
        """
        tg = self.get_talkgroup(tgid)
        return tg.service_type if tg else 'unknown'

    def enrich_observation(self, tgid: int, site_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Enrich a raw observation with all available metadata.

        This is the primary method Edge uses to add context to observations
        before emission to Synapse.

        Args:
            tgid: Decimal talkgroup ID
            site_id: Optional site ID

        Returns:
            Dict with enrichment data for EdgeObservation.v1 schema
        """
        result = {
            'talkgroup': None,
            'site': None,
            'system': {
                'name': self._system_info.get('short_name', 'ORION'),
                'sysid': self._system_info.get('system_id', {}).get('sysid'),
                'wacn': self._system_info.get('system_id', {}).get('wacn')
            }
        }

        tg = self.get_talkgroup(tgid)
        if tg:
            result['talkgroup'] = {
                'tgid': tgid,
                'alpha_tag': tg.alpha_tag,
                'description': tg.description,
                'tag': tg.tag,
                'category': tg.category,
                'mode': tg.mode,
                'encrypted': tg.encrypted,
                'service_type': tg.service_type,
                'is_dispatch': tg.is_dispatch,
                'is_tactical': tg.is_tactical
            }
        else:
            # Unknown talkgroup - still emit what we know
            result['talkgroup'] = {
                'tgid': tgid,
                'alpha_tag': f'Unknown-{tgid}',
                'description': f'Unknown talkgroup {tgid}',
                'tag': 'Unknown',
                'category': 'Unknown',
                'mode': 'D',
                'encrypted': False,
                'service_type': 'unknown',
                'is_dispatch': False,
                'is_tactical': False
            }

        if site_id:
            site = self.get_site(site_id)
            if site:
                result['site'] = site.to_dict()

        return result

    @property
    def system_name(self) -> str:
        """Return system short name."""
        return self._system_info.get('short_name', 'ORION')

    @property
    def talkgroup_count(self) -> int:
        """Return number of known talkgroups."""
        return len(self._talkgroups)

    @property
    def site_count(self) -> int:
        """Return number of known sites."""
        return len(self._sites)

    def get_talkgroups_by_category(self, category: str) -> list:
        """Get all talkgroups in a category."""
        return [tg for tg in self._talkgroups.values()
                if tg.category.lower() == category.lower()]

    def get_talkgroups_by_service(self, service: str) -> list:
        """Get all talkgroups for a service type (law, fire, ems, multi)."""
        return [tg for tg in self._talkgroups.values()
                if tg.service_type == service.lower()]


# Convenience singleton for simple usage
_default_lookup: Optional[ORIONLookup] = None

def get_lookup() -> ORIONLookup:
    """Get the default ORION lookup instance (singleton)."""
    global _default_lookup
    if _default_lookup is None:
        _default_lookup = ORIONLookup()
    return _default_lookup


def lookup_talkgroup(tgid: int) -> Optional[Dict[str, Any]]:
    """Quick talkgroup lookup using default instance."""
    return get_lookup().get_talkgroup_dict(tgid)


def enrich(tgid: int, site_id: Optional[int] = None) -> Dict[str, Any]:
    """Quick observation enrichment using default instance."""
    return get_lookup().enrich_observation(tgid, site_id)
