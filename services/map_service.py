"""
MapService client for fetching POI configurations
"""
import httpx
import logging
from typing import List, Dict
from config.config import settings

logger = logging.getLogger(__name__)


class MapServiceClient:
    """Client for communicating with MapService"""
    
    def __init__(self, base_url: str = None, timeout: int = 10):
        """
        Args:
            base_url: MapService base URL (from env or config)
            timeout: Request timeout in seconds
        """
        self.base_url = base_url or getattr(settings, 'MAP_SERVICE_URL', 'http://mapservice:8000')
        self.timeout = timeout
    
    async def fetch_pois(self) -> List[Dict]:
        """
        Fetch all POI configurations from MapService
        
        Returns:
            List of POI dictionaries with structure:
            {
                "id": "restroom-a3",
                "name": "Restrooms Section A Level 3",
                "type": "restroom",
                "num_servers": 8,
                "service_rate": 0.5
            }
        
        MapService returns Node objects with additional fields (x, y, level, etc),
        but we only need the queue-related fields for wait time calculations.
        """
        url = f"{self.base_url}/pois"
        
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                logger.info(f"Fetching POIs from MapService: {url}")
                response = await client.get(url)
                response.raise_for_status()
                
                pois = response.json()
                logger.info(f"Successfully fetched {len(pois)} POIs from MapService")
                
                # Validate that POIs have required fields for queue calculations
                for poi in pois:
                    if not poi.get('num_servers') or not poi.get('service_rate'):
                        logger.warning(
                            f"POI {poi.get('id')} missing num_servers or service_rate, "
                            f"using defaults (num_servers=1, service_rate=0.5)"
                        )
                        poi['num_servers'] = poi.get('num_servers', 1)
                        poi['service_rate'] = poi.get('service_rate', 0.5)
                
                return pois
                
        except httpx.HTTPError as e:
            logger.error(f"Failed to fetch POIs from MapService: {e}")
            raise RuntimeError(f"MapService unavailable: {e}")
    
    async def fetch_poi_by_id(self, poi_id: str) -> Dict:
        """
        Fetch specific POI configuration by ID
        
        Args:
            poi_id: POI identifier
            
        Returns:
            POI dictionary
        """
        url = f"{self.base_url}/pois/{poi_id}"
        
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(url)
                response.raise_for_status()
                
                poi = response.json()
                logger.info(f"Fetched POI {poi_id} from MapService")
                
                return poi
                
        except httpx.HTTPError as e:
            logger.error(f"Failed to fetch POI {poi_id}: {e}")
            raise RuntimeError(f"POI {poi_id} not found in MapService")
    
    async def health_check(self) -> bool:
        """
        Check if MapService is available
        
        Returns:
            True if service is healthy, False otherwise
        """
        url = f"{self.base_url}/health"
        
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                response = await client.get(url)
                return response.status_code == 200
        except Exception:
            return False
