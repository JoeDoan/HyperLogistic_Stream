"""Canonical 40-hub registry for US logistics and passenger airports."""

from typing import NamedTuple


class HubInfo(NamedTuple):
    iata: str
    icao: str
    tz: str
    lat: float
    lon: float
    name: str


HUB_REGISTRY: dict[str, HubInfo] = {
    "ATL": HubInfo("ATL", "KATL", "America/New_York", 33.6407, -84.4277, "Atlanta Hartsfield"),
    "ORD": HubInfo("ORD", "KORD", "America/Chicago", 41.9742, -87.9073, "Chicago O'Hare"),
    "DFW": HubInfo("DFW", "KDFW", "America/Chicago", 32.8998, -97.0403, "Dallas/Fort Worth"),
    "DEN": HubInfo("DEN", "KDEN", "America/Denver", 39.8561, -104.6737, "Denver"),
    "LAX": HubInfo("LAX", "KLAX", "America/Los_Angeles", 33.9425, -118.4081, "Los Angeles"),
    "JFK": HubInfo("JFK", "KJFK", "America/New_York", 40.6413, -73.7781, "New York JFK"),
    "EWR": HubInfo("EWR", "KEWR", "America/New_York", 40.6895, -74.1745, "Newark Liberty"),
    "SFO": HubInfo("SFO", "KSFO", "America/Los_Angeles", 37.6213, -122.3790, "San Francisco"),
    "MEM": HubInfo("MEM", "KMEM", "America/Chicago", 35.0424, -89.9767, "Memphis (FedEx Hub)"),
    "SDF": HubInfo("SDF", "KSDF", "America/New_York", 38.1744, -85.7360, "Louisville (UPS Hub)"),
    "CVG": HubInfo("CVG", "KCVG", "America/New_York", 39.0489, -84.6678, "Cincinnati (Amazon Air)"),
    "CLT": HubInfo("CLT", "KCLT", "America/New_York", 35.2140, -80.9431, "Charlotte Douglas"),
    "LAS": HubInfo("LAS", "KLAS", "America/Los_Angeles", 36.0840, -115.1537, "Las Vegas"),
    "PHX": HubInfo("PHX", "KPHX", "America/Phoenix", 33.4352, -112.0101, "Phoenix Sky Harbor"),
    "MIA": HubInfo("MIA", "KMIA", "America/New_York", 25.7959, -80.2870, "Miami"),
    "IAH": HubInfo("IAH", "KIAH", "America/Chicago", 29.9844, -95.3414, "Houston"),
    "BOS": HubInfo("BOS", "KBOS", "America/New_York", 42.3656, -71.0096, "Boston Logan"),
    "SEA": HubInfo("SEA", "KSEA", "America/Los_Angeles", 47.4502, -122.3088, "Seattle-Tacoma"),
    "MSP": HubInfo("MSP", "KMSP", "America/Chicago", 44.8848, -93.2223, "Minneapolis"),
    "DTW": HubInfo("DTW", "KDTW", "America/New_York", 42.2162, -83.3554, "Detroit Metro"),
    "PHL": HubInfo("PHL", "KPHL", "America/New_York", 39.8744, -75.2424, "Philadelphia"),
    "BWI": HubInfo("BWI", "KBWI", "America/New_York", 39.1774, -76.6684, "Baltimore/Washington"),
    "SLC": HubInfo("SLC", "KSLC", "America/Denver", 40.7899, -111.9791, "Salt Lake City"),
    "MCO": HubInfo("MCO", "KMCO", "America/New_York", 28.4312, -81.3081, "Orlando"),
    "IND": HubInfo("IND", "KIND", "America/Indiana/Indianapolis", 39.7250, -86.2820, "Indianapolis (FedEx Hub)"),
    "ONT": HubInfo("ONT", "KONT", "America/Los_Angeles", 34.0530, -117.5770, "Ontario CA (Freight Hub)"),
    "MCI": HubInfo("MCI", "KMCI", "America/Chicago", 39.2970, -94.7310, "Kansas City"),
    "STL": HubInfo("STL", "KSTL", "America/Chicago", 38.7520, -90.3730, "St. Louis"),
    "IAD": HubInfo("IAD", "KIAD", "America/New_York", 38.9350, -77.4470, "Washington Dulles"),
    "DCA": HubInfo("DCA", "KDCA", "America/New_York", 38.8470, -77.0350, "Washington Reagan"),
    "SAN": HubInfo("SAN", "KSAN", "America/Los_Angeles", 32.7340, -117.1830, "San Diego"),
    "TPA": HubInfo("TPA", "KTPA", "America/New_York", 27.9630, -82.5400, "Tampa"),
    "AUS": HubInfo("AUS", "KAUS", "America/Chicago", 30.1830, -97.6800, "Austin"),
    "BNA": HubInfo("BNA", "KBNA", "America/Chicago", 36.1110, -86.6880, "Nashville"),
    "RDU": HubInfo("RDU", "KRDU", "America/New_York", 35.8920, -78.7820, "Raleigh-Durham"),
    "PDX": HubInfo("PDX", "KPDX", "America/Los_Angeles", 45.5960, -122.6090, "Portland"),
    "SMF": HubInfo("SMF", "KSMF", "America/Los_Angeles", 38.7010, -121.5950, "Sacramento"),
    "MKE": HubInfo("MKE", "KMKE", "America/Chicago", 42.9550, -87.9050, "Milwaukee"),
    "PIT": HubInfo("PIT", "KPIT", "America/New_York", 40.4850, -80.2140, "Pittsburgh"),
    "CLE": HubInfo("CLE", "KCLE", "America/New_York", 41.4060, -81.8520, "Cleveland"),
}

HUBS: list[str] = list(HUB_REGISTRY.keys())
DEFAULT_ICAO_AIRPORTS: list[str] = [h.icao for h in HUB_REGISTRY.values()]
AIRPORT_TZ: dict[str, str] = {k: h.tz for k, h in HUB_REGISTRY.items()}
HUB_COORDS_LATLON: dict[str, tuple[float, float]] = {k: (h.lat, h.lon) for k, h in HUB_REGISTRY.items()}
HUB_COORDS_NAMED: dict[str, tuple[float, float, str]] = {k: (h.lat, h.lon, h.name) for k, h in HUB_REGISTRY.items()}
