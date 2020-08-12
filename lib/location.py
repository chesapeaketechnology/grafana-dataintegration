from dataclasses import dataclass
from psycopg2.extensions import register_adapter, adapt, AsIs


@dataclass(repr=True)
class Location:
    longitude: float
    latitude: float
    altitude: float


def adapt_location(location):
    lon = adapt(location.longitude).getquoted().decode('utf-8')
    lat = adapt(location.latitude).getquoted().decode('utf-8')
    return AsIs('ST_MakePoint(%s, %s)' % (lon, lat))


register_adapter(Location, adapt_location)
