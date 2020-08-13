from dataclasses import dataclass
from psycopg2.extensions import register_adapter, adapt, AsIs


@dataclass(repr=True)
class Location:
    """
    A simple structure for geo location information
    """
    longitude: float
    latitude: float
    altitude: float


def adapt_location(location):
    """
    Adapts a :class:Location object to be parameterized as part of a psycopg2 query. It is
    converted to a PostGIS geometry(Point) object.

    Please refer to `PostGIS <https://postgis.net/docs/ST_MakePoint.html>`_ for more information.

    :param Location location: the location to be parameterized
    :return: None
    """
    lon = adapt(location.longitude).getquoted().decode('utf-8')
    lat = adapt(location.latitude).getquoted().decode('utf-8')
    return AsIs('ST_MakePoint(%s, %s)' % (lon, lat))

# Register the adapter when the location.py file is loaded.
register_adapter(Location, adapt_location)
