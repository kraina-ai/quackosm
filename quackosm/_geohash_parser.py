"""
Geohash encoding/decoding and associated functions.

(c) Chris Veness 2014-2016 / MIT Licence https://www.movable-type.co.uk/scripts/geohash.html

(c) Anu Joy 2018 / MIT Licence
https://github.com/joyanujoy/geolib/blob/19c8e544e3ed6d39a13af6e96461346a778ae685/geolib/geohash.py#L50

http://en.wikipedia.org/wiki/Geohash
"""

from collections.abc import Generator

base32 = "0123456789bcdefghjkmnpqrstuvwxyz"


def _indexes(geohash: str) -> Generator[int, None, None]:
    if not geohash:
        raise ValueError("Invalid geohash")

    for char in geohash:
        try:
            yield base32.index(char)
        except ValueError:
            raise ValueError("Invalid geohash") from None


def geohash_bounds(geohash: str) -> tuple[float, float, float, float]:
    geohash = geohash.lower()

    even_bit = True
    lat_min = -90.0
    lat_max = 90.0
    lon_min = -180.0
    lon_max = 180.0

    # 5 bits for a char. So divide the decimal by power of 2, then AND 1
    # to get the binary bit - fast modulo operation.
    for index in _indexes(geohash):
        for n in range(4, -1, -1):
            bit = (index >> n) & 1
            if even_bit:
                # longitude
                lon_mid = (lon_min + lon_max) / 2
                if bit == 1:
                    lon_min = lon_mid
                else:
                    lon_max = lon_mid
            else:
                # latitude
                lat_mid = (lat_min + lat_max) / 2
                if bit == 1:
                    lat_min = lat_mid
                else:
                    lat_max = lat_mid
            even_bit = not even_bit

    return lon_min, lat_min, lon_max, lat_max
