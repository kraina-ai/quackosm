import geopandas as gpd
from packaging import version

GEOPANDAS_NEW_API = version.parse(gpd.__version__) >= version.parse("1.0.0")
