"""Recalculate OSM indexes and copy them to dedicated location."""

from pathlib import Path

from quackosm.osm_extracts import OSM_EXTRACT_SOURCE_INDEX_FUNCTION, clear_osm_index_cache
from quackosm.osm_extracts.extract import OsmExtractSource, _get_global_cache_file_path

if __name__ == "__main__":
    clear_osm_index_cache()
    for get_index_function in OSM_EXTRACT_SOURCE_INDEX_FUNCTION.values():
        get_index_function(force_recalculation=True)

    extract_sources = [_source for _source in OsmExtractSource if _source != OsmExtractSource.any]

    for extract_source in extract_sources:
        cache_path = _get_global_cache_file_path(extract_source)
        destination_path = (
            Path(__file__).parent.parent
            / "precalculated_indexes"
            / f"{extract_source.value.lower()}_index.geojson"
        )
        print(f"Copying cache file {cache_path} to {destination_path}.")
        destination_path.write_bytes(cache_path.read_bytes())
