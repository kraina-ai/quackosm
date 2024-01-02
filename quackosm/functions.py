"""
Functions.

This module contains helper functions to simplify the usage.
"""

from collections.abc import Iterable
from pathlib import Path
from typing import Any, Optional, Union

import geopandas as gpd
from shapely.geometry.base import BaseGeometry

from quackosm._osm_tags_filters import GroupedOsmTagsFilter, OsmTagsFilter
from quackosm._osm_way_polygon_features import OsmWayPolygonConfig
from quackosm.pbf_file_reader import PbfFileReader


def convert_pbf_to_gpq(
    pbf_path: Union[str, Path],
    tags_filter: Optional[Union[OsmTagsFilter, GroupedOsmTagsFilter]] = None,
    geometry_filter: Optional[BaseGeometry] = None,
    result_file_path: Optional[Union[str, Path]] = None,
    explode_tags: Optional[bool] = None,
    ignore_cache: bool = False,
    filter_osm_ids: Optional[list[str]] = None,
    working_directory: Union[str, Path] = "files",
    osm_way_polygon_features_config: Optional[Union[OsmWayPolygonConfig, dict[str, Any]]] = None,
) -> Path:
    """
    Convert PBF file to GeoParquet file.

    Args:
        pbf_path (Union[str, Path]): Pbf file to be parsed to GeoParquet.
        tags_filter (Union[OsmTagsFilter, GroupedOsmTagsFilter], optional): A dictionary
            specifying which tags to download.
            The keys should be OSM tags (e.g. `building`, `amenity`).
            The values should either be `True` for retrieving all objects with the tag,
            string for retrieving a single tag-value pair
            or list of strings for retrieving all values specified in the list.
            `tags={'leisure': 'park}` would return parks from the area.
            `tags={'leisure': 'park, 'amenity': True, 'shop': ['bakery', 'bicycle']}`
            would return parks, all amenity types, bakeries and bicycle shops.
            If `None`, handler will allow all of the tags to be parsed. Defaults to `None`.
        geometry_filter (BaseGeometry, optional): Region which can be used to filter only
            intersecting OSM objects. Defaults to `None`.
        result_file_path (Union[str, Path], optional): Where to save
            the geoparquet file. If not provided, will be generated based on hashes
            from provided tags filter and geometry filter. Defaults to `None`.
        explode_tags (bool, optional): Whether to split tags into columns based on OSM tag keys.
            If `None`, will be set based on `tags_filter` parameter. If no tags filter is provided,
            then `explode_tags` will set to `False`, if there is tags filter it will set to `True`.
            Defaults to `None`.
        ignore_cache (bool, optional): Whether to ignore precalculated geoparquet files or not.
            Defaults to False.
        filter_osm_ids: (list[str], optional): List of OSM features ids to read from the file.
            Have to be in the form of 'node/<id>', 'way/<id>' or 'relation/<id>'.
            Defaults to an empty list.
        working_directory (Union[str, Path], optional): Directory where to save
            the parsed `*.parquet` files. Defaults to "files".
        osm_way_polygon_features_config (Union[OsmWayPolygonConfig, dict[str, Any]], optional):
            Config used to determine which closed way features are polygons.
            Modifications to this config left are left for experienced OSM users.
            Defaults to predefined "osm_way_polygon_features.json".

    Returns:
        Path: Path to the generated GeoParquet file.

    Examples:
        Get OSM data from a PBF file.

        Tags will be kept in a single column.
        >>> import quackosm as qosm
        >>> gpq_path = qosm.convert_pbf_to_gpq(monaco_pbf_path)
        >>> gpq_path.as_posix()
        'files/monaco_nofilter_noclip_compact.geoparquet'

        Inspect the file with duckdb
        >>> import duckdb
        >>> duckdb.load_extension('spatial')
        >>> duckdb.read_parquet(str(gpq_path)).project(
        ...     "* REPLACE (ST_GeomFromWKB(geometry) AS geometry)"
        ... ).order("feature_id") # doctest: +SKIP
        ┌──────────────────┬──────────────────────┬──────────────────────────────────────────────┐
        │    feature_id    │         tags         │                   geometry                   │
        │     varchar      │ map(varchar, varch…  │                   geometry                   │
        ├──────────────────┼──────────────────────┼──────────────────────────────────────────────┤
        │ node/10005045289 │ {shop=bakery}        │ POINT (7.4224498 43.7310532)                 │
        │ node/10020887517 │ {leisure=swimming_…  │ POINT (7.4131561 43.7338391)                 │
        │ node/10021298117 │ {leisure=swimming_…  │ POINT (7.4277743 43.7427669)                 │
        │ node/10021298717 │ {leisure=swimming_…  │ POINT (7.4263029 43.7409734)                 │
        │ node/10025656383 │ {ferry=yes, name=Q…  │ POINT (7.4254971 43.7369002)                 │
        │ node/10025656390 │ {amenity=restauran…  │ POINT (7.4269287 43.7368818)                 │
        │ node/10025656391 │ {name=Capitainerie…  │ POINT (7.4272127 43.7359593)                 │
        │ node/10025656392 │ {name=Direction de…  │ POINT (7.4270392 43.7365262)                 │
        │ node/10025656393 │ {name=IQOS, openin…  │ POINT (7.4275175 43.7373195)                 │
        │ node/10025656394 │ {artist_name=Anna …  │ POINT (7.4293446 43.737448)                  │
        │       ·          │          ·           │              ·                               │
        │       ·          │          ·           │              ·                               │
        │       ·          │          ·           │              ·                               │
        │ way/986864693    │ {natural=bare_rock}  │ POLYGON ((7.4340482 43.745598, 7.4340263 4…  │
        │ way/986864694    │ {barrier=wall}       │ LINESTRING (7.4327547 43.7445382, 7.432808…  │
        │ way/986864695    │ {natural=bare_rock}  │ POLYGON ((7.4332994 43.7449315, 7.4332912 …  │
        │ way/986864696    │ {barrier=wall}       │ LINESTRING (7.4356006 43.7464325, 7.435574…  │
        │ way/986864697    │ {natural=bare_rock}  │ POLYGON ((7.4362767 43.74697, 7.4362983 43…  │
        │ way/990669427    │ {amenity=shelter, …  │ POLYGON ((7.4146087 43.733883, 7.4146192 4…  │
        │ way/990669428    │ {highway=secondary…  │ LINESTRING (7.4136598 43.7334433, 7.413640…  │
        │ way/990669429    │ {highway=secondary…  │ LINESTRING (7.4137621 43.7334251, 7.413746…  │
        │ way/990848785    │ {addr:city=Monaco,…  │ POLYGON ((7.4142551 43.7339622, 7.4143113 …  │
        │ way/993121275    │ {building=yes, nam…  │ POLYGON ((7.4321416 43.7481309, 7.4321638 …  │
        ├──────────────────┴──────────────────────┴──────────────────────────────────────────────┤
        │ 7906 rows (20 shown)                                                         3 columns │
        └────────────────────────────────────────────────────────────────────────────────────────┘

        Get only buildings, amenities and highways from a PBF file.

        Tags will be split into separate columns because of applying the filter.
        >>> gpq_path = qosm.convert_pbf_to_gpq(
        ...     monaco_pbf_path,
        ...     tags_filter={"building": True, "amenity": True, "highway": True}
        ... )
        >>> gpq_path.as_posix()
        'files/monaco_6593ca69098459d039054bc5fe0a87c56681e29a5f59d38ce3485c03cb0e9374_noclip_exploded.geoparquet'

        Inspect the file with duckdb
        >>> import duckdb
        >>> duckdb.load_extension('spatial')
        >>> duckdb.read_parquet(str(gpq_path)).project(
        ...     "* REPLACE (ST_GeomFromWKB(geometry) AS geometry)"
        ... ).order("feature_id") # doctest: +SKIP
        ┌──────────────────┬──────────┬────────────┬─────────────┬───────────────────────────────┐
        │    feature_id    │ building │  amenity   │   highway   │           geometry            │
        │     varchar      │ varchar  │  varchar   │   varchar   │           geometry            │
        ├──────────────────┼──────────┼────────────┼─────────────┼───────────────────────────────┤
        │ node/10025656390 │ NULL     │ restaurant │ NULL        │ POINT (7.4269287 43.7368818)  │
        │ node/10025843517 │ NULL     │ restaurant │ NULL        │ POINT (7.4219362 43.7367446)  │
        │ node/10025852089 │ NULL     │ bar        │ NULL        │ POINT (7.4227543 43.7369926)  │
        │ node/10025852090 │ NULL     │ restaurant │ NULL        │ POINT (7.4225093 43.7369627)  │
        │ node/10068880332 │ NULL     │ NULL       │ platform    │ POINT (7.4380849 43.7493273)  │
        │ node/10068880335 │ NULL     │ bench      │ NULL        │ POINT (7.4186855 43.7321515)  │
        │ node/10127713363 │ NULL     │ cafe       │ NULL        │ POINT (7.4266367 43.7420755)  │
        │ node/10601158089 │ NULL     │ restaurant │ NULL        │ POINT (7.4213086 43.7336187)  │
        │ node/10671507005 │ NULL     │ bar        │ NULL        │ POINT (7.4296915 43.7423307)  │
        │ node/10674256605 │ NULL     │ bar        │ NULL        │ POINT (7.4213558 43.7336317)  │
        │       ·          │  ·       │  ·         │  ·          │              ·                │
        │       ·          │  ·       │  ·         │  ·          │              ·                │
        │       ·          │  ·       │  ·         │  ·          │              ·                │
        │ way/981971425    │ NULL     │ NULL       │ residential │ LINESTRING (7.4321217 43.74…  │
        │ way/982061461    │ NULL     │ NULL       │ secondary   │ LINESTRING (7.4246341 43.74…  │
        │ way/982081599    │ NULL     │ NULL       │ tertiary    │ LINESTRING (7.4225202 43.73…  │
        │ way/982081600    │ NULL     │ NULL       │ service     │ LINESTRING (7.4225202 43.73…  │
        │ way/986029035    │ NULL     │ NULL       │ path        │ LINESTRING (7.4189462 43.73…  │
        │ way/990669427    │ NULL     │ shelter    │ NULL        │ POLYGON ((7.4146087 43.7338…  │
        │ way/990669428    │ NULL     │ NULL       │ secondary   │ LINESTRING (7.4136598 43.73…  │
        │ way/990669429    │ NULL     │ NULL       │ secondary   │ LINESTRING (7.4137621 43.73…  │
        │ way/990848785    │ yes      │ NULL       │ NULL        │ POLYGON ((7.4142551 43.7339…  │
        │ way/993121275    │ yes      │ NULL       │ NULL        │ POLYGON ((7.4321416 43.7481…  │
        ├──────────────────┴──────────┴────────────┴─────────────┴───────────────────────────────┤
        │ 5772 rows (20 shown)                                                         5 columns │
        └────────────────────────────────────────────────────────────────────────────────────────┘

        Get features for Malé - the capital city of Maldives

        Tags will be kept in a single column.
        >>> from shapely.geometry import box
        >>> gpq_path = qosm.convert_pbf_to_gpq(
        ...     maldives_pbf_path,
        ...     geometry_filter=box(
        ...         minx=73.4975872,
        ...         miny=4.1663240,
        ...         maxx=73.5215528,
        ...         maxy=4.1818121
        ...     )
        ... )
        >>> gpq_path.as_posix()
        'files/maldives_nofilter_35532d32333a47a057265be0d7903ce27f6aa6ca3df31fe45f4ce67e4dbb3fb5_compact.geoparquet'

        Inspect the file with duckdb
        >>> import duckdb
        >>> duckdb.load_extension('spatial')
        >>> duckdb.read_parquet(str(gpq_path)).project(
        ...     "* REPLACE (ST_GeomFromWKB(geometry) AS geometry)"
        ... ).order("feature_id") # doctest: +SKIP
        ┌──────────────────┬──────────────────────┬──────────────────────────────────────────────┐
        │    feature_id    │         tags         │                   geometry                   │
        │     varchar      │ map(varchar, varch…  │                   geometry                   │
        ├──────────────────┼──────────────────────┼──────────────────────────────────────────────┤
        │ node/10010180778 │ {brand=Ooredoo, br…  │ POINT (73.5179039 4.1752105)                 │
        │ node/10062500171 │ {contact:facebook=…  │ POINT (73.509583 4.1724485)                  │
        │ node/10078084764 │ {addr:city=Male', …  │ POINT (73.5047972 4.1726734)                 │
        │ node/10078086040 │ {addr:city=Malé, a…  │ POINT (73.5031714 4.1759622)                 │
        │ node/10158825718 │ {addr:postcode=201…  │ POINT (73.5083189 4.1730108)                 │
        │ node/10289176711 │ {addr:street=Dhona…  │ POINT (73.5133902 4.1725724)                 │
        │ node/10294045310 │ {amenity=restauran…  │ POINT (73.5091277 4.1735378)                 │
        │ node/10294045311 │ {amenity=restauran…  │ POINT (73.5055534 4.1759515)                 │
        │ node/10294045411 │ {amenity=restauran…  │ POINT (73.5037257 4.1717866)                 │
        │ node/10294045412 │ {amenity=restauran…  │ POINT (73.5024147 4.1761633)                 │
        │      ·           │          ·           │              ·                               │
        │      ·           │          ·           │              ·                               │
        │      ·           │          ·           │              ·                               │
        │ way/91986244     │ {highway=residenti…  │ LINESTRING (73.5069785 4.1704686, 73.50759…  │
        │ way/91986245     │ {highway=residenti…  │ LINESTRING (73.5135834 4.1740562, 73.51383…  │
        │ way/91986249     │ {highway=residenti…  │ LINESTRING (73.5153971 4.1735146, 73.51601…  │
        │ way/91986251     │ {highway=residenti…  │ LINESTRING (73.5082522 4.1709887, 73.50823…  │
        │ way/91986254     │ {highway=residenti…  │ LINESTRING (73.508114 4.1693477, 73.508154…  │
        │ way/91986255     │ {landuse=cemetery,…  │ POLYGON ((73.507509 4.1731064, 73.5078884 …  │
        │ way/91986256     │ {highway=residenti…  │ LINESTRING (73.5106692 4.1744828, 73.51082…  │
        │ way/935784864    │ {layer=-1, locatio…  │ LINESTRING (73.4875382 4.1703263, 73.50074…  │
        │ way/935784867    │ {layer=-1, locatio…  │ LINESTRING (73.446172 4.1856738, 73.460937…  │
        │ way/959150179    │ {amenity=place_of_…  │ POLYGON ((73.5184052 4.1755282, 73.5184863…  │
        ├──────────────────┴──────────────────────┴──────────────────────────────────────────────┤
        │ 2140 rows (20 shown)                                                         3 columns │
        └────────────────────────────────────────────────────────────────────────────────────────┘
    """
    return PbfFileReader(
        tags_filter=tags_filter,
        geometry_filter=geometry_filter,
        working_directory=working_directory,
        osm_way_polygon_features_config=osm_way_polygon_features_config,
    ).convert_pbf_to_gpq(
        pbf_path=pbf_path,
        result_file_path=result_file_path,
        explode_tags=explode_tags,
        ignore_cache=ignore_cache,
        filter_osm_ids=filter_osm_ids,
    )


def get_features_gdf(
    file_paths: Union[str, Path, Iterable[Union[str, Path]]],
    tags_filter: Optional[Union[OsmTagsFilter, GroupedOsmTagsFilter]] = None,
    geometry_filter: Optional[BaseGeometry] = None,
    explode_tags: Optional[bool] = None,
    ignore_cache: bool = False,
    filter_osm_ids: Optional[list[str]] = None,
    working_directory: Union[str, Path] = "files",
    osm_way_polygon_features_config: Optional[Union[OsmWayPolygonConfig, dict[str, Any]]] = None,
) -> gpd.GeoDataFrame:
    """
    Get features GeoDataFrame from a PBF file or list of PBF files.

    Function can parse multiple PBF files and returns a single GeoDataFrame with loaded
    OSM objects.

    Args:
        file_paths (Union[str, Path, Iterable[Union[str, Path]]]):
            Path or list of paths of `*.osm.pbf` files to be parsed.
        tags_filter (Union[OsmTagsFilter, GroupedOsmTagsFilter], optional): A dictionary
            specifying which tags to download.
            The keys should be OSM tags (e.g. `building`, `amenity`).
            The values should either be `True` for retrieving all objects with the tag,
            string for retrieving a single tag-value pair
            or list of strings for retrieving all values specified in the list.
            `tags={'leisure': 'park}` would return parks from the area.
            `tags={'leisure': 'park, 'amenity': True, 'shop': ['bakery', 'bicycle']}`
            would return parks, all amenity types, bakeries and bicycle shops.
            If `None`, handler will allow all of the tags to be parsed. Defaults to `None`.
        geometry_filter (BaseGeometry, optional): Region which can be used to filter only
            intersecting OSM objects. Defaults to `None`.
        explode_tags (bool, optional): Whether to split tags into columns based on OSM tag keys.
            If `None`, will be set based on `tags_filter` parameter. If no tags filter is provided,
            then `explode_tags` will set to `False`, if there is tags filter it will set to `True`.
            Defaults to `None`.
        ignore_cache: (bool, optional): Whether to ignore precalculated geoparquet files or not.
            Defaults to False.
        filter_osm_ids: (list[str], optional): List of OSM features ids to read from the file.
            Have to be in the form of 'node/<id>', 'way/<id>' or 'relation/<id>'.
            Defaults to an empty list.
        working_directory (Union[str, Path], optional): Directory where to save
            the parsed `*.parquet` files. Defaults to "files".
        osm_way_polygon_features_config (Union[OsmWayPolygonConfig, dict[str, Any]], optional):
            Config used to determine which closed way features are polygons.
            Modifications to this config left are left for experienced OSM users.
            Defaults to predefined "osm_way_polygon_features.json".

    Returns:
        gpd.GeoDataFrame: GeoDataFrame with OSM features.

    Examples:
        Get OSM data from a PBF file.

        Tags will be kept in a single column.
        >>> import quackosm as qosm
        >>> qosm.get_features_gdf(monaco_pbf_path).sort_index()
                                                      tags                      geometry
        feature_id
        node/10005045289                {'shop': 'bakery'}      POINT (7.42245 43.73105)
        node/10020887517  {'leisure': 'swimming_pool', ...      POINT (7.41316 43.73384)
        node/10021298117  {'leisure': 'swimming_pool', ...      POINT (7.42777 43.74277)
        node/10021298717  {'leisure': 'swimming_pool', ...      POINT (7.42630 43.74097)
        node/10025656383  {'ferry': 'yes', 'name': 'Qua...      POINT (7.42550 43.73690)
        ...                                            ...                           ...
        way/990669427     {'amenity': 'shelter', 'shelt...  POLYGON ((7.41461 43.7338...
        way/990669428     {'highway': 'secondary', 'jun...  LINESTRING (7.41366 43.73...
        way/990669429     {'highway': 'secondary', 'jun...  LINESTRING (7.41376 43.73...
        way/990848785     {'addr:city': 'Monaco', 'addr...  POLYGON ((7.41426 43.7339...
        way/993121275      {'building': 'yes', 'name': ...  POLYGON ((7.43214 43.7481...
        <BLANKLINE>
        [7906 rows x 2 columns]

        Get only buildings from a PBF file.

        Tags will be split into separate columns because of applying the filter.
        >>> qosm.get_features_gdf(
        ...     monaco_pbf_path, tags_filter={"building": True}
        ... ).sort_index()
                              building                                           geometry
        feature_id
        relation/11384697          yes  POLYGON ((7.42749 43.73125, 7.42672 43.73063, ...
        relation/11484092        hotel  POLYGON ((7.41790 43.72483, 7.41783 43.72486, ...
        relation/11484093   apartments  POLYGON ((7.41815 43.72561, 7.41836 43.72547, ...
        relation/11484094  residential  POLYGON ((7.41753 43.72583, 7.41753 43.72563, ...
        relation/11485520   apartments  POLYGON ((7.42071 43.73260, 7.42125 43.73260, ...
        ...                        ...                                                ...
        way/94452886        apartments  POLYGON ((7.43242 43.74761, 7.43242 43.74778, ...
        way/946074428              yes  POLYGON ((7.42235 43.74037, 7.42244 43.74045, ...
        way/952067351              yes  POLYGON ((7.42207 43.73434, 7.42211 43.73434, ...
        way/990848785              yes  POLYGON ((7.41426 43.73396, 7.41431 43.73402, ...
        way/993121275              yes  POLYGON ((7.43214 43.74813, 7.43216 43.74817, ...
        <BLANKLINE>
        [1283 rows x 2 columns]

        Get features for Malé - the capital city of Maldives

        Tags will be kept in a single column.
        >>> from shapely.geometry import box
        >>> qosm.get_features_gdf(
        ...     maldives_pbf_path,
        ...     geometry_filter=box(
        ...         minx=73.4975872,
        ...         miny=4.1663240,
        ...         maxx=73.5215528,
        ...         maxy=4.1818121
        ...     )
        ... ).sort_index()
                                                   tags                                     geometry
        feature_id
        node/10010180778  {'brand': 'Ooredoo', 'bran...                     POINT (73.51790 4.17521)
        node/10062500171  {'contact:facebook': 'http...                     POINT (73.50958 4.17245)
        node/10078084764  {'addr:city': 'Male'', 'ad...                     POINT (73.50480 4.17267)
        node/10078086040  {'addr:city': 'Malé', 'add...                     POINT (73.50317 4.17596)
        node/10158825718  {'addr:postcode': '20175',...                     POINT (73.50832 4.17301)
        ...                                         ...                                          ...
        way/91986255      {'landuse': 'cemetery', 'n...  POLYGON ((73.50751 4.17311, 73.50789 4.1...
        way/91986256      {'highway': 'residential',...  LINESTRING (73.51067 4.17448, 73.51082 4...
        way/935784864     {'layer': '-1', 'location'...    LINESTRING (73.48754 4.17033, 73.50075...
        way/935784867     {'layer': '-1', 'location'...  LINESTRING (73.44617 4.18567, 73.46094 4...
        way/959150179     {'amenity': 'place_of_wors...  POLYGON ((73.51841 4.17553, 73.51849 4.1...
        <BLANKLINE>
        [2140 rows x 2 columns]


        Get features grouped into catgegories for Christmas Island

        Even though we apply the filter, the tags will be kept in a single column
        because of manual `explode_tags` value setting.
        >>> qosm.get_features_gdf(
        ...     kiribati_pbf_path,
        ...     tags_filter={
        ...         "highway": {"highway": True},
        ...         "tree": {"natural": "tree"},
        ...         "building": {"building": True},
        ...     },
        ...     geometry_filter=box(
        ...         minx=-157.6046004,
        ...         miny=1.6724409,
        ...         maxx=-157.1379507,
        ...         maxy=2.075240
        ...     ),
        ...     explode_tags=False,
        ... ).sort_index()
                                                  tags                                      geometry
        feature_id
        node/2377661784  {'building': 'building=ruin'}                    POINT (-157.18826 1.75186)
        node/4150479646       {'tree': 'natural=tree'}                    POINT (-157.36152 1.98363)
        node/4396875565       {'tree': 'natural=tree'}                    POINT (-157.36143 1.98364)
        node/4396875566       {'tree': 'natural=tree'}                    POINT (-157.36135 1.98364)
        node/4396875567       {'tree': 'natural=tree'}                    POINT (-157.36141 1.98371)
        ...                                        ...                                           ...
        way/997441336     {'highway': 'highway=track'}  LINESTRING (-157.38083 1.77798, -157.3814...
        way/997441337     {'highway': 'highway=track'}  LINESTRING (-157.39796 1.79933, -157.3978...
        way/998103305      {'highway': 'highway=path'}  LINESTRING (-157.56048 1.87379, -157.5577...
        way/998103306     {'highway': 'highway=track'}  LINESTRING (-157.55513 1.86847, -157.5585...
        way/998370723      {'highway': 'highway=path'}  LINESTRING (-157.47069 1.83903, -157.4707...
        <BLANKLINE>
        [3109 rows x 2 columns]
    """
    return PbfFileReader(
        tags_filter=tags_filter,
        geometry_filter=geometry_filter,
        working_directory=working_directory,
        osm_way_polygon_features_config=osm_way_polygon_features_config,
    ).get_features_gdf(
        file_paths=file_paths,
        explode_tags=explode_tags,
        ignore_cache=ignore_cache,
        filter_osm_ids=filter_osm_ids,
    )
