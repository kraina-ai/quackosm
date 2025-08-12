"""CLI module for parsing pbf file to geoparquet."""

import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Optional, Union, cast

import click
import typer
from rq_geo_toolkit._geopandas_api_version import GEOPANDAS_NEW_API

from quackosm._constants import (
    PARQUET_COMPRESSION,
    PARQUET_COMPRESSION_LEVEL,
    PARQUET_ROW_GROUP_SIZE,
    PARQUET_VERSION,
)
from quackosm._osm_tags_filters import GroupedOsmTagsFilter, OsmTagsFilter
from quackosm.osm_extracts.extract import OsmExtractSource
from quackosm.pbf_file_reader import _is_url_path

if TYPE_CHECKING:
    from typing import Literal

    from quackosm._rich_progress import VERBOSITY_MODE

app = typer.Typer(context_settings={"help_option_names": ["-h", "--help"]}, rich_markup_mode="rich")


def _version_callback(value: bool) -> None:
    if value:
        from quackosm import __app_name__, __version__

        typer.echo(f"{__app_name__} {__version__}")
        raise typer.Exit()


def _display_osm_extracts_callback(ctx: typer.Context, value: bool) -> None:
    if value:
        from quackosm.osm_extracts import display_available_extracts

        param_values = {p.name: p.default for p in ctx.command.params}
        param_values.update(ctx.params)
        osm_source = cast("str", param_values.get("osm_extract_source"))
        display_available_extracts(source=osm_source, use_full_names=True, use_pager=True)
        raise typer.Exit()


def _path_callback(ctx: typer.Context, value: Path) -> Path:
    if not _is_url_path(value) and not Path(value).exists():
        raise typer.BadParameter(f"File not found error: {value}")
    return value


def _empty_path_callback(ctx: typer.Context, value: Path) -> Optional[Path]:
    if not value:
        return None
    return _path_callback(ctx, value)


class BboxGeometryParser(click.ParamType):  # type: ignore
    """Parser for geometry in WKT form."""

    name = "BBOX"

    def convert(self, value, param=None, ctx=None):  # type: ignore
        """Convert parameter value."""
        try:
            from shapely import box

            bbox_values = [float(x.strip()) for x in value.split(",")]
            return box(*bbox_values)
        except ValueError:  # ValueError raised when passing non-numbers to float()
            raise typer.BadParameter(
                "Cannot parse provided bounding box."
                " Valid value must contain 4 floating point numbers"
                " separated by commas."
            ) from None


class WktGeometryParser(click.ParamType):  # type: ignore
    """Parser for geometry in WKT form."""

    name = "TEXT (WKT)"

    def convert(self, value, param=None, ctx=None):  # type: ignore
        """Convert parameter value."""
        if not value:
            return None
        try:
            from shapely import from_wkt

            return from_wkt(value)
        except Exception:
            raise typer.BadParameter("Cannot parse provided WKT") from None


class GeoJsonGeometryParser(click.ParamType):  # type: ignore
    """Parser for geometry in GeoJSON form."""

    name = "TEXT (GeoJSON)"

    def convert(self, value, param=None, ctx=None):  # type: ignore
        """Convert parameter value."""
        if not value:
            return None
        try:
            from shapely import from_geojson

            return from_geojson(value)
        except Exception:
            raise typer.BadParameter("Cannot parse provided GeoJSON") from None


class GeoFileGeometryParser(click.ParamType):  # type: ignore
    """Parser for geometry in geo file form."""

    name = "PATH"

    def convert(self, value, param=None, ctx=None):  # type: ignore
        """Convert parameter value."""
        if not value:
            return None

        value = _path_callback(ctx=ctx, value=value)

        try:
            import geopandas as gpd

            gdf = gpd.read_file(value)
            if GEOPANDAS_NEW_API:
                return gdf.union_all()
            else:
                return gdf.unary_union
        except Exception:
            raise typer.BadParameter("Cannot parse provided geo file") from None


class GeocodeGeometryParser(click.ParamType):  # type: ignore
    """Parser for geometry in string Nominatim query form."""

    name = "TEXT"

    def convert(self, value, param=None, ctx=None):  # type: ignore
        """Convert parameter value."""
        if not value:
            return None

        try:
            from quackosm.geocode import geocode_to_geometry

            return geocode_to_geometry(value)
        except Exception:
            raise typer.BadParameter("Cannot geocode provided Nominatim query") from None


class GeohashGeometryParser(click.ParamType):  # type: ignore
    """Parser for geometry in string Nominatim query form."""

    name = "TEXT (Geohash)"

    def convert(self, value, param=None, ctx=None):  # type: ignore
        """Convert parameter value."""
        if not value:
            return None

        try:
            import geopandas as gpd
            from shapely.geometry import box

            from quackosm._geohash_parser import geohash_bounds

            geometries = []
            for geohash in value.split(","):
                bounds = geohash_bounds(geohash.strip())
                geometries.append(box(*bounds))
            if GEOPANDAS_NEW_API:
                return gpd.GeoSeries(geometries).union_all()
            else:
                return gpd.GeoSeries(geometries).unary_union
        except Exception:
            raise


class H3GeometryParser(click.ParamType):  # type: ignore
    """Parser for geometry in string Nominatim query form."""

    name = "TEXT (H3)"

    def convert(self, value, param=None, ctx=None):  # type: ignore
        """Convert parameter value."""
        if not value:
            return None

        import duckdb
        import geopandas as gpd
        from shapely import from_wkt

        duckdb.install_extension("h3", repository="community")
        duckdb.load_extension("h3")

        geometries = []  # noqa: FURB138
        for h3_index in value.split(","):
            stripped_h3_index = h3_index.strip()
            if not duckdb.sql(f"SELECT h3_is_valid_cell('{stripped_h3_index}')").fetchone()[0]:
                raise typer.BadParameter(
                    f"Cannot parse provided H3 value: {stripped_h3_index}"
                ) from None

            parsed_geometry = from_wkt(
                duckdb.sql(f"SELECT h3_cell_to_boundary_wkt('{stripped_h3_index}')").fetchone()[0]
            )

            geometries.append(parsed_geometry)

        if not GEOPANDAS_NEW_API:
            return gpd.GeoSeries(geometries).unary_union

        return gpd.GeoSeries(geometries).union_all()


class S2GeometryParser(click.ParamType):  # type: ignore
    """Parser for geometry in string Nominatim query form."""

    name = "TEXT (S2)"

    def convert(self, value, param=None, ctx=None):  # type: ignore
        """Convert parameter value."""
        if not value:
            return None

        import geopandas as gpd
        import s2sphere
        from shapely import Polygon

        geometries = []  # noqa: FURB138
        for s2_index in value.split(","):
            stripped_s2_index = s2_index.strip()
            try:
                s2_cell = s2sphere.Cell(s2sphere.CellId.from_token(stripped_s2_index))
                points = [
                    s2sphere.LatLng.from_point(s2_cell.get_vertex(i)) for i in (0, 1, 2, 3, 0)
                ]
                geometries.append(
                    Polygon([[point.lng().degrees, point.lat().degrees] for point in points])
                )
            except Exception:
                raise typer.BadParameter(
                    f"Cannot parse provided S2 value: {stripped_s2_index}"
                ) from None

        if not GEOPANDAS_NEW_API:
            return gpd.GeoSeries(geometries).unary_union

        return gpd.GeoSeries(geometries).union_all()


class OsmTagsFilterJsonParser(click.ParamType):  # type: ignore
    """Parser for OSM tags filter in JSON form."""

    name = "TEXT (JSON)"

    def convert(self, value, param=None, ctx=None):  # type: ignore
        """Convert parameter value."""
        if not value:
            return None
        try:
            parsed_dict = json.loads(value)
        except Exception:
            raise typer.BadParameter("Cannot parse provided OSM tags filter") from None

        from quackosm._typing import is_expected_type

        if not is_expected_type(parsed_dict, OsmTagsFilter) and not is_expected_type(
            parsed_dict, GroupedOsmTagsFilter
        ):
            raise typer.BadParameter(
                "Provided OSM tags filter is not in a required format."
            ) from None

        return cast("Union[OsmTagsFilter, GroupedOsmTagsFilter]", parsed_dict)


class OsmTagsFilterFileParser(OsmTagsFilterJsonParser):
    """Parser for OSM tags filter in file form."""

    name = "PATH"

    def convert(self, value, param=None, ctx=None):  # type: ignore
        """Convert parameter value."""
        if not value:
            return None

        value = _path_callback(ctx=ctx, value=value)

        return super().convert(Path(value).read_text(), param, ctx)  # type: ignore


def _filter_osm_ids_callback(value: str) -> Optional[list[str]]:
    if not value:
        return None

    import re

    osm_ids = value.split(",")
    matcher = re.compile(r"^(node|way|relation)\/\d*$")
    parsed_osm_ids = []
    for osm_id in osm_ids:
        stripped_osm_id = osm_id.strip()
        if not stripped_osm_id.startswith(("node/", "way/", "relation/")) or not matcher.match(
            stripped_osm_id
        ):
            raise typer.BadParameter(f"Cannot parse provided OSM id: {stripped_osm_id}") from None
        parsed_osm_ids.append(stripped_osm_id)

    return parsed_osm_ids


@app.command()  # type: ignore
def main(
    pbf_file: Annotated[
        Optional[str],
        typer.Argument(
            help="PBF file to convert into GeoParquet. Can be an URL.",
            metavar="PBF file path",
            callback=_empty_path_callback,
            show_default=False,
        ),
    ] = None,
    osm_tags_filter: Annotated[
        Optional[str],
        typer.Option(
            help=(
                "OSM tags used to filter the data in the "
                "[bold dark_orange]JSON text[/bold dark_orange] form."
                " Can take the form of a flat or grouped dict "
                "(look: [bold green]OsmTagsFilter[/bold green]"
                " and [bold green]GroupedOsmTagsFilter[/bold green])."
                " Cannot be used together with"
                " [bold bright_cyan]osm-tags-filter-file[/bold bright_cyan]."
            ),
            click_type=OsmTagsFilterJsonParser(),
            show_default=False,
        ),
    ] = None,
    osm_tags_filter_file: Annotated[
        Optional[str],
        typer.Option(
            help=(
                "OSM tags used to filter the data in the "
                "[bold dark_orange]JSON file[/bold dark_orange] form."
                " Can take the form of a flat or grouped dict "
                "(look: [bold green]OsmTagsFilter[/bold green]"
                " and [bold green]GroupedOsmTagsFilter[/bold green])."
                " Cannot be used together with"
                " [bold bright_cyan]osm-tags-filter[/bold bright_cyan]."
            ),
            click_type=OsmTagsFilterFileParser(),
            show_default=False,
        ),
    ] = None,
    keep_all_tags: Annotated[
        bool,
        typer.Option(
            "--keep-all-tags/",
            "--all-tags/",
            help=(
                "Whether to keep all tags while filtering with OSM tags."
                " Doesn't work when there is no OSM tags filter applied"
                " ([bold bright_cyan]osm-tags-filter[/bold bright_cyan]"
                " or [bold bright_cyan]osm-tags-filter-file[/bold bright_cyan])."
                " Will override grouping if [bold green]GroupedOsmTagsFilter[/bold green]"
                " has been passed as a filter."
            ),
            show_default=False,
        ),
    ] = False,
    geom_filter_bbox: Annotated[
        Optional[str],
        typer.Option(
            help=(
                "Geometry to use as a filter in the"
                " [bold dark_orange]bounding box[/bold dark_orange] format - 4 floating point"
                " numbers separated by commas."
                " Cannot be used together with other"
                " [bold bright_cyan]geom-filter-...[/bold bright_cyan] parameters."
            ),
            click_type=BboxGeometryParser(),
            show_default=False,
        ),
    ] = None,
    geom_filter_file: Annotated[
        Optional[str],
        typer.Option(
            help=(
                "Geometry to use as a filter in the"
                " [bold dark_orange]file[/bold dark_orange] format - any that can be opened by"
                " GeoPandas. Will return the unary union of the geometries in the file."
                " Cannot be used together with other"
                " [bold bright_cyan]geom-filter-...[/bold bright_cyan] parameters."
            ),
            click_type=GeoFileGeometryParser(),
            show_default=False,
        ),
    ] = None,
    geom_filter_geocode: Annotated[
        Optional[str],
        typer.Option(
            help=(
                "Geometry to use as a filter in the"
                " [bold dark_orange]string to geocode[/bold dark_orange] format - it will be"
                " geocoded to the geometry using Nominatim API (GeoPy library)."
                " Cannot be used together with other"
                " [bold bright_cyan]geom-filter-...[/bold bright_cyan] parameters."
            ),
            click_type=GeocodeGeometryParser(),
            show_default=False,
        ),
    ] = None,
    geom_filter_geojson: Annotated[
        Optional[str],
        typer.Option(
            help=(
                "Geometry to use as a filter in the [bold dark_orange]GeoJSON[/bold dark_orange]"
                " format."
                " Cannot be used together with other"
                " [bold bright_cyan]geom-filter-...[/bold bright_cyan] parameters."
            ),
            click_type=GeoJsonGeometryParser(),
            show_default=False,
        ),
    ] = None,
    geom_filter_index_geohash: Annotated[
        Optional[str],
        typer.Option(
            help=(
                "Geometry to use as a filter in the"
                " [bold dark_orange]Geohash index[/bold dark_orange]"
                " format. Separate multiple values with a comma."
                " Cannot be used together with other"
                " [bold bright_cyan]geom-filter-...[/bold bright_cyan] parameters."
            ),
            click_type=GeohashGeometryParser(),
            show_default=False,
        ),
    ] = None,
    geom_filter_index_h3: Annotated[
        Optional[str],
        typer.Option(
            help=(
                "Geometry to use as a filter in the [bold dark_orange]H3 index[/bold dark_orange]"
                " format. Separate multiple values with a comma."
                " Cannot be used together with other"
                " [bold bright_cyan]geom-filter-...[/bold bright_cyan] parameters."
            ),
            click_type=H3GeometryParser(),
            show_default=False,
        ),
    ] = None,
    geom_filter_index_s2: Annotated[
        Optional[str],
        typer.Option(
            help=(
                "Geometry to use as a filter in the [bold dark_orange]S2 index[/bold dark_orange]"
                " format. Separate multiple values with a comma."
                " Cannot be used together with other"
                " [bold bright_cyan]geom-filter-...[/bold bright_cyan] parameters."
            ),
            click_type=S2GeometryParser(),
            show_default=False,
        ),
    ] = None,
    geom_filter_wkt: Annotated[
        Optional[str],
        typer.Option(
            help=(
                "Geometry to use as a filter in the [bold dark_orange]WKT[/bold dark_orange]"
                " format."
                " Cannot be used together with other"
                " [bold bright_cyan]geom-filter-...[/bold bright_cyan] parameters."
            ),
            click_type=WktGeometryParser(),
            show_default=False,
        ),
    ] = None,
    custom_sql_filter: Annotated[
        Optional[str],
        typer.Option(
            help=(
                "Allows users to pass custom SQL conditions used to filter OSM features. "
                "It will be embedded into predefined queries and requires DuckDB syntax to operate "
                "on tags map object."
            ),
            case_sensitive=False,
            show_default=False,
        ),
    ] = None,
    osm_extract_query: Annotated[
        Optional[str],
        typer.Option(
            help=(
                "Query to find an OpenStreetMap extract from available sources. "
                "Will automatically find and download OSM extract. "
                "Can be used instead of [bold yellow]PBF file path[/bold yellow] argument."
            ),
            case_sensitive=False,
            show_default=False,
        ),
    ] = None,
    osm_extract_source: Annotated[
        OsmExtractSource,
        typer.Option(
            "--osm-extract-source",
            "--pbf-download-source",
            help=(
                "Source where to download the PBF file from."
                " Can be Geofabrik, BBBike, OSMfr (OpenStreetMap.fr) or any."
            ),
            case_sensitive=False,
            show_default="any",
            is_eager=True,
        ),
    ] = OsmExtractSource.any,
    explode_tags: Annotated[
        Optional[bool],
        typer.Option(
            "--explode-tags/--compact-tags",
            "--explode/--compact",
            help=(
                "Whether to split tags into columns based on the OSM tag keys."
                " If [bold violet]None[/bold violet], it will be set based on"
                " the [bold bright_cyan]osm-tags-filter[/bold bright_cyan]"
                "/[bold bright_cyan]osm-tags-filter-file[/bold bright_cyan]"
                " and [bold bright_cyan]keep-all-tags[/bold bright_cyan] parameters."
                " If there is a tags filter applied without"
                " [bold bright_cyan]keep-all-tags[/bold bright_cyan] then it'll be set to"
                " [bold bright_cyan]explode-tags[/bold bright_cyan]"
                " ([bold green]True[/bold green])."
                " Otherwise it'll be set to [bold magenta]compact-tags[/bold magenta]"
                " ([bold red]False[/bold red])."
            ),
            show_default=None,
        ),
    ] = None,
    result_file_path: Annotated[
        Optional[Path],
        typer.Option(
            "--output",
            "-o",
            help=(
                "Path where to save final result file. If not provided, it will be generated"
                " automatically based on the input pbf file name."
                " Can be [bold green].parquet[/bold green] or"
                " [bold green].db[/bold green] or [bold green].duckdb[/bold green] extension."
            ),
            show_default=False,
        ),
    ] = None,
    duckdb: Annotated[
        bool,
        typer.Option(
            "--duckdb",
            help=(
                "Export to duckdb database. If not provided, data can still be exported if"
                " [bold bright_cyan]output[/bold bright_cyan] has [bold green].db[/bold green]"
                " or [bold green].duckdb[/bold green] extension."
            ),
        ),
    ] = False,
    duckdb_table_name: Annotated[
        Optional[str],
        typer.Option(
            "--duckdb-table-name",
            help="Table name which the data will be imported into in the DuckDB database.",
        ),
    ] = "quackosm",
    compression: Annotated[
        str,
        typer.Option(
            "--compression",
            help="Compression of the final parquet file.",
            show_default=True,
        ),
    ] = PARQUET_COMPRESSION,
    compression_level: Annotated[
        int,
        typer.Option(
            "--compression-level",
            help=(
                "Compression level of the final parquet file. Supported only for zstd compression."
            ),
            show_default=True,
        ),
    ] = PARQUET_COMPRESSION_LEVEL,
    row_group_size: Annotated[
        int,
        typer.Option(
            "--row-group-size",
            help="Approximate number of rows per row group in the final parquet file.",
            show_default=True,
        ),
    ] = PARQUET_ROW_GROUP_SIZE,
    parquet_version: Annotated[
        str,
        typer.Option(
            "--parquet-version",
            help="Type of parquet version used to save final file. Supported options: v1 and v2.",
            show_default=True,
        ),
    ] = PARQUET_VERSION,
    ignore_cache: Annotated[
        bool,
        typer.Option(
            "--ignore-cache/",
            "--no-cache/",
            help="Whether to ignore previously precalculated geoparquet files or not.",
            show_default=False,
        ),
    ] = False,
    working_directory: Annotated[
        Path,
        typer.Option(
            "--working-directory",
            "--work-dir",
            help=(
                "Directory where to save the parsed parquet and geoparquet files."
                " Will be created if doesn't exist."
            ),
        ),
    ] = "files",  # type: ignore
    osm_way_polygon_features_config: Annotated[
        Optional[Path],
        typer.Option(
            "--osm-way-polygon-config",
            help=(
                "Config where alternative OSM way polygon features config is defined."
                " Will determine how to parse way features based on tags."
                " Option is intended for experienced users. It's recommended to disable"
                " cache ([bold bright_cyan]no-cache[/bold bright_cyan]) when using this option,"
                " since file names don't contain information what config file has been used"
                " for file generation."
            ),
            callback=_empty_path_callback,
            show_default=False,
        ),
    ] = None,
    filter_osm_ids: Annotated[
        Optional[str],
        typer.Option(
            "--filter-osm-ids",
            help=(
                "List of OSM features IDs to read from the file."
                " Have to be in the form of 'node/<id>', 'way/<id>' or 'relation/<id>'."
                " Separate multiple values with a comma."
            ),
            callback=_filter_osm_ids_callback,
            show_default=False,
        ),
    ] = None,
    sort_result: Annotated[
        bool,
        typer.Option(
            "--sort/--no-sort",
            help="Whether to sort the final geoparquet file by geometry or not.",
            show_default=True,
        ),
    ] = True,
    ignore_metadata_tags: Annotated[
        bool,
        typer.Option(
            "--ignore-metadata-tags/--keep-metadata-tags",
            help="Whether to remove metadata tags, based on the default GDAL config.",
            show_default=True,
        ),
    ] = True,
    wkt_result: Annotated[
        bool,
        typer.Option(
            "--wkt-result/",
            "--wkt/",
            help="Whether to save the geometry as a WKT string instead of WKB blob.",
            show_default=False,
        ),
    ] = False,
    silent_mode: Annotated[
        bool,
        typer.Option(
            "--silent/",
            help="Whether to disable progress reporting.",
            show_default=False,
        ),
    ] = False,
    transient_mode: Annotated[
        bool,
        typer.Option(
            "--transient/",
            help="Whether to make more transient (concise) progress reporting.",
            show_default=False,
        ),
    ] = False,
    geometry_coverage_iou_threshold: Annotated[
        float,
        typer.Option(
            "--iou-threshold",
            help=(
                "Minimal value of the Intersection over Union metric for selecting the matching OSM"
                " extracts. Is best matching extract has value lower than the threshold, it is"
                " discarded (except the first one). Has to be in range between 0 and 1."
                " Value of 0 will allow every intersected extract, value of 1 will only allow"
                " extracts that match the geometry exactly. Works only when PbfFileReader is asked"
                " to download OSM extracts automatically."
            ),
            show_default=0.01,
            min=0,
            max=1,
        ),
    ] = 0.01,
    allow_uncovered_geometry: Annotated[
        bool,
        typer.Option(
            "--allow-uncovered-geometry/",
            help=(
                "Suppresses an error if some geometry parts aren't covered by any OSM extract."
                " Works only when PbfFileReader is asked to download OSM extracts automatically."
            ),
            show_default=False,
        ),
    ] = False,
    cpu_limit: Annotated[
        Optional[int],
        typer.Option(
            "--cpu-limit",
            help=(
                "Max number of threads available for processing."
                " By default, will use all available threads."
            ),
            show_default=False,
        ),
    ] = None,
    show_extracts: Annotated[
        Optional[bool],
        typer.Option(
            "--show-extracts",
            "--show-osm-extracts",
            help="Show available OSM extracts and exit.",
            callback=_display_osm_extracts_callback,
            is_eager=False,
        ),
    ] = None,
    version: Annotated[
        Optional[bool],
        typer.Option(
            "--version",
            "-v",
            help="Show the application's version and exit.",
            callback=_version_callback,
            is_eager=True,
        ),
    ] = None,
) -> None:
    """
    QuackOSM CLI.

    Wraps convert_pbf_to_parquet, convert_geometry_to_parquet and convert_osm_extract_to_parquet
    functions and prints final path to the saved geoparquet file at the end.
    """
    if parquet_version not in ("v1", "v2"):
        raise typer.BadParameter(
            f"Provided incompatible parquet_version ({parquet_version}). Valid options: v1 and v2."
        )
    parquet_version = cast('Literal["v1", "v2"]', parquet_version)

    number_of_geometries_provided = sum(
        geom is not None
        for geom in (
            geom_filter_bbox,
            geom_filter_file,
            geom_filter_geocode,
            geom_filter_geojson,
            geom_filter_index_geohash,
            geom_filter_index_h3,
            geom_filter_index_s2,
            geom_filter_wkt,
        )
    )
    if number_of_geometries_provided > 1:
        raise typer.BadParameter("Provided more than one geometry for filtering")

    geometry_filter_value = (
        geom_filter_bbox
        or geom_filter_file
        or geom_filter_geocode
        or geom_filter_geojson
        or geom_filter_index_geohash
        or geom_filter_index_h3
        or geom_filter_index_s2
        or geom_filter_wkt
    )

    if pbf_file is osm_extract_query is geometry_filter_value is None:
        from click import Argument
        from click.exceptions import MissingParameter

        raise MissingParameter(
            message=(
                "QuackOSM requires either the path to the pbf file,"
                " an OSM extract query (--osm-extract-query) or a geometry filter"
                " (one of --geom-filter-bbox, --geom-filter-file, --geom-filter-geocode,"
                " --geom-filter-geojson, --geom-filter-index-geohash,"
                " --geom-filter-index-h3, --geom-filter-index-s2, --geom-filter-wkt)"
                " to download the file automatically. All three cannot be empty at once."
            ),
            param=Argument(["pbf_file"], type=Path, metavar="PBF file path"),
        )

    if osm_tags_filter is not None and osm_tags_filter_file is not None:
        raise typer.BadParameter("Provided more than one osm tags filter parameter")

    if transient_mode and silent_mode:
        raise typer.BadParameter("Cannot pass both silent and transient mode at once.")

    verbosity_mode: VERBOSITY_MODE = "verbose"

    if transient_mode:
        verbosity_mode = "transient"
    elif silent_mode:
        verbosity_mode = "silent"

    if wkt_result and sort_result:
        sort_result = False

    logging.disable(logging.CRITICAL)

    is_duckdb = (result_file_path and result_file_path.suffix in (".duckdb", ".db")) or duckdb

    pbf_file_parquet = pbf_file and not is_duckdb
    pbf_file_duckdb = pbf_file and is_duckdb
    osm_extract_parquet = osm_extract_query and not is_duckdb
    osm_extract_duckdb = osm_extract_query and is_duckdb
    geometry_parquet = not pbf_file and not osm_extract_query and not is_duckdb
    geometry_duckdb = not pbf_file and not osm_extract_query and is_duckdb

    if pbf_file_parquet:
        from quackosm.functions import convert_pbf_to_parquet

        result_path = convert_pbf_to_parquet(
            pbf_path=cast("str", pbf_file),
            tags_filter=osm_tags_filter or osm_tags_filter_file,  # type: ignore
            keep_all_tags=keep_all_tags,
            geometry_filter=geometry_filter_value,
            explode_tags=explode_tags,
            ignore_cache=ignore_cache,
            working_directory=working_directory,
            result_file_path=result_file_path,
            compression=compression,
            compression_level=compression_level,
            row_group_size=row_group_size,
            parquet_version=parquet_version,
            osm_way_polygon_features_config=(
                json.loads(Path(osm_way_polygon_features_config).read_text())
                if osm_way_polygon_features_config
                else None
            ),
            ignore_metadata_tags=ignore_metadata_tags,
            filter_osm_ids=filter_osm_ids,  # type: ignore
            custom_sql_filter=custom_sql_filter,
            sort_result=sort_result,
            save_as_wkt=wkt_result,
            verbosity_mode=verbosity_mode,
            cpu_limit=cpu_limit,
        )
    elif pbf_file_duckdb:
        from quackosm.functions import convert_pbf_to_duckdb

        result_path = convert_pbf_to_duckdb(
            pbf_path=cast("str", pbf_file),
            tags_filter=osm_tags_filter or osm_tags_filter_file,  # type: ignore
            keep_all_tags=keep_all_tags,
            geometry_filter=geometry_filter_value,
            explode_tags=explode_tags,
            ignore_cache=ignore_cache,
            working_directory=working_directory,
            result_file_path=result_file_path,
            compression=compression,
            compression_level=compression_level,
            row_group_size=row_group_size,
            parquet_version=parquet_version,
            osm_way_polygon_features_config=(
                json.loads(Path(osm_way_polygon_features_config).read_text())
                if osm_way_polygon_features_config
                else None
            ),
            ignore_metadata_tags=ignore_metadata_tags,
            filter_osm_ids=filter_osm_ids,  # type: ignore
            custom_sql_filter=custom_sql_filter,
            sort_result=sort_result,
            duckdb_table_name=duckdb_table_name or "quackosm",
            verbosity_mode=verbosity_mode,
            cpu_limit=cpu_limit,
        )
    elif osm_extract_parquet:
        from quackosm._exceptions import OsmExtractSearchError
        from quackosm.functions import convert_osm_extract_to_parquet

        try:
            result_path = convert_osm_extract_to_parquet(
                osm_extract_query=cast("str", osm_extract_query),
                osm_extract_source=osm_extract_source,
                tags_filter=osm_tags_filter or osm_tags_filter_file,  # type: ignore
                keep_all_tags=keep_all_tags,
                geometry_filter=geometry_filter_value,
                explode_tags=explode_tags,
                ignore_cache=ignore_cache,
                working_directory=working_directory,
                result_file_path=result_file_path,
                compression=compression,
                compression_level=compression_level,
                row_group_size=row_group_size,
                parquet_version=parquet_version,
                osm_way_polygon_features_config=(
                    json.loads(Path(osm_way_polygon_features_config).read_text())
                    if osm_way_polygon_features_config
                    else None
                ),
                ignore_metadata_tags=ignore_metadata_tags,
                filter_osm_ids=filter_osm_ids,  # type: ignore
                custom_sql_filter=custom_sql_filter,
                sort_result=sort_result,
                save_as_wkt=wkt_result,
                verbosity_mode=verbosity_mode,
                cpu_limit=cpu_limit,
            )
        except OsmExtractSearchError as ex:
            from rich.console import Console

            err_console = Console(stderr=True)
            err_console.print(ex)
            raise typer.Exit(code=1) from None
    elif osm_extract_duckdb:
        from quackosm._exceptions import OsmExtractSearchError
        from quackosm.functions import convert_osm_extract_to_duckdb

        try:
            result_path = convert_osm_extract_to_duckdb(
                osm_extract_query=cast("str", osm_extract_query),
                osm_extract_source=osm_extract_source,
                tags_filter=osm_tags_filter or osm_tags_filter_file,  # type: ignore
                keep_all_tags=keep_all_tags,
                geometry_filter=geometry_filter_value,
                explode_tags=explode_tags,
                ignore_cache=ignore_cache,
                working_directory=working_directory,
                result_file_path=result_file_path,
                compression=compression,
                compression_level=compression_level,
                row_group_size=row_group_size,
                parquet_version=parquet_version,
                osm_way_polygon_features_config=(
                    json.loads(Path(osm_way_polygon_features_config).read_text())
                    if osm_way_polygon_features_config
                    else None
                ),
                ignore_metadata_tags=ignore_metadata_tags,
                filter_osm_ids=filter_osm_ids,  # type: ignore
                custom_sql_filter=custom_sql_filter,
                sort_result=sort_result,
                duckdb_table_name=duckdb_table_name or "quackosm",
                save_as_wkt=wkt_result,
                verbosity_mode=verbosity_mode,
                cpu_limit=cpu_limit,
            )
        except OsmExtractSearchError as ex:
            from rich.console import Console

            err_console = Console(stderr=True)
            err_console.print(ex)
            raise typer.Exit(code=1) from None
    elif geometry_parquet:
        from quackosm.functions import convert_geometry_to_parquet

        result_path = convert_geometry_to_parquet(
            geometry_filter=geometry_filter_value,
            osm_extract_source=osm_extract_source,
            tags_filter=osm_tags_filter or osm_tags_filter_file,  # type: ignore
            keep_all_tags=keep_all_tags,
            explode_tags=explode_tags,
            ignore_cache=ignore_cache,
            working_directory=working_directory,
            result_file_path=result_file_path,
            compression=compression,
            compression_level=compression_level,
            row_group_size=row_group_size,
            parquet_version=parquet_version,
            osm_way_polygon_features_config=(
                json.loads(Path(osm_way_polygon_features_config).read_text())
                if osm_way_polygon_features_config
                else None
            ),
            ignore_metadata_tags=ignore_metadata_tags,
            filter_osm_ids=filter_osm_ids,  # type: ignore
            custom_sql_filter=custom_sql_filter,
            sort_result=sort_result,
            save_as_wkt=wkt_result,
            verbosity_mode=verbosity_mode,
            geometry_coverage_iou_threshold=geometry_coverage_iou_threshold,
            allow_uncovered_geometry=allow_uncovered_geometry,
            cpu_limit=cpu_limit,
        )
    elif geometry_duckdb:
        from quackosm.functions import convert_geometry_to_duckdb

        result_path = convert_geometry_to_duckdb(
            geometry_filter=geometry_filter_value,
            osm_extract_source=osm_extract_source,
            tags_filter=osm_tags_filter or osm_tags_filter_file,  # type: ignore
            keep_all_tags=keep_all_tags,
            explode_tags=explode_tags,
            ignore_cache=ignore_cache,
            working_directory=working_directory,
            result_file_path=result_file_path,
            compression=compression,
            compression_level=compression_level,
            row_group_size=row_group_size,
            parquet_version=parquet_version,
            osm_way_polygon_features_config=(
                json.loads(Path(osm_way_polygon_features_config).read_text())
                if osm_way_polygon_features_config
                else None
            ),
            ignore_metadata_tags=ignore_metadata_tags,
            filter_osm_ids=filter_osm_ids,  # type: ignore
            custom_sql_filter=custom_sql_filter,
            duckdb_table_name=duckdb_table_name or "quackosm",
            sort_result=sort_result,
            save_as_wkt=wkt_result,
            verbosity_mode=verbosity_mode,
            geometry_coverage_iou_threshold=geometry_coverage_iou_threshold,
            allow_uncovered_geometry=allow_uncovered_geometry,
            cpu_limit=cpu_limit,
        )
    else:
        raise RuntimeError("Unknown operation mode")

    typer.secho(result_path, fg="green")
