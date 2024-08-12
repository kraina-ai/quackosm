"""CLI module for parsing pbf file to geoparquet."""

import json
import logging
from pathlib import Path
from typing import Annotated, Literal, Optional, Union, cast

import click
import typer

from quackosm._osm_tags_filters import GroupedOsmTagsFilter, OsmTagsFilter
from quackosm.osm_extracts.extract import OsmExtractSource
from quackosm.pbf_file_reader import _is_url_path

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
        osm_source = cast(str, param_values.get("osm_extract_source"))
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
            from geohash import bbox as geohash_bbox
            from shapely.geometry import box

            geometries = []
            for geohash in value.split(","):
                bounds = geohash_bbox(geohash.strip())
                geometries.append(
                    box(minx=bounds["w"], miny=bounds["s"], maxx=bounds["e"], maxy=bounds["n"])
                )
            return gpd.GeoSeries(geometries).unary_union
        except Exception:
            raise typer.BadParameter(f"Cannot parse provided Geohash value: {geohash}") from None


class H3GeometryParser(click.ParamType):  # type: ignore
    """Parser for geometry in string Nominatim query form."""

    name = "TEXT (H3)"

    def convert(self, value, param=None, ctx=None):  # type: ignore
        """Convert parameter value."""
        if not value:
            return None

        try:
            import geopandas as gpd
            import h3
            from shapely.geometry import Polygon

            geometries = []  # noqa: FURB138
            for h3_cell in value.split(","):
                geometries.append(
                    Polygon([coords[::-1] for coords in h3.cell_to_boundary(h3_cell.strip())])
                )
            return gpd.GeoSeries(geometries).unary_union
        except Exception as ex:
            raise typer.BadParameter(f"Cannot parse provided H3 values: {value}") from ex


class S2GeometryParser(click.ParamType):  # type: ignore
    """Parser for geometry in string Nominatim query form."""

    name = "TEXT (S2)"

    def convert(self, value, param=None, ctx=None):  # type: ignore
        """Convert parameter value."""
        if not value:
            return None

        try:
            import geopandas as gpd
            from s2 import s2
            from shapely.geometry import Polygon

            geometries = []  # noqa: FURB138
            for s2_index in value.split(","):
                geometries.append(
                    Polygon(s2.s2_to_geo_boundary(s2_index.strip(), geo_json_conformant=True))
                )
            return gpd.GeoSeries(geometries).unary_union
        except Exception:
            raise typer.BadParameter(f"Cannot parse provided S2 value: {s2_index}") from None


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

        return cast(Union[OsmTagsFilter, GroupedOsmTagsFilter], parsed_dict)


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
    geom_filter_file: Annotated[
        Optional[str],
        typer.Option(
            help=(
                "Geometry to use as a filter in the"
                " [bold dark_orange]file[/bold dark_orange] format - any that can be opened by"
                " GeoPandas. Will return the unary union of the geometries in the file."
                " Cannot be used together with"
                " [bold bright_cyan]geom-filter-geocode[/bold bright_cyan] or"
                " [bold bright_cyan]geom-filter-geojson[/bold bright_cyan] or"
                " [bold bright_cyan]geom-filter-index-geohash[/bold bright_cyan] or"
                " [bold bright_cyan]geom-filter-index-h3[/bold bright_cyan] or"
                " [bold bright_cyan]geom-filter-index-s2[/bold bright_cyan] or"
                " [bold bright_cyan]geom-filter-wkt[/bold bright_cyan]."
            ),
            click_type=GeoFileGeometryParser(),
        ),
    ] = None,
    geom_filter_geocode: Annotated[
        Optional[str],
        typer.Option(
            help=(
                "Geometry to use as a filter in the"
                " [bold dark_orange]string to geocode[/bold dark_orange] format - it will be"
                " geocoded to the geometry using Nominatim API (GeoPy library)."
                " Cannot be used together with"
                " [bold bright_cyan]geom-filter-file[/bold bright_cyan] or"
                " [bold bright_cyan]geom-filter-geojson[/bold bright_cyan] or"
                " [bold bright_cyan]geom-filter-index-geohash[/bold bright_cyan] or"
                " [bold bright_cyan]geom-filter-index-h3[/bold bright_cyan] or"
                " [bold bright_cyan]geom-filter-index-s2[/bold bright_cyan] or"
                " [bold bright_cyan]geom-filter-wkt[/bold bright_cyan]."
            ),
            click_type=GeocodeGeometryParser(),
        ),
    ] = None,
    geom_filter_geojson: Annotated[
        Optional[str],
        typer.Option(
            help=(
                "Geometry to use as a filter in the [bold dark_orange]GeoJSON[/bold dark_orange]"
                " format."
                " Cannot be used used together with"
                " [bold bright_cyan]geom-filter-file[/bold bright_cyan] or"
                " [bold bright_cyan]geom-filter-geocode[/bold bright_cyan] or"
                " [bold bright_cyan]geom-filter-index-geohash[/bold bright_cyan] or"
                " [bold bright_cyan]geom-filter-index-h3[/bold bright_cyan] or"
                " [bold bright_cyan]geom-filter-index-s2[/bold bright_cyan] or"
                " [bold bright_cyan]geom-filter-wkt[/bold bright_cyan]."
            ),
            click_type=GeoJsonGeometryParser(),
        ),
    ] = None,
    geom_filter_index_geohash: Annotated[
        Optional[str],
        typer.Option(
            help=(
                "Geometry to use as a filter in the"
                " [bold dark_orange]Geohash index[/bold dark_orange]"
                " format. Separate multiple values with a comma."
                " Cannot be used used together with"
                " [bold bright_cyan]geom-filter-file[/bold bright_cyan] or"
                " [bold bright_cyan]geom-filter-geocode[/bold bright_cyan] or"
                " [bold bright_cyan]geom-filter-geojson[/bold bright_cyan] or"
                " [bold bright_cyan]geom-filter-index-h3[/bold bright_cyan] or"
                " [bold bright_cyan]geom-filter-index-s2[/bold bright_cyan] or"
                " [bold bright_cyan]geom-filter-wkt[/bold bright_cyan]."
            ),
            click_type=GeohashGeometryParser(),
        ),
    ] = None,
    geom_filter_index_h3: Annotated[
        Optional[str],
        typer.Option(
            help=(
                "Geometry to use as a filter in the [bold dark_orange]H3 index[/bold dark_orange]"
                " format. Separate multiple values with a comma."
                " Cannot be used used together with"
                " [bold bright_cyan]geom-filter-file[/bold bright_cyan] or"
                " [bold bright_cyan]geom-filter-geocode[/bold bright_cyan] or"
                " [bold bright_cyan]geom-filter-geojson[/bold bright_cyan] or"
                " [bold bright_cyan]geom-filter-index-geohash[/bold bright_cyan] or"
                " [bold bright_cyan]geom-filter-index-s2[/bold bright_cyan] or"
                " [bold bright_cyan]geom-filter-wkt[/bold bright_cyan]."
            ),
            click_type=H3GeometryParser(),
        ),
    ] = None,
    geom_filter_index_s2: Annotated[
        Optional[str],
        typer.Option(
            help=(
                "Geometry to use as a filter in the [bold dark_orange]S2 index[/bold dark_orange]"
                " format. Separate multiple values with a comma."
                " Cannot be used used together with"
                " [bold bright_cyan]geom-filter-file[/bold bright_cyan] or"
                " [bold bright_cyan]geom-filter-geocode[/bold bright_cyan] or"
                " [bold bright_cyan]geom-filter-geojson[/bold bright_cyan] or"
                " [bold bright_cyan]geom-filter-index-geohash[/bold bright_cyan] or"
                " [bold bright_cyan]geom-filter-index-h3[/bold bright_cyan] or"
                " [bold bright_cyan]geom-filter-wkt[/bold bright_cyan]."
            ),
            click_type=S2GeometryParser(),
        ),
    ] = None,
    geom_filter_wkt: Annotated[
        Optional[str],
        typer.Option(
            help=(
                "Geometry to use as a filter in the [bold dark_orange]WKT[/bold dark_orange]"
                " format."
                " Cannot be used together with"
                " [bold bright_cyan]geom-filter-file[/bold bright_cyan] or"
                " [bold bright_cyan]geom-filter-geocode[/bold bright_cyan] or"
                " [bold bright_cyan]geom-filter-geojson[/bold bright_cyan] or"
                " [bold bright_cyan]geom-filter-index-geohash[/bold bright_cyan] or"
                " [bold bright_cyan]geom-filter-index-h3[/bold bright_cyan] or"
                " [bold bright_cyan]geom-filter-index-s2[/bold bright_cyan]."
            ),
            click_type=WktGeometryParser(),
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
                "Path where to save final geoparquet file. If not provided, it will be generated"
                " automatically based on the input pbf file name."
            ),
        ),
    ] = None,
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
        ),
    ] = None,
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
    number_of_geometries_provided = sum(
        geom is not None
        for geom in (
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
        geom_filter_file
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
                " (one of --geom-filter-file, --geom-filter-geocode,"
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

    verbosity_mode: Literal["silent", "transient", "verbose"] = "verbose"

    if transient_mode:
        verbosity_mode = "transient"
    elif silent_mode:
        verbosity_mode = "silent"

    logging.disable(logging.CRITICAL)
    if pbf_file:
        from quackosm.functions import convert_pbf_to_parquet

        geoparquet_path = convert_pbf_to_parquet(
            pbf_path=pbf_file,
            tags_filter=osm_tags_filter or osm_tags_filter_file,  # type: ignore
            keep_all_tags=keep_all_tags,
            geometry_filter=geometry_filter_value,
            explode_tags=explode_tags,
            ignore_cache=ignore_cache,
            working_directory=working_directory,
            result_file_path=result_file_path,
            osm_way_polygon_features_config=(
                json.loads(Path(osm_way_polygon_features_config).read_text())
                if osm_way_polygon_features_config
                else None
            ),
            filter_osm_ids=filter_osm_ids,  # type: ignore
            save_as_wkt=wkt_result,
            verbosity_mode=verbosity_mode,
        )
    elif osm_extract_query:
        from quackosm._exceptions import OsmExtractSearchError
        from quackosm.functions import convert_osm_extract_to_parquet

        try:
            geoparquet_path = convert_osm_extract_to_parquet(
                osm_extract_query=osm_extract_query,
                osm_extract_source=osm_extract_source,
                tags_filter=osm_tags_filter or osm_tags_filter_file,  # type: ignore
                keep_all_tags=keep_all_tags,
                geometry_filter=geometry_filter_value,
                explode_tags=explode_tags,
                ignore_cache=ignore_cache,
                working_directory=working_directory,
                result_file_path=result_file_path,
                osm_way_polygon_features_config=(
                    json.loads(Path(osm_way_polygon_features_config).read_text())
                    if osm_way_polygon_features_config
                    else None
                ),
                filter_osm_ids=filter_osm_ids,  # type: ignore
                save_as_wkt=wkt_result,
                verbosity_mode=verbosity_mode,
            )
        except OsmExtractSearchError as ex:
            from rich.console import Console

            err_console = Console(stderr=True)
            err_console.print(ex)
            raise typer.Exit(code=1) from None
    else:
        from quackosm.functions import convert_geometry_to_parquet

        geoparquet_path = convert_geometry_to_parquet(
            geometry_filter=geometry_filter_value,
            osm_extract_source=osm_extract_source,
            tags_filter=osm_tags_filter or osm_tags_filter_file,  # type: ignore
            keep_all_tags=keep_all_tags,
            explode_tags=explode_tags,
            ignore_cache=ignore_cache,
            working_directory=working_directory,
            result_file_path=result_file_path,
            osm_way_polygon_features_config=(
                json.loads(Path(osm_way_polygon_features_config).read_text())
                if osm_way_polygon_features_config
                else None
            ),
            filter_osm_ids=filter_osm_ids,  # type: ignore
            save_as_wkt=wkt_result,
            verbosity_mode=verbosity_mode,
            geometry_coverage_iou_threshold=geometry_coverage_iou_threshold,
            allow_uncovered_geometry=allow_uncovered_geometry,
        )
    typer.secho(geoparquet_path, fg="green")
