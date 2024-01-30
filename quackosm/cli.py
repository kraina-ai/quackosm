"""CLI module for parsing pbf file to geoparquet."""

import json
import logging
import re
import warnings
from pathlib import Path
from typing import Annotated, Optional, Union, cast

import click
import geopandas as gpd
import h3
import osmnx as ox
import typer
from click import Argument
from click.exceptions import MissingParameter
from geohash import bbox as geohash_bbox
from h3ronpy.arrow.vector import cells_to_wkb_polygons
from s2 import s2
from shapely import from_geojson, from_wkt
from shapely.geometry import Polygon, box

from quackosm import __app_name__, __version__
from quackosm._osm_tags_filters import GroupedOsmTagsFilter, OsmTagsFilter
from quackosm._typing import is_expected_type
from quackosm.functions import convert_geometry_to_gpq, convert_pbf_to_gpq
from quackosm.osm_extracts import OsmExtractSource

app = typer.Typer(context_settings={"help_option_names": ["-h", "--help"]}, rich_markup_mode="rich")


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"{__app_name__} {__version__}")
        raise typer.Exit()


def _path_callback(ctx: typer.Context, value: Path) -> Path:
    if not Path(value).exists():
        raise typer.BadParameter(f"File not found error: {value}")
    return value


def _empty_path_callback(ctx: typer.Context, value: Path) -> Optional[Path]:
    if not value:
        return None
    return _path_callback(ctx, value)


class WktGeometryParser(click.ParamType):  # type: ignore
    """Parser for geometry in WKT form."""

    name = "TEXT (WKT)"

    def convert(self, value, param, ctx):  # type: ignore
        """Convert parameter value."""
        if not value:
            return None
        try:
            return from_wkt(value)
        except Exception:
            raise typer.BadParameter("Cannot parse provided WKT") from None


class GeoJsonGeometryParser(click.ParamType):  # type: ignore
    """Parser for geometry in GeoJSON form."""

    name = "TEXT (GeoJSON)"

    def convert(self, value, param, ctx):  # type: ignore
        """Convert parameter value."""
        if not value:
            return None
        try:
            return from_geojson(value)
        except Exception:
            raise typer.BadParameter("Cannot parse provided GeoJSON") from None


class GeoFileGeometryParser(click.ParamType):  # type: ignore
    """Parser for geometry in geo file form."""

    name = "PATH"

    def convert(self, value, param, ctx):  # type: ignore
        """Convert parameter value."""
        if not value:
            return None

        value = _path_callback(ctx=ctx, value=value)

        try:
            gdf = gpd.read_file(value)
            return gdf.unary_union
        except Exception:
            raise typer.BadParameter("Cannot parse provided geo file") from None


class GeocodeGeometryParser(click.ParamType):  # type: ignore
    """Parser for geometry in string Nominatim query form."""

    name = "TEXT"

    def convert(self, value, param, ctx):  # type: ignore
        """Convert parameter value."""
        if not value:
            return None

        try:
            gdf = ox.geocode_to_gdf(query=value, which_result=None)
            return gdf.unary_union
        except Exception:
            raise typer.BadParameter("Cannot geocode provided Nominatim query") from None


class GeohashGeometryParser(click.ParamType):  # type: ignore
    """Parser for geometry in string Nominatim query form."""

    name = "TEXT (Geohash)"

    def convert(self, value, param, ctx):  # type: ignore
        """Convert parameter value."""
        if not value:
            return None

        try:
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

    def convert(self, value, param, ctx):  # type: ignore
        """Convert parameter value."""
        if not value:
            return None

        try:
            h3_int_indexes = [h3.str_to_int(h3_cell.strip()) for h3_cell in value.split(",")]
            return gpd.GeoSeries.from_wkb(cells_to_wkb_polygons(h3_int_indexes)).unary_union
        except Exception:
            raise typer.BadParameter(f"Cannot parse provided H3 values: {value}") from None


class S2GeometryParser(click.ParamType):  # type: ignore
    """Parser for geometry in string Nominatim query form."""

    name = "TEXT (S2)"

    def convert(self, value, param, ctx):  # type: ignore
        """Convert parameter value."""
        if not value:
            return None

        try:
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

    def convert(self, value, param, ctx):  # type: ignore
        """Convert parameter value."""
        if not value:
            return None
        try:
            parsed_dict = json.loads(value)
        except Exception:
            raise typer.BadParameter("Cannot parse provided OSM tags filter") from None

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

    def convert(self, value, param, ctx):  # type: ignore
        """Convert parameter value."""
        if not value:
            return None

        value = _path_callback(ctx=ctx, value=value)

        return super().convert(Path(value).read_text(), param, ctx)  # type: ignore


def _filter_osm_ids_callback(value: str) -> Optional[list[str]]:
    if not value:
        return None

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
        Optional[Path],
        typer.Argument(
            help="PBF file to convert into GeoParquet",
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
                " geocoded to the geometry using Nominatim API (OSMnx library)."
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
    osm_extract_source: Annotated[
        OsmExtractSource,
        typer.Option(
            "--osm-extract-source",
            "--pbf-download-source",
            help=(
                "Source where to download the PBF file from."
                " Can be Geofabrik, BBBike, OpenStreetMap.fr or any."
            ),
            case_sensitive=False,
            show_default="any",
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

    Wraps convert_pbf_to_gpq function and print final path to the saved geoparquet file at the end.
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

    if pbf_file is None and geometry_filter_value is None:  # noqa: FURB124
        raise MissingParameter(
            message=(
                "QuackOSM requires either the path to the pbf file or a geometry filter"
                " (one of --geom-filter-file, --geom-filter-geocode,"
                " --geom-filter-geojson, --geom-filter-index-geohash,"
                " --geom-filter-index-h3, --geom-filter-index-s2, --geom-filter-wkt)"
                " to download the file automatically. Both cannot be empty at once."
            ),
            param=Argument(["pbf_file"], type=Path, metavar="PBF file path"),
        )

    if osm_tags_filter is not None and osm_tags_filter_file is not None:
        raise typer.BadParameter("Provided more than one osm tags filter parameter")

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        logging.disable(logging.CRITICAL)
        if pbf_file:
            geoparquet_path = convert_pbf_to_gpq(
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
            )
        else:
            geoparquet_path = convert_geometry_to_gpq(
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
            )
    typer.secho(geoparquet_path, fg="green")
