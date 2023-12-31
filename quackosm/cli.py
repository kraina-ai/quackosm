"""CLI module for parsing pbf file to geoparquet."""

import json
import pathlib
from typing import Annotated, Optional, Union, cast

import geopandas as gpd
import typer
from shapely import from_geojson, from_wkt
from shapely.geometry.base import BaseGeometry

from quackosm import __app_name__, __version__
from quackosm._osm_tags_filters import GroupedOsmTagsFilter, OsmTagsFilter
from quackosm.functions import convert_pbf_to_gpq

app = typer.Typer(context_settings={"help_option_names": ["-h", "--help"]}, rich_markup_mode="rich")


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"{__app_name__} {__version__}")
        raise typer.Exit()


def _path_callback(ctx: typer.Context, value: pathlib.Path) -> pathlib.Path:
    if ctx.resilient_parsing:
        return value
    if not pathlib.Path(value).exists():
        raise typer.BadParameter(f"File not found error: {value}")
    return value


def _empty_path_callback(ctx: typer.Context, value: pathlib.Path) -> Optional[pathlib.Path]:
    if not value:
        return None
    if not pathlib.Path(value).exists():
        raise typer.BadParameter(f"File not found error: {value}")
    return value


def _wkt_callback(value: str) -> BaseGeometry:
    if not value:
        return None
    try:
        return from_wkt(value)
    except Exception:
        raise typer.BadParameter("Cannot parse provided WKT") from None


def _geojson_callback(value: str) -> BaseGeometry:
    if not value:
        return None
    try:
        return from_geojson(value)
    except Exception:
        raise typer.BadParameter("Cannot parse provided GeoJSON") from None


def _geo_file_callback(value: str) -> BaseGeometry:
    if not value:
        return None

    if not pathlib.Path(value).exists():
        raise typer.BadParameter("Cannot parse provided geo file")

    try:
        gdf = gpd.read_file(value)
        return gdf.unary_union
    except Exception:
        raise typer.BadParameter("Cannot parse provided geo file") from None


def parse_tags_filter(value: str) -> Optional[Union[OsmTagsFilter, GroupedOsmTagsFilter]]:
    """Parse provided cli agrument to tags filter."""
    if not value:
        return None
    try:
        parsed_dict = json.loads(value)
        return cast(Union[OsmTagsFilter, GroupedOsmTagsFilter], parsed_dict)
    except Exception:
        raise typer.BadParameter("Cannot parse provided OSM tags filter") from None


def _filter_osm_ids_callback(value: list[str]) -> list[str]:
    for osm_id in value:
        if not osm_id.startswith(("node/", "way/", "relation/")):
            raise typer.BadParameter(f"Cannot parse provided OSM id: {osm_id}") from None

    return value


@app.command()  # type: ignore
def main(
    pbf_file: Annotated[
        pathlib.Path,
        typer.Argument(
            help="PBF file to convert into GeoParquet",
            metavar="PBF file path",
            callback=_path_callback,
        ),
    ],
    osm_tags_filter: Annotated[
        Optional[str],
        typer.Option(
            parser=parse_tags_filter,
            help=(
                "OSM tags used to filter the data. Can the the form of flat or grouped dict "
                "(look: [bold green]OsmTagsFilter[/bold green]"
                " and [bold green]GroupedOsmTagsFilter[/bold green])."
            ),
        ),
    ] = None,
    geom_filter_wkt: Annotated[
        Optional[str],
        typer.Option(
            help=(
                "Geometry to use as filter in [bold green]WKT[/bold green] format."
                " Cannot be used together with"
                " [bold dark_orange]geom-filter-geojson[/bold dark_orange] or"
                " [bold dark_orange]geom-filter-file[/bold dark_orange]."
            ),
            parser=_wkt_callback,
        ),
    ] = None,
    geom_filter_geojson: Annotated[
        Optional[str],
        typer.Option(
            help=(
                "Geometry to use as filter in [bold green]GeoJSON[/bold green] format."
                " Cannot be used used together with"
                " [bold dark_orange]geom-filter-wkt[/bold dark_orange] or"
                " [bold dark_orange]geom-filter-file[/bold dark_orange]."
            ),
            parser=_geojson_callback,
        ),
    ] = None,
    geom_filter_file: Annotated[
        Optional[str],
        typer.Option(
            help=(
                "Geometry to use as filter in [bold green]file[/bold green] format - any that can"
                " be opened by GeoPandas. Fill return unary_union of the file."
                " Cannot be used together with"
                " [bold dark_orange]geom-filter-wkt[/bold dark_orange] or"
                " [bold dark_orange]geom-filter-geojson[/bold dark_orange]."
            ),
            parser=_geo_file_callback,
        ),
    ] = None,
    explode_tags: Annotated[
        Optional[bool],
        typer.Option(
            "--explode-tags/--compact-tags",
            "--explode/--compact",
            help=(
                "Whether to split tags into columns based on OSM tag keys. "
                "If [bold violet]None[/bold violet], will be set based on "
                "[bold dark_orange]osm-tags-filter[/bold dark_orange] parameter. "
                "If no tags filter is provided, then explode_tags will set to [bold"
                " red]False[/bold red], "
                "if there is tags filter it will set to [bold green]True[/bold green]."
            ),
            show_default=None,
        ),
    ] = None,
    result_file_path: Annotated[
        Optional[pathlib.Path],
        typer.Option(
            "--output",
            "-o",
            help=(
                "Path where to save final geoparquet file. If not provided, will be generated"
                " automatically based on input pbf file name."
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
        pathlib.Path,
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
        Optional[pathlib.Path],
        typer.Option(
            "--osm-way-polygon-config",
            help=(
                "Config where alternative OSM way polygon features config is defined."
                " Will determine how to parse way features based on tags."
                " Option is intended for experienced users. It's recommended to disable"
                " cache ([bold dark_orange]no-cache[/bold dark_orange]) when using this option,"
                " since file names don't contain information what config file has been used"
                " for file generation."
            ),
            callback=_empty_path_callback,
        ),
    ] = None,
    filter_osm_ids: Annotated[
        Optional[list[str]],
        typer.Option(
            "--filter-osm-id",
            "--filter",
            help=(
                "List of OSM features ids to read from the file."
                "Have to be in the form of 'node/<id>', 'way/<id>' or 'relation/<id>'."
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
    more_than_one_geometry_provided = (
        sum(geom is not None for geom in (geom_filter_wkt, geom_filter_geojson, geom_filter_file))
        > 1
    )
    if more_than_one_geometry_provided:
        raise typer.BadParameter("Provided more than one geometry for filtering")

    geoparquet_path = convert_pbf_to_gpq(
        pbf_path=pbf_file,
        tags_filter=osm_tags_filter,  # type: ignore
        geometry_filter=geom_filter_wkt or geom_filter_geojson or geom_filter_file,
        explode_tags=explode_tags,
        ignore_cache=ignore_cache,
        working_directory=working_directory,
        result_file_path=result_file_path,
        osm_way_polygon_features_config=(
            json.loads(pathlib.Path(osm_way_polygon_features_config).read_text())
            if osm_way_polygon_features_config
            else None
        ),
        filter_osm_ids=filter_osm_ids,
    )
    typer.secho(geoparquet_path, fg="green")
