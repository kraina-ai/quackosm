"""CLI module for parsing pbf file to geoparquet."""

import json
import pathlib
from typing import Annotated, Optional, Union, cast

import click
import geopandas as gpd
import typer
from shapely import from_geojson, from_wkt

from quackosm import __app_name__, __version__
from quackosm._osm_tags_filters import GroupedOsmTagsFilter, OsmTagsFilter
from quackosm._typing import is_expected_type
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

        if not pathlib.Path(value).exists():
            raise typer.BadParameter("Cannot parse provided geo file")

        try:
            gdf = gpd.read_file(value)
            return gdf.unary_union
        except Exception:
            raise typer.BadParameter("Cannot parse provided geo file") from None


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

        file_path = pathlib.Path(value)

        if not file_path.exists():
            raise typer.BadParameter("Cannot parse provided OSM tags filter file")

        return super().convert(file_path.read_text(), param, ctx)  # type: ignore


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
    geom_filter_wkt: Annotated[
        Optional[str],
        typer.Option(
            help=(
                "Geometry to use as a filter in the [bold dark_orange]WKT[/bold dark_orange]"
                " format."
                " Cannot be used together with"
                " [bold bright_cyan]geom-filter-geojson[/bold bright_cyan] or"
                " [bold bright_cyan]geom-filter-file[/bold bright_cyan]."
            ),
            click_type=WktGeometryParser(),
        ),
    ] = None,
    geom_filter_geojson: Annotated[
        Optional[str],
        typer.Option(
            help=(
                "Geometry to use as a filter in the [bold dark_orange]GeoJSON[/bold dark_orange]"
                " format."
                " Cannot be used used together with"
                " [bold bright_cyan]geom-filter-wkt[/bold bright_cyan] or"
                " [bold bright_cyan]geom-filter-file[/bold bright_cyan]."
            ),
            click_type=GeoJsonGeometryParser(),
        ),
    ] = None,
    geom_filter_file: Annotated[
        Optional[str],
        typer.Option(
            help=(
                "Geometry to use as a filter in the"
                " [bold dark_orange]file[/bold dark_orange] format - any that can be opened by"
                " GeoPandas. Will return the unary union of the geometries in the file."
                " Cannot be used together with"
                " [bold bright_cyan]geom-filter-wkt[/bold bright_cyan] or"
                " [bold bright_cyan]geom-filter-geojson[/bold bright_cyan]."
            ),
            click_type=GeoFileGeometryParser(),
        ),
    ] = None,
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
        Optional[pathlib.Path],
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
                " cache ([bold bright_cyan]no-cache[/bold bright_cyan]) when using this option,"
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
                "List of OSM features IDs to read from the file."
                " Have to be in the form of 'node/<id>', 'way/<id>' or 'relation/<id>'."
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

    if osm_tags_filter is not None and osm_tags_filter_file is not None:
        raise typer.BadParameter("Provided more than one osm tags filter parameter")

    geoparquet_path = convert_pbf_to_gpq(
        pbf_path=pbf_file,
        tags_filter=osm_tags_filter or osm_tags_filter_file,  # type: ignore
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
