<p align="center">
  <img width="300" src="https://raw.githubusercontent.com/kraina-ai/quackosm/main/docs/assets/logos/quackosm_logo.png"><br/>
  <small>Generated using DALL·E 3 model with this prompt: A logo for a python library with White background, high quality, 8k. Cute duck and globe with cartography elements. Library for reading OpenStreetMap data using DuckDB.</small>
</p>

<p align="center">
    <img alt="GitHub" src="https://img.shields.io/github/license/kraina-ai/quackosm?logo=apache&logoColor=%23fff">
    <img src="https://img.shields.io/github/checks-status/kraina-ai/quackosm/main?logo=GitHubActions&logoColor=%23fff" alt="Checks">
    <a href="https://github.com/kraina-ai/quackosm/actions/workflows/ci-dev.yml" target="_blank">
        <img alt="GitHub Workflow Status - DEV" src="https://img.shields.io/github/actions/workflow/status/kraina-ai/quackosm/ci-dev.yml?label=build-dev&logo=GitHubActions&logoColor=%23fff">
    </a>
    <a href="https://github.com/kraina-ai/quackosm/actions/workflows/ci-prod.yml" target="_blank">
        <img alt="GitHub Workflow Status - PROD" src="https://img.shields.io/github/actions/workflow/status/kraina-ai/quackosm/ci-prod.yml?label=build-prod&logo=GitHubActions&logoColor=%23fff">
    </a>
    <a href="https://results.pre-commit.ci/latest/github/kraina-ai/quackosm/main" target="_blank">
        <img src="https://results.pre-commit.ci/badge/github/kraina-ai/quackosm/main.svg" alt="pre-commit.ci status">
    </a>
    <a href="https://www.codefactor.io/repository/github/kraina-ai/quackosm"><img alt="CodeFactor Grade" src="https://img.shields.io/codefactor/grade/github/kraina-ai/quackosm?logo=codefactor&logoColor=%23fff"></a>
    <a href="https://app.codecov.io/gh/kraina-ai/quackosm/tree/main"><img alt="Codecov" src="https://img.shields.io/codecov/c/github/kraina-ai/quackosm?logo=codecov&token=PRS4E02ZX0&logoColor=%23fff"></a>
    <a href="https://pypi.org/project/quackosm" target="_blank">
        <img src="https://img.shields.io/pypi/v/quackosm?color=%2334D058&label=pypi%20package&logo=pypi&logoColor=%23fff" alt="Package version">
    </a>
    <a href="https://pypi.org/project/quackosm" target="_blank">
        <img src="https://img.shields.io/pypi/pyversions/quackosm.svg?color=%2334D058&logo=python&logoColor=%23fff" alt="Supported Python versions">
    </a>
    <a href="https://pypi.org/project/quackosm" target="_blank">
        <img alt="PyPI - Downloads" src="https://img.shields.io/pypi/dm/quackosm">
    </a>
</p>

# QuackOSM

An open-source tool for reading OpenStreetMap PBF files using DuckDB.

## What is **QuackOSM** 🦆?

- Scalable reader for OpenStreetMap ProtoBuffer (`pbf`) files.
- Is based on top of `DuckDB`[^1] with its `Spatial`[^2] extension.
- Saves files in the `GeoParquet`[^3] file format for easier integration with modern cloud stacks.
- Utilizes multithreading unlike GDAL that works in a single thread only.
- Can filter data based on geometry without the need for `ogr2ogr` clipping before operation.
- Can filter data based on OSM tags.
- Utilizes caching to reduce repeatable computations.
- Can be used as Python module as well as a beautiful CLI based on `Typer`[^4].

[^1]: [DuckDB Website](https://duckdb.org/)
[^2]: [DuckDB Spatial extension repository](https://github.com/duckdb/duckdb_spatial)
[^3]: [GeoParquet data format](https://geoparquet.org/)
[^4]: [Typer docs](https://typer.tiangolo.com/)

## Installing

### As pure Python module

```
pip install quackosm
```

### With beautiful CLI

```
pip install quackosm[cli]
```

### Required Python version?

QuackOSM supports **Python >= 3.9**

### Dependencies

Required:

- `duckdb (>=0.10.2)`: For all DuckDB operations on PBF files

- `pyarrow (>=16.0.0)`: For parquet files wrangling

- `pyarrow-ops`: For easy removal of duplicated features in parquet files

- `geoarrow-pyarrow (>=0.1.2)`: For GeoParquet IO operations

- `geoarrow-pandas (>=0.1.1)`: For GeoParquet integration with GeoPandas

- `geopandas (>=0.6)`: For returning GeoDataFrames and reading Geo files

- `shapely (>=2.0)`: For parsing WKT and GeoJSON strings and fixing geometries

- `geoarrow-rust-core (>=0.2)`: For transforming Arrow data to Shapely objects

- `polars (>=0.19.4)`: For faster OSM ways grouping operation

- `typeguard (>=3.0)`: For internal validation of types

- `psutil (>=5.6.2)`: For automatic scaling of parameters based on available resources

- `pooch (>=1.6.0)`: For downloading `*.osm.pbf` files

- `rich (>=12.0.0)` & `tqdm (>=4.42.0)`: For showing progress bars

- `requests`: For iterating OSM PBF files services

- `beautifulsoup4`: For parsing HTML files and scraping required information

- `geopy (>=2.0.0)`: For geocoding of strings

Optional:

- `typer[all] (>=0.9.0)` (click, colorama, rich, shellingham): Required in CLI

- `h3 (>=4.0.0b1)`: For reading H3 strings. Required in CLI

- `s2 (>=0.1.9)`: For transforming S2 indexes into geometries. Required in CLI

- `python-geohash (>=0.8)`: For transforming GeoHash indexes into geometries. Required in CLI

## Usage

### If you already have downloaded the PBF file 📁🗺️

#### Load data as a GeoDataFrame

```python
>>> import quackosm as qosm
>>> qosm.convert_pbf_to_geodataframe(monaco_pbf_path)
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

[7906 rows x 2 columns]
```

#### Just convert PBF to GeoParquet

```python
>>> import quackosm as qosm
>>> gpq_path = qosm.convert_pbf_to_parquet(monaco_pbf_path)
>>> gpq_path.as_posix()
'files/monaco_nofilter_noclip_compact.parquet'
```

#### Inspect the file with duckdb

```python
>>> import duckdb
>>> duckdb.load_extension('spatial')
>>> duckdb.read_parquet(str(gpq_path)).project(
...     "* REPLACE (ST_GeomFromWKB(geometry) AS geometry)"
... ).order("feature_id")
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
```

#### Use as CLI

```console
$ quackosm monaco.osm.pbf
⠋ [   1/32] Reading nodes • 0:00:00
⠋ [   2/32] Filtering nodes - intersection • 0:00:00
⠋ [   3/32] Filtering nodes - tags • 0:00:00
⠋ [   4/32] Calculating distinct filtered nodes ids • 0:00:00
⠋ [   5/32] Reading ways • 0:00:00
⠋ [   6/32] Unnesting ways • 0:00:00
⠋ [   7/32] Filtering ways - valid refs • 0:00:00
⠋ [   8/32] Filtering ways - intersection • 0:00:00
⠋ [   9/32] Filtering ways - tags • 0:00:00
⠋ [  10/32] Calculating distinct filtered ways ids • 0:00:00
⠋ [  11/32] Reading relations • 0:00:00
⠋ [  12/32] Unnesting relations • 0:00:00
⠸ [  13/32] Filtering relations - valid refs • 0:00:00
⠋ [  14/32] Filtering relations - intersection • 0:00:00
⠋ [  15/32] Filtering relations - tags • 0:00:00
⠋ [  16/32] Calculating distinct filtered relations ids • 0:00:00
⠋ [  17/32] Loading required ways - by relations • 0:00:00
⠋ [  18/32] Calculating distinct required ways ids • 0:00:00
⠋ [  19/32] Saving filtered nodes with geometries • 0:00:00
⠋ [20.1/32] Grouping filtered ways - assigning groups • 0:00:00
⠧ [20.2/32] Grouping filtered ways - joining with nodes • 0:00:00
⠋ [20.3/32] Grouping filtered ways - partitioning by group • 0:00:00
  [  21/32] Saving filtered ways with linestrings 100% ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 1/1 • 0:00:00 < 0:00:00 •
⠋ [22.1/32] Grouping required ways - assigning groups • 0:00:00
⠧ [22.2/32] Grouping required ways - joining with nodes • 0:00:00
⠋ [22.3/32] Grouping required ways - partitioning by group • 0:00:00
  [  23/32] Saving required ways with linestrings 100% ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 1/1 • 0:00:00 < 0:00:00 •
⠙ [  24/32] Saving filtered ways with geometries • 0:00:00
⠋ [  25/32] Saving valid relations parts • 0:00:00
⠋ [  26/32] Saving relations inner parts • 0:00:00
⠋ [  27/32] Saving relations outer parts • 0:00:00
⠋ [  28/32] Saving relations outer parts with holes • 0:00:00
⠋ [  29/32] Saving relations outer parts without holes • 0:00:00
⠋ [  30/32] Saving filtered relations with geometries • 0:00:00
⠋ [  31/32] Saving all features • 0:00:00
⠋ [  32/32] Saving final geoparquet file • 0:00:00
Finished operation in 0:00:03
files/monaco_nofilter_noclip_compact.parquet
```

### Let the QuackOSM automatically download the required OSM PBF files for you based on geometry 🔎🌍

#### Load data as a GeoDataFrame

```python
>>> import quackosm as qosm
>>> geometry = qosm.geocode_to_geometry("Vatican City")
>>> qosm.convert_geometry_to_geodataframe(geometry)
                                              tags                      geometry
feature_id
node/10253371713   {'crossing': 'uncontrolled',...     POINT (12.45603 41.90454)
node/10253371714               {'highway': 'stop'}     POINT (12.45705 41.90400)
node/10253371715               {'highway': 'stop'}     POINT (12.45242 41.90164)
node/10253371720     {'artwork_type': 'statue',...     POINT (12.45147 41.90484)
node/10253371738               {'natural': 'tree'}     POINT (12.45595 41.90609)
...                                            ...                           ...
way/983015528     {'barrier': 'hedge', 'height'...  POLYGON ((12.45027 41.901...
way/983015529     {'barrier': 'hedge', 'height'...  POLYGON ((12.45028 41.901...
way/983015530     {'barrier': 'hedge', 'height'...  POLYGON ((12.45023 41.901...
way/998561138     {'barrier': 'bollard', 'bicyc...  LINESTRING (12.45821 41.9...
way/998561139     {'barrier': 'bollard', 'bicyc...  LINESTRING (12.45828 41.9...

[3286 rows x 2 columns]
```

#### Just convert geometry to GeoParquet

```python
>>> import quackosm as qosm
>>> from shapely import from_wkt
>>> geometry = from_wkt(
...     "POLYGON ((14.4861 35.9107, 14.4861 35.8811, 14.5331 35.8811, 14.5331 35.9107, 14.4861 35.9107))"
... )
>>> gpq_path = qosm.convert_geometry_to_parquet(geometry)
>>> gpq_path.as_posix()
'files/4b2967088a8fe31cdc15401e29bff9b7b882565cd8143e90443f39f2dc5fe6de_nofilter_compact.parquet'
```

#### Inspect the file with duckdb

```python
>>> import duckdb
>>> duckdb.load_extension('spatial')
>>> duckdb.read_parquet(str(gpq_path)).project(
...     "* REPLACE (ST_GeomFromWKB(geometry) AS geometry)"
... ).order("feature_id")
┌──────────────────┬──────────────────────┬──────────────────────────────────────────────┐
│    feature_id    │         tags         │                   geometry                   │
│     varchar      │ map(varchar, varch…  │                   geometry                   │
├──────────────────┼──────────────────────┼──────────────────────────────────────────────┤
│ node/10001388317 │ {amenity=bench, ba…  │ POINT (14.5093988 35.8936881)                │
│ node/10001388417 │ {amenity=bench, ba…  │ POINT (14.5094635 35.8937135)                │
│ node/10001388517 │ {amenity=bench, ba…  │ POINT (14.5095215 35.8937305)                │
│ node/10018287160 │ {opening_hours=Mo-…  │ POINT (14.5184916 35.8915925)                │
│ node/10018287161 │ {defensive_works=b…  │ POINT (14.5190093 35.8909471)                │
│ node/10018287162 │ {defensive_works=h…  │ POINT (14.5250094 35.8883199)                │
│ node/10018742746 │ {defibrillator:loc…  │ POINT (14.5094082 35.8965151)                │
│ node/10018742747 │ {amenity=bank, nam…  │ POINT (14.51329 35.8991614)                  │
│ node/10032244899 │ {amenity=restauran…  │ POINT (14.4946298 35.8986226)                │
│ node/10034853491 │ {amenity=pharmacy}   │ POINT (14.4945884 35.9012758)                │
│       ·          │         ·            │               ·                              │
│       ·          │         ·            │               ·                              │
│       ·          │         ·            │               ·                              │
│ way/884730763    │ {highway=footway, …  │ LINESTRING (14.5218277 35.8896022, 14.5218…  │
│ way/884730764    │ {bridge=yes, highw…  │ LINESTRING (14.5218054 35.8896015, 14.5218…  │
│ way/884730765    │ {highway=footway, …  │ LINESTRING (14.5204069 35.889924, 14.52044…  │
│ way/884730766    │ {handrail=yes, hig…  │ LINESTRING (14.5204375 35.8898663, 14.5204…  │
│ way/884730767    │ {access=yes, handr…  │ LINESTRING (14.5196113 35.8906142, 14.5196…  │
│ way/884730768    │ {highway=steps, la…  │ LINESTRING (14.5197226 35.890676, 14.51972…  │
│ way/884730769    │ {access=yes, handr…  │ LINESTRING (14.5197184 35.8906707, 14.5197…  │
│ way/884738591    │ {highway=pedestria…  │ LINESTRING (14.5204163 35.8897296, 14.5204…  │
│ way/884744870    │ {highway=residenti…  │ LINESTRING (14.5218931 35.8864046, 14.5221…  │
│ way/884744871    │ {access=yes, handr…  │ LINESTRING (14.5221083 35.8864287, 14.5221…  │
├──────────────────┴──────────────────────┴──────────────────────────────────────────────┤
│ ? rows (>9999 rows, 20 shown)                                                3 columns │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

#### Use as CLI

```console
$ quackosm --geom-filter-geocode "Shibuya, Tokyo"
100%|██████████████████████████████████████| 46.3M/46.3M [00:00<00:00, 327GB/s]
⠋ [   1/32] Reading nodes • 0:00:01
⠹ [   2/32] Filtering nodes - intersection • 0:00:00
⠋ [   3/32] Filtering nodes - tags • 0:00:00
⠋ [   4/32] Calculating distinct filtered nodes ids • 0:00:00
⠸ [   5/32] Reading ways • 0:00:03
⠴ [   6/32] Unnesting ways • 0:00:01
⠼ [   7/32] Filtering ways - valid refs • 0:00:00
⠹ [   8/32] Filtering ways - intersection • 0:00:00
⠋ [   9/32] Filtering ways - tags • 0:00:00
⠋ [  10/32] Calculating distinct filtered ways ids • 0:00:00
⠼ [  11/32] Reading relations • 0:00:00
⠸ [  12/32] Unnesting relations • 0:00:00
⠋ [  13/32] Filtering relations - valid refs • 0:00:00
⠋ [  14/32] Filtering relations - intersection • 0:00:00
⠋ [  15/32] Filtering relations - tags • 0:00:00
⠋ [  16/32] Calculating distinct filtered relations ids • 0:00:00
⠋ [  17/32] Loading required ways - by relations • 0:00:00
⠋ [  18/32] Calculating distinct required ways ids • 0:00:00
⠹ [  19/32] Saving filtered nodes with geometries • 0:00:00
⠋ [20.1/32] Grouping filtered ways - assigning groups • 0:00:00
⠼ [20.2/32] Grouping filtered ways - joining with nodes • 0:00:01
⠋ [20.3/32] Grouping filtered ways - partitioning by group • 0:00:00
  [  21/32] Saving filtered ways with linestrings 100% ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 1/1 • 0:00:00 < 0:00:00 •
⠋ [22.1/32] Grouping required ways - assigning groups • 0:00:00
⠼ [22.2/32] Grouping required ways - joining with nodes • 0:00:01
⠋ [22.3/32] Grouping required ways - partitioning by group • 0:00:00
  [  23/32] Saving required ways with linestrings 100% ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 1/1 • 0:00:00 < 0:00:00 •
⠴ [  24/32] Saving filtered ways with geometries • 0:00:00
⠋ [  25/32] Saving valid relations parts • 0:00:00
⠋ [  26/32] Saving relations inner parts • 0:00:00
⠋ [  27/32] Saving relations outer parts • 0:00:00
⠋ [  28/32] Saving relations outer parts with holes • 0:00:00
⠋ [  29/32] Saving relations outer parts without holes • 0:00:00
⠋ [  30/32] Saving filtered relations with geometries • 0:00:00
⠙ [  31/32] Saving all features • 0:00:00
⠋ [  32/32] Saving final geoparquet file • 0:00:00
Finished operation in 0:00:13
files/78580cf29b5ba1073366a257e1909bfeee43c9f5859e48fb3b2d592028bb58aa_nofilter_compact.parquet
```

### Let the QuackOSM automatically find the required OSM PBF file for you based on text query 🔎📄

#### Load data as a GeoDataFrame

```python
>>> import quackosm as qosm
>>> qosm.convert_osm_extract_to_geodataframe("Vatican City")
                                              tags                      geometry
feature_id
node/4227893563    {'addr:housenumber': '139', ...      POINT (12.45966 41.9039)
node/4227893564    {'amenity': 'fast_food', 'na...     POINT (12.45952 41.90391)
node/4227893565    {'name': 'Ferramenta Pieroni...     POINT (12.46042 41.90385)
node/4227893566    {'amenity': 'ice_cream', 'na...     POINT (12.45912 41.90394)
node/4227893568    {'amenity': 'cafe', 'name': ...     POINT (12.46112 41.90381)
...                                            ...                           ...
relation/2939617   {'building': 'yes', 'type': ...  POLYGON ((12.45269 41.908...
relation/11839271  {'building': 'yes', 'type': ...  POLYGON ((12.44939 41.897...
relation/12988851  {'access': 'private', 'ameni...  POLYGON ((12.45434 41.903...
relation/13571840  {'layer': '1', 'man_made': '...  POLYGON ((12.45132 41.899...
relation/3256168   {'building': 'yes', 'type': ...  POLYGON ((12.46061 41.907...

[8318 rows x 2 columns]
```

#### Just convert OSM extract to GeoParquet

```python
>>> import quackosm as qosm
>>> gpq_path = qosm.convert_osm_extract_to_parquet("Paris", osm_extract_source="OSMfr")
>>> gpq_path.as_posix()
'files/osmfr_europe_france_ile_de_france_paris_nofilter_noclip_compact.parquet'
```

#### Inspect the file with duckdb

```python
>>> import duckdb
>>> duckdb.load_extension('spatial')
>>> duckdb.read_parquet(str(gpq_path)).project(
...     "* REPLACE (ST_GeomFromWKB(geometry) AS geometry)"
... ).order("feature_id")
┌──────────────────┬────────────────────────────┬──────────────────────────────┐
│    feature_id    │            tags            │           geometry           │
│     varchar      │   map(varchar, varchar)    │           geometry           │
├──────────────────┼────────────────────────────┼──────────────────────────────┤
│ node/10000001235 │ {information=guidepost, …  │ POINT (2.3423756 48.8635788) │
│ node/10000001236 │ {barrier=bollard}          │ POINT (2.3423613 48.8635746) │
│ node/10000001237 │ {barrier=bollard}          │ POINT (2.3423555 48.8635657) │
│ node/10000001238 │ {barrier=bollard}          │ POINT (2.34235 48.8635575)   │
│ node/10000001239 │ {barrier=bollard}          │ POINT (2.3423438 48.8635481) │
│ node/10000005002 │ {amenity=vending_machine…  │ POINT (2.3438906 48.8642058) │
│ node/10000005003 │ {addr:city=Paris, addr:h…  │ POINT (2.3441257 48.8642723) │
│ node/10000005297 │ {emergency=fire_hydrant,…  │ POINT (2.2943897 48.8356289) │
│ node/10000034353 │ {name=Elisa&Marie, shop=…  │ POINT (2.3476407 48.8636628) │
│ node/10000079406 │ {emergency=fire_hydrant,…  │ POINT (2.2951077 48.8349097) │
│        ·         │         ·                  │              ·               │
│        ·         │         ·                  │              ·               │
│        ·         │         ·                  │              ·               │
│ node/10180452313 │ {highway=crossing}         │ POINT (2.2668596 48.8351167) │
│ node/10180457217 │ {amenity=charging_statio…  │ POINT (2.2996381 48.8654136) │
│ node/10180457222 │ {advertising=poster_box,…  │ POINT (2.2996126 48.8651971) │
│ node/10180457223 │ {advertising=poster_box,…  │ POINT (2.2990548 48.8651713) │
│ node/10180457224 │ {advertising=poster_box,…  │ POINT (2.3002578 48.8651435) │
│ node/10180457225 │ {advertising=poster_box,…  │ POINT (2.3001396 48.8649086) │
│ node/10180457226 │ {advertising=column, col…  │ POINT (2.3002337 48.8648869) │
│ node/10180457227 │ {advertising=poster_box,…  │ POINT (2.3004355 48.8648103) │
│ node/10180457247 │ {advertising=poster_box,…  │ POINT (2.3006468 48.8647237) │
│ node/10180457248 │ {advertising=poster_box,…  │ POINT (2.3008908 48.8643751) │
├──────────────────┴────────────────────────────┴──────────────────────────────┤
│ ? rows (>9999 rows, 20 shown)                                      3 columns │
└──────────────────────────────────────────────────────────────────────────────┘
```

#### Use as CLI

```console
$ quackosm --osm-extract-query "Gibraltar"
100%|█████████████████████████████████████| 1.57M/1.57M [00:00<00:00, 8.66GB/s]
⠙ [   1/32] Reading nodes • 0:00:00
⠋ [   2/32] Filtering nodes - intersection • 0:00:00
⠙ [   3/32] Filtering nodes - tags • 0:00:00
⠋ [   4/32] Calculating distinct filtered nodes ids • 0:00:00
⠙ [   5/32] Reading ways • 0:00:00
⠋ [   6/32] Unnesting ways • 0:00:00
⠹ [   7/32] Filtering ways - valid refs • 0:00:00
⠋ [   8/32] Filtering ways - intersection • 0:00:00
⠙ [   9/32] Filtering ways - tags • 0:00:00
⠋ [  10/32] Calculating distinct filtered ways ids • 0:00:00
⠙ [  11/32] Reading relations • 0:00:00
⠙ [  12/32] Unnesting relations • 0:00:00
⠼ [  13/32] Filtering relations - valid refs • 0:00:00
⠋ [  14/32] Filtering relations - intersection • 0:00:00
⠙ [  15/32] Filtering relations - tags • 0:00:00
⠙ [  16/32] Calculating distinct filtered relations ids • 0:00:00
⠙ [  17/32] Loading required ways - by relations • 0:00:00
⠙ [  18/32] Calculating distinct required ways ids • 0:00:00
⠙ [  19/32] Saving filtered nodes with geometries • 0:00:00
⠋ [20.1/32] Grouping filtered ways - assigning groups • 0:00:00
⠸ [20.2/32] Grouping filtered ways - joining with nodes • 0:00:10
⠙ [20.3/32] Grouping filtered ways - partitioning by group • 0:00:00
  [  21/32] Saving filtered ways with linestrings 100% ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 1/1 • 0:00:11 < 0:00:00 •
⠙ [22.1/32] Grouping required ways - assigning groups • 0:00:00
⠹ [22.2/32] Grouping required ways - joining with nodes • 0:00:12
⠙ [22.3/32] Grouping required ways - partitioning by group • 0:00:00
  [  23/32] Saving required ways with linestrings 100% ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 1/1 • 0:00:11 < 0:00:00 •
⠹ [  24/32] Saving filtered ways with geometries • 0:00:00
⠸ [  25/32] Saving valid relations parts • 0:00:00
⠋ [  26/32] Saving relations inner parts • 0:00:00
⠋ [  27/32] Saving relations outer parts • 0:00:00
⠙ [  28/32] Saving relations outer parts with holes • 0:00:00
⠙ [  29/32] Saving relations outer parts without holes • 0:00:00
⠹ [  30/32] Saving filtered relations with geometries • 0:00:00
⠹ [  31/32] Saving all features • 0:00:00
  [  32/32] Saving final geoparquet file 100% ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 16/16 • 0:00:00 < 0:00:00 • 163.96 it/s
Finished operation in 0:00:50
files/osmfr_europe_gibraltar_nofilter_noclip_compact.parquet
```

<details>
  <summary>CLI Help output (<code>QuackOSM -h</code>)</summary>

```console
 Usage: QuackOSM [OPTIONS] PBF file path

 QuackOSM CLI.
 Wraps convert_pbf_to_parquet, convert_geometry_to_parquet and convert_osm_extract_to_parquet
 functions and prints final path to the saved geoparquet file at the end.

╭─ Arguments ───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│   pbf_file      PBF file path  PBF file to convert into GeoParquet. Can be an URL. [default: None]                                                                                                                                │
╰───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
╭─ Options ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ --osm-tags-filter                                                           TEXT (JSON)                   OSM tags used to filter the data in the JSON text form. Can take the form of a flat or grouped dict (look:              │
│                                                                                                           OsmTagsFilter and GroupedOsmTagsFilter). Cannot be used together with osm-tags-filter-file.                             │
│                                                                                                           [default: None]                                                                                                         │
│ --osm-tags-filter-file                                                      PATH                          OSM tags used to filter the data in the JSON file form. Can take the form of a flat or grouped dict (look:              │
│                                                                                                           OsmTagsFilter and GroupedOsmTagsFilter). Cannot be used together with osm-tags-filter.                                  │
│                                                                                                           [default: None]                                                                                                         │
│ --keep-all-tags,--all-tags                                                                                Whether to keep all tags while filtering with OSM tags. Doesn't work when there is no OSM tags filter applied           │
│                                                                                                           (osm-tags-filter or osm-tags-filter-file). Will override grouping if GroupedOsmTagsFilter has been passed as a filter.  │
│ --geom-filter-file                                                          PATH                          Geometry to use as a filter in the file format - any that can be opened by GeoPandas. Will return the unary union of    │
│                                                                                                           the geometries in the file. Cannot be used together with geom-filter-geocode or geom-filter-geojson or                  │
│                                                                                                           geom-filter-index-geohash or geom-filter-index-h3 or geom-filter-index-s2 or geom-filter-wkt.                           │
│                                                                                                           [default: None]                                                                                                         │
│ --geom-filter-geocode                                                       TEXT                          Geometry to use as a filter in the string to geocode format - it will be geocoded to the geometry using Nominatim API   │
│                                                                                                           (GeoPy library). Cannot be used together with geom-filter-file or geom-filter-geojson or geom-filter-index-geohash or   │
│                                                                                                           geom-filter-index-h3 or geom-filter-index-s2 or geom-filter-wkt.                                                        │
│                                                                                                           [default: None]                                                                                                         │
│ --geom-filter-geojson                                                       TEXT (GEOJSON)                Geometry to use as a filter in the GeoJSON format. Cannot be used used together with geom-filter-file or                │
│                                                                                                           geom-filter-geocode or geom-filter-index-geohash or geom-filter-index-h3 or geom-filter-index-s2 or geom-filter-wkt.    │
│                                                                                                           [default: None]                                                                                                         │
│ --geom-filter-index-geohash                                                 TEXT (GEOHASH)                Geometry to use as a filter in the Geohash index format. Separate multiple values with a comma. Cannot be used used     │
│                                                                                                           together with geom-filter-file or geom-filter-geocode or geom-filter-geojson or geom-filter-index-h3 or                 │
│                                                                                                           geom-filter-index-s2 or geom-filter-wkt.                                                                                │
│                                                                                                           [default: None]                                                                                                         │
│ --geom-filter-index-h3                                                      TEXT (H3)                     Geometry to use as a filter in the H3 index format. Separate multiple values with a comma. Cannot be used used together │
│                                                                                                           with geom-filter-file or geom-filter-geocode or geom-filter-geojson or geom-filter-index-geohash or                     │
│                                                                                                           geom-filter-index-s2 or geom-filter-wkt.                                                                                │
│                                                                                                           [default: None]                                                                                                         │
│ --geom-filter-index-s2                                                      TEXT (S2)                     Geometry to use as a filter in the S2 index format. Separate multiple values with a comma. Cannot be used used together │
│                                                                                                           with geom-filter-file or geom-filter-geocode or geom-filter-geojson or geom-filter-index-geohash or                     │
│                                                                                                           geom-filter-index-h3 or geom-filter-wkt.                                                                                │
│                                                                                                           [default: None]                                                                                                         │
│ --geom-filter-wkt                                                           TEXT (WKT)                    Geometry to use as a filter in the WKT format. Cannot be used together with geom-filter-file or geom-filter-geocode or  │
│                                                                                                           geom-filter-geojson or geom-filter-index-geohash or geom-filter-index-h3 or geom-filter-index-s2.                       │
│                                                                                                           [default: None]                                                                                                         │
│ --osm-extract-query                                                         TEXT                          Query to find an OpenStreetMap extract from available sources. Will automatically find and download OSM extract. Can be │
│                                                                                                           used instead of PBF file path argument.                                                                                 │
│                                                                                                           [default: None]                                                                                                         │
│ --osm-extract-source,--pbf-download-source                                  [any|Geofabrik|osmfr|BBBike]  Source where to download the PBF file from. Can be Geofabrik, BBBike, OSMfr (OpenStreetMap.fr) or any. [default: (any)] │
│ --explode-tags,--explode                        --compact-tags,--compact                                  Whether to split tags into columns based on the OSM tag keys. If None, it will be set based on the                      │
│                                                                                                           osm-tags-filter/osm-tags-filter-file and keep-all-tags parameters. If there is a tags filter applied without            │
│                                                                                                           keep-all-tags then it'll be set to explode-tags (True). Otherwise it'll be set to compact-tags (False).                 │
│ --output                                    -o                              PATH                          Path where to save final geoparquet file. If not provided, it will be generated automatically based on the input pbf    │
│                                                                                                           file name.                                                                                                              │
│                                                                                                           [default: None]                                                                                                         │
│ --ignore-cache,--no-cache                                                                                 Whether to ignore previously precalculated geoparquet files or not.                                                     │
│ --working-directory,--work-dir                                              PATH                          Directory where to save the parsed parquet and geoparquet files. Will be created if doesn't exist. [default: files]     │
│ --osm-way-polygon-config                                                    PATH                          Config where alternative OSM way polygon features config is defined. Will determine how to parse way features based on  │
│                                                                                                           tags. Option is intended for experienced users. It's recommended to disable cache (no-cache) when using this option,    │
│                                                                                                           since file names don't contain information what config file has been used for file generation.                          │
│                                                                                                           [default: None]                                                                                                         │
│ --filter-osm-ids                                                            TEXT                          List of OSM features IDs to read from the file. Have to be in the form of 'node/<id>', 'way/<id>' or 'relation/<id>'.   │
│                                                                                                           Separate multiple values with a comma.                                                                                  │
│                                                                                                           [default: None]                                                                                                         │
│ --wkt-result,--wkt                                                                                        Whether to save the geometry as a WKT string instead of WKB blob.                                                       │
│ --silent                                                                                                  Whether to disable progress reporting.                                                                                  │
│ --transient                                                                                               Whether to make more transient (concise) progress reporting.                                                            │
│ --iou-threshold                                                             FLOAT RANGE [0<=x<=1]         Minimal value of the Intersection over Union metric for selecting the matching OSM extracts. Is best matching extract   │
│                                                                                                           has value lower than the threshold, it is discarded (except the first one). Has to be in range between 0 and 1. Value   │
│                                                                                                           of 0 will allow every intersected extract, value of 1 will only allow extracts that match the geometry exactly. Works   │
│                                                                                                           only when PbfFileReader is asked to download OSM extracts automatically.                                                │
│                                                                                                           [default: 0.01]                                                                                                         │
│ --allow-uncovered-geometry                                                                                Suppresses an error if some geometry parts aren't covered by any OSM extract. Works only when PbfFileReader is asked to │
│                                                                                                           download OSM extracts automatically.                                                                                    │
│ --show-extracts,--show-osm-extracts                                                                       Show available OSM extracts and exit.                                                                                   │
│ --version                                   -v                                                            Show the application's version and exit.                                                                                │
│ --install-completion                                                                                      Install completion for the current shell.                                                                               │
│ --show-completion                                                                                         Show completion for the current shell, to copy it or customize the installation.                                        │
│ --help                                      -h                                                            Show this message and exit.                                                                                             │
╰───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```
</details>

---

You can find full API + more examples in the [docs](https://kraina-ai.github.io/quackosm/).

## How does it work?

### Basic logic

QuackOSM utilizes `ST_ReadOSM` function from `DuckDB`'s `Spatial` extension to read raw data from the PBF file:

- **Nodes** with coordinates and tags;
- **Ways** with nodes refs and tags;
- **Relations** with nodes and ways refs, ref roles and tags.

Library contains a logic to construct geometries (points, linestrings, polygons) from those raw features.

1. Read nodes from the PBF file, save them to the parquet file.
   1. (Optional) Filter nodes based on geometry filter
   2. (Optional) Filter nodes based on tags filter
2. Read ways from the PBF file, save them to the parquet file.
   1. Select all nodes refs and join them with previously read nodes.
   2. (Optional) Filter ways based on geometry filter - join intersecting nodes
   3. (Optional) Filter ways based on tags filter
3. Read relations from the PBF file, save them to the parquet file.
   1. Select all ways refs and join them with previously read ways.
   2. (Optional) Filter relations based on geometry filter - join intersecting ways
   3. (Optional) Filter relations based on tags filter
4. Select ways required by filtered relations
5. Select nodes required by filtered and required ways
6. Save filtered nodes with point geometries
7. Group ways with nodes geometries and contruct linestrings
8. Save filtered ways with linestrings and polygon geometries (depending on tags values)
9. Divide relation parts into inner and outer polygons
10. Group relation parts into full (multi)polygons and save them
11. Fix invalid geometries
12. Return final GeoParquet file

### Geometry validation

You might ask a question: _How do I know that these geometries are reconstructed correctly?_

To answer this question, the `QuackOSM` has implemented dedicated tests that validate the results of `GDAL` geometries vs `QuackOSM`.
This might come as a surprise, but since OSM geometries aren't always perfectly defined (especially relations), the `QuackOSM` can even fix geometries that are loaded with weird artifacts by `GDAL`.

You can inspect the comparison algorithm in the `test_gdal_parity` function from `tests/base/test_pbf_file_reader.py` file.

### Caching

Library utilizes caching system to reduce repeatable computations.

By default, the library is saving results in the `files` directory created in the working directory. Result file name is generated based on the original `*.osm.pbf` file name.

Original file name to be converted: `example.osm.pbf`.

Default output without any filtering: `example_nofilter_noclip_compact.parquet`.

The nofilter part can be replaced by the hash of OSM tags provided for filtering.
`example_a9dd1c3c2e3d6a94354464e9a1a536ef44cca77eebbd882f48ca52799eb4ca91_noclip_exploded.parquet`

The noclip part can be replaced by the hash of geometry used for filtering.
`example_nofilter_430020b6b1ba7bef8ea919b2fb4472dab2972c70a2abae253760a56c29f449c4_compact.parquet`

The `compact` part can also take the form of `exploded`, it represents the form of OSM tags - either kept together in a single dictionary or split into columns.

When filtering by selecting individual features IDs, an additional hash based on those IDs is appended to the file.
`example_nofilter_noclip_compact_c740a1597e53ae8c5e98c5119eaa1893ddc177161afe8642addcbe54a6dc089d.parquet`

When the `keep_all_tags` parameter is passed while filtering by OSM tags, and additional `alltags` component is added after the osm filter hash part.
`example_a9dd1c3c2e3d6a94354464e9a1a536ef44cca77eebbd882f48ca52799eb4ca91_alltags_noclip_compact.parquet`

General schema of multiple segments that are concatenated together:
`pbf_file_name`\_(`osm_filter_tags_hash_part`/`nofilter`)(\_`alltags`)\_(`clipping_geometry_hash_part`/`noclip`)\_(`compact`/`exploded`)(\_`filter_osm_ids_hash_part`).parquet

> If the WKT mode is turned on, then the result file will be saved with a `_wkt` suffix.

### Memory usage

DuckDB queries requiring `JOIN`, `GROUP` and `ORDER BY` operations are very memory intensive. Because of that, some steps are divided into chunks (groups) with a set number of rows per chunk.

QuackOSM has been roughly tuned to different workloads. The `rows_per_group` variable is set based on an available memory in the system:

|     Memory | Rows per group |
| ---------: | -------------: |
|     < 8 GB |        100 000 |
|  8 - 16 GB |        500 000 |
| 16 - 24 GB |      1 000 000 |
|    > 24 GB |      5 000 000 |

> WSL usage: sometimes code can break since DuckDB is trying to use all available memory, that can be occupied by Windows.

### Resources usage

The algorithm depends on saving intermediate `.parquet` files between queries.
As a rule of thumb, when parsing a full file without filtering, you should have at least 10x more free space on disk than the base file size (100MB pbf file -> 1GB free space to parse it).

Below you can see the chart of resources usage during operation. Generated on a Github Actions Ubuntu virtual machine with 4 threads and 16 GB of memory.

#### Monaco

PBF file size: 525 KB

[Geofabrik link](https://download.geofabrik.de/europe/monaco.html)

![Monaco PBF file result](https://raw.githubusercontent.com/kraina-ai/quackosm/main/docs/assets/images/monaco_disk_spillage.png)

#### Estonia

PBF file size: 100 MB

[Geofabrik link](https://download.geofabrik.de/europe/estonia.html)

![Estonia PBF file result](https://raw.githubusercontent.com/kraina-ai/quackosm/main/docs/assets/images/estonia_disk_spillage.png)

#### Poland

PBF file size: 1.7 GB

[Geofabrik link](https://download.geofabrik.de/europe/poland.html)

![Poland PBF file result](https://raw.githubusercontent.com/kraina-ai/quackosm/main/docs/assets/images/poland_disk_spillage.png)

## License

The library is distributed under Apache-2.0 License.

The free [OpenStreetMap](https://www.openstreetmap.org/) data, which is used for the development of QuackOSM, is licensed under the [Open Data Commons Open Database License](https://opendatacommons.org/licenses/odbl/) (ODbL) by the [OpenStreetMap Foundation](https://osmfoundation.org/) (OSMF).
