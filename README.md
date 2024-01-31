<p align="center">
  <img width="300" src="https://raw.githubusercontent.com/kraina-ai/quackosm/main/docs/assets/logos/quackosm_logo.png"><br/>
  <small>Generated using DALL·E 3 model with this prompt: A logo for a python library with White background, high quality, 8k. Cute duck and globe with cartography elements. Library for reading OpenStreetMap data using DuckDB.</small>
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
- duckdb (>=0.9.2) - For all DuckDB operations on PBF files
- pyarrow (>=13.0.0) - For parquet files wrangling
- pyarrow-ops - For easy removal of duplicated features in parquet files
- geoarrow-pyarrow (>=0.1.1) - For GeoParquet IO operations
- geopandas - For returning GeoDataFrames and reading Geo files
- shapely (>=2.0) - For parsing WKT and GeoJSON strings and fixing geometries
- typeguard - For internal validation of types
- psutil - For automatic scaling of parameters based on available resources
- pooch - For downloading `*.osm.pbf` files
- tqdm - For showing progress bars
- requests - For iterating OSM PBF files services
- beautifulsoup4 - For parsing HTML files and scraping required information

Optional:
- typer[all] (click, colorama, rich, shellingham) - For CLI
- osmnx - For geocoding of strings in CLI
- h3 - For reading H3 strings in CLI
- h3ronpy - For transforming H3 indexes into geometries
- s2 - For transforming S2 indexes into geometries
- python-geohash - For transforming GeoHash indexes into geometries

## Usage

### If you already have downloaded the PBF file 📁🗺️

#### Load data as a GeoDataFrame
```python
>>> import quackosm as qosm
>>> qosm.get_features_gdf(monaco_pbf_path)
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
>>> gpq_path = qosm.convert_pbf_to_gpq(monaco_pbf_path)
>>> gpq_path.as_posix()
'files/monaco_nofilter_noclip_compact.geoparquet'
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
⠏ [   1/33] Reading nodes • 0:00:00
⠋ [   2/33] Filtering nodes - intersection • 0:00:00
⠋ [   3/33] Filtering nodes - tags • 0:00:00
⠋ [   4/33] Calculating distinct filtered nodes ids • 0:00:00
⠸ [   5/33] Reading ways • 0:00:00
⠙ [   6/33] Unnesting ways • 0:00:00
⠋ [   7/33] Filtering ways - valid refs • 0:00:00
⠋ [   8/33] Filtering ways - intersection • 0:00:00
⠋ [   9/33] Filtering ways - tags • 0:00:00
⠋ [  10/33] Calculating distinct filtered ways ids • 0:00:00
⠋ [  11/33] Reading relations • 0:00:00
⠋ [  12/33] Unnesting relations • 0:00:00
⠋ [  13/33] Filtering relations - valid refs • 0:00:00
⠋ [  14/33] Filtering relations - intersection • 0:00:00
⠋ [  15/33] Filtering relations - tags • 0:00:00
⠋ [  16/33] Calculating distinct filtered relations ids • 0:00:00
⠋ [  17/33] Loading required ways - by relations • 0:00:00
⠋ [  18/33] Calculating distinct required ways ids • 0:00:00
⠙ [  19/33] Saving filtered nodes with geometries • 0:00:00
⠸ [  20/33] Saving required nodes with structs • 0:00:00
⠼ [  21/33] Grouping filtered ways • 0:00:00
  [  22/33] Saving filtered ways with linestrings 100% ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 1/1 • 0:00:00 < 0:00:00 •
⠙ [  23/33] Grouping required ways • 0:00:00
  [  24/33] Saving required ways with linestrings 100% ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 1/1 • 0:00:00 < 0:00:00 •
⠴ [  25/33] Saving filtered ways with geometries • 0:00:00
⠹ [  26/33] Saving valid relations parts • 0:00:00
⠋ [27.1/33] Saving relations inner parts - valid geometries • 0:00:00
⠋ [27.2/33] Saving relations inner parts - invalid geometries • 0:00:00
⠋ [28.1/33] Saving relations outer parts - valid geometries • 0:00:00
⠋ [28.2/33] Saving relations outer parts - invalid geometries • 0:00:00
⠋ [  29/33] Saving relations outer parts with holes • 0:00:00
⠋ [  30/33] Saving relations outer parts without holes • 0:00:00
⠋ [  31/33] Saving filtered relations with geometries • 0:00:00
⠸ [32.1/33] Saving valid features • 0:00:00
⠙ [  33/33] Saving final geoparquet file • 0:00:00
files/monaco_nofilter_noclip_compact.geoparquet
```

### Let the QuackOSM automatically download the required OSM PBF files for you 🔎🌍

#### Load data as a GeoDataFrame
```python
>>> import quackosm as qosm
>>> import osmnx as ox
>>> geometry = ox.geocode_to_gdf("Vatican City").unary_union
>>> qosm.get_features_gdf_from_geometry(geometry)
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
>>> gpq_path = qosm.convert_geometry_to_gpq(geometry)
>>> gpq_path.as_posix()
'files/4b2967088a8fe31cdc15401e29bff9b7b882565cd8143e90443f39f2dc5fe6de_nofilter_compact.geoparquet'
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
100%|██████████████████████████████████████| 45.7M/45.7M [00:00<00:00, 259GB/s]
⠹ [   1/33] Reading nodes • 0:00:03
⠇ [   2/33] Filtering nodes - intersection • 0:00:01
⠇ [   3/33] Filtering nodes - tags • 0:00:01
⠙ [   4/33] Calculating distinct filtered nodes ids • 0:00:00
⠋ [   5/33] Reading ways • 0:00:04
⠧ [   6/33] Unnesting ways • 0:00:02
⠹ [   7/33] Filtering ways - valid refs • 0:00:03
⠴ [   8/33] Filtering ways - intersection • 0:00:02
⠼ [   9/33] Filtering ways - tags • 0:00:00
⠋ [  10/33] Calculating distinct filtered ways ids • 0:00:00
⠦ [  11/33] Reading relations • 0:00:00
⠴ [  12/33] Unnesting relations • 0:00:00
⠼ [  13/33] Filtering relations - valid refs • 0:00:00
⠸ [  14/33] Filtering relations - intersection • 0:00:00
⠙ [  15/33] Filtering relations - tags • 0:00:00
⠋ [  16/33] Calculating distinct filtered relations ids • 0:00:00
⠹ [  17/33] Loading required ways - by relations • 0:00:00
⠋ [  18/33] Calculating distinct required ways ids • 0:00:00
⠇ [  19/33] Saving filtered nodes with geometries • 0:00:02
⠋ [  20/33] Saving required nodes with structs • 0:00:05
⠧ [  21/33] Grouping filtered ways • 0:00:03
  [  22/33] Saving filtered ways with linestrings 100% ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 1/1 • 0:00:00 < 0:00:00 •
⠴ [  23/33] Grouping required ways • 0:00:02
  [  24/33] Saving required ways with linestrings 100% ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 1/1 • 0:00:00 < 0:00:00 •
⠹ [  25/33] Saving filtered ways with geometries • 0:00:01
⠦ [  26/33] Saving valid relations parts • 0:00:00
⠙ [27.1/33] Saving relations inner parts - valid geometries • 0:00:00
⠙ [27.2/33] Saving relations inner parts - invalid geometries • 0:00:00
⠙ [28.1/33] Saving relations outer parts - valid geometries • 0:00:00
⠙ [28.2/33] Saving relations outer parts - invalid geometries • 0:00:00
⠋ [  29/33] Saving relations outer parts with holes • 0:00:00
⠙ [  30/33] Saving relations outer parts without holes • 0:00:00
⠙ [  31/33] Saving filtered relations with geometries • 0:00:00
⠼ [32.1/33] Saving valid features • 0:00:00
⠋ [  33/33] Saving final geoparquet file • 0:00:00
files/9ae2b160eb7556991148f5a2693aaf4b38bbb225c3700a6bfe9e5e54f48b987e_nofilter_compact.geoparquet
```
CLI Help output (`QuackOSM -h`):
![CLI Help output](https://raw.githubusercontent.com/kraina-ai/quackosm/main/docs/assets/images/cli_help.png)

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

Default output without any filtering: `example_nofilter_noclip_compact.geoparquet`.

The nofilter part can be replaced by the hash of OSM tags provided for filtering.
`example_a9dd1c3c2e3d6a94354464e9a1a536ef44cca77eebbd882f48ca52799eb4ca91_noclip_exploded.geoparquet`

The noclip part can be replaced by the hash of geometry used for filtering.
`example_nofilter_430020b6b1ba7bef8ea919b2fb4472dab2972c70a2abae253760a56c29f449c4_compact.geoparquet`

The `compact` part can also take the form of `exploded`, it represents the form of OSM tags - either kept together in a single dictionary or split into columns.

When filtering by selecting individual features IDs, an additional hash based on those IDs is appended to the file.
`example_nofilter_noclip_compact_c740a1597e53ae8c5e98c5119eaa1893ddc177161afe8642addcbe54a6dc089d.geoparquet`

When the `keep_all_tags` parameter is passed while filtering by OSM tags, and additional `alltags` component is added after the osm filter hash part.
`example_a9dd1c3c2e3d6a94354464e9a1a536ef44cca77eebbd882f48ca52799eb4ca91_alltags_noclip_compact.geoparquet`

General schema of multiple segments that are concatenated together:
`pbf_file_name`\_(`osm_filter_tags_hash_part`/`nofilter`)(\_`alltags`)\_(`clipping_geometry_hash_part`/`noclip`)\_(`compact`/`exploded`)(\_`filter_osm_ids_hash_part`).geoparquet

### Memory usage

DuckDB queries requiring `JOIN`, `GROUP` and `ORDER BY` operations are very memory intensive. Because of that, some steps are divided into chunks (groups) with a set number of rows per chunk.

QuackOSM has been roughly tuned to different workloads. The `rows_per_bucket` variable is set based on an available memory in the system:

| Memory     | Rows per group |
|-----------:|---------------:|
|     < 8 GB |        100 000 |
|  8 - 16 GB |        500 000 |
| 16 - 24 GB |      1 000 000 |
|    > 24 GB |      5 000 000 |

> WSL usage: sometimes code can break since DuckDB is trying to use all available memory, that can be occupied by Windows.

### Disk usage

The algorithm depends on saving intermediate `.parquet` files between queries.
As a rule of thumb, when parsing a full file without filtering, you should have at least 10x more free space on disk than the base file size (100MB pbf file -> 1GB free space to parse it).

Below you can see the chart of disk usage during operation. Generated on a machine with Intel i7-4790 CPU with 32 GB of RAM. Red dotted line represents the size of the base file.

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
