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
- duckdb (>=0.9.2)
- pyarrow (>=13.0.0)
- geoarrow-pyarrow (>=0.1.1)
- geopandas
- shapely
- typeguard

Optional:
- typer[all] (click, colorama, rich, shellingham)

## Usage
### Load data as a GeoDataFrame
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
### Just convert PBF to GeoParquet
```python
>>> import quackosm as qosm
>>> gpq_path = qosm.convert_pbf_to_gpq(monaco_pbf_path)
>>> gpq_path.as_posix()
'files/monaco_nofilter_noclip_compact.geoparquet'
```
### Inspect the file with duckdb
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
### Use as CLI
```console
$ quackosm monaco.osm.pbf
⠹ [   1/18] Filtering nodes • 0:00:00
⠧ [   2/18] Filtering ways • 0:00:00
⠴ [   3/18] Filtering relations • 0:00:00
⠹ [   4/18] Loading required ways • 0:00:00
⠼ [   5/18] Loading required nodes • 0:00:00
⠙ [   6/18] Saving nodes with geometries • 0:00:00
⠙ [   7/18] Saving filtered nodes with structs • 0:00:00
⠋ [   8/18] Grouping required ways • 0:00:00
  [   9/18] Saving required ways with linestrings 100% ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ 1/1 • 0:00:00 • 0:00:00
⠹ [  10/18] Saving ways with geometries • 0:00:00
⠸ [  11/18] Saving valid relation parts • 0:00:00
⠋ [12.1/18] Saving relation inner parts - valid geometries • 0:00:00
⠋ [12.2/18] Saving relation inner parts - invalid geometries • 0:00:00
⠋ [13.1/18] Saving relation outer parts - valid geometries • 0:00:00
⠋ [13.2/18] Saving relation outer parts - invalid geometries • 0:00:00
⠋ [  14/18] Saving relation outer parts with holes • 0:00:00
⠋ [  15/18] Saving relation outer parts without holes • 0:00:00
⠙ [  16/18] Saving relation with geometries • 0:00:00
⠹ [17.1/18] Saving valid features • 0:00:00
⠋ [  18/18] Saving final geoparquet file • 0:00:00
files/monaco_nofilter_noclip_compact.geoparquet
```

You can find full API + more examples in the [docs](https://kraina-ai.github.io/quackosm/).
