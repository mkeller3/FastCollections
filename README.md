# FastCollections

FastCollections is a api built for serving/anaylzing large geometric tables from PostgreSQL. FastCollections is written in [Python](https://www.python.org/) using the [FastAPI](https://fastapi.tiangolo.com/) web framework. 

---

**Source Code**: <a href="https://github.com/mkeller3/FastCollections" target="_blank">https://github.com/mkeller3/FastCollections</a>

---

## Requirements

FastCollections requires PostGIS >= 2.4.0.

## Configuration

In order for the api to work you will need to add a `.env` file with your database connections.

Example
```bash
DB_HOST=localhost
DB_DATABASE=geoportal
DB_USERNAME=postgres
DB_PASSWORD=postgres
DB_PORT=5432
CACHE_AGE_IN_SECONDS=0
MAX_FEATURES_PER_TILE=100000
```

## Usage

### Running Locally

To run the app locally `uvicorn api.main:app --reload`

### Production
Build Dockerfile into a docker image to deploy to the cloud.

## API Endpoints

| Method | URL                                                                              | Description                                             |
| ------ | -------------------------------------------------------------------------------- | ------------------------------------------------------- |
| `GET`  | `/api/v1/collections`  | [Collections](#collections) |
| `GET`  | `/api/v1/collections/{schema}.{table}`  | [Collection](#collection) |
| `GET`  | `/api/v1/collections/{schema}.{table}/queryables`  | [Queryables](#queryables) |
| `GET`  | `/api/v1/collections/{schema}.{table}/items`  | [Items](#items) |
| `POST`  | `/api/v1/collections/{schema}.{table}/items`  | [Items](#items) |
| `POST`  | `/api/v1/collections/{schema}.{table}/items/create`  | [Create Item](#create-item) |
| `GET`  | `/api/v1/collections/{schema}.{table}/items/{id}`  | [Item](#item) |
| `PUT`  | `/api/v1/collections/{schema}.{table}/items/{id}`  | [Update Item](#update-item) |
| `PATCH`  | `/api/v1/collections/{schema}.{table}/items/{id}`  | [Modify Item](#modify-item) |
| `DELETE`  | `/api/v1/collections/{schema}.{table}/items/{id}`  | [Delete Item](#delete-item) |
| `GET`  | `/api/v1/collections/{schema}.{table}/tiles`  | [Tiles](#tiles) |
| `GET`  | `/api/v1/collections/{schema}.{table}/tiles/{tile_matrix_set_id}/{tile_matrix}/{tile_row}/{tile_col}`  | [Tile](#tile) |
| `GET`  | `/api/v1/collections/{schema}.{table}/tiles/{tile_matrix_set_id}/metadata`  | [Tiles Metadata](#tiles-metadata) |
| `GET`  | `/api/v1/collections/{schema}.{table}/tiles/cache_size`  | [Get Tile Cache Size](#get-tile-cache-size) |
| `DELETE`  | `/api/v1/collections/{schema}.{table}/tiles/cache`  | [Delete Tile Cache](#delete-tile-cache) |
| `POST`  | `/api/v1/collections/{schema}.{table}/statistics`  | [Statistics](#statistics) |
| `POST`  | `/api/v1/collections/{schema}.{table}/bins`  | [Bins](#bins) |
| `POST`  | `/api/v1/collections/{schema}.{table}/numeric_breaks`  | [Numeric Breaks](#numeric-breaks) |
| `POST`  | `/api/v1/collections/{schema}.{table}/custom_break_values`  | [Custom Break Values](#custom-break-values) |
| `GET`  | `/api/v1/collections/{schema}.{table}/autocomplete/{column}/{q}`  | [Autocomplete](#autocomplete) |
| `GET`  | `/api/v1/collections/{schema}.{table}/closest_features`  | [Closest Features](#closest-features) |
| `POST`  | `/api/v1/collections/{schema}.{table}/add_column`  | [Add Column](#add-column) |
| `DELETE`  | `/api/v1/collections/{schema}.{table}/column/{column}`  | [Delete Column](#delete-column) |
| `GET`  | `/api/v1/collections/{schema}.{table}/download`  | [Download](#download) |


## Endpoints

## Collections
Get a list of collections available to query.

```curl
  http://localhost:8000/api/v1/collections
```
## Collection
Get information about a collection.

```curl
  http://localhost:8000/api/v1/collections/{schema}.{table}
```
#### Parameters:

* `schema=schema`

* `table=table`

## Queryables
Get queryable information about a collection.

```curl
  http://localhost:8000/api/v1/collections/{schema}.{table}/queryables
```
#### Parameters:

* `schema=schema`

* `table=table`

## Items
Get geojson from a collection.

```curl
  http://localhost:8000/api/v1/collections/{schema}.{table}/items
```
#### Parameters:

* `schema=schema`

* `table=table`

* `bbox=bbox`

* `limit=limit`

* `offset=offset`

* `properties=properties`

* `sortby=sortby`

* `sortdesc=sortdesc`

* `filter=filter`

* `srid=srid`

* `return_geometry=return_geometry`

## Items
Get geojson from a collection.

```curl
  http://localhost:8000/api/v1/collections/{schema}.{table}/items
```
#### Parameters:

* `schema=schema`

* `table=table`

## Create Item
Create a new item to a collection.

```curl
  http://localhost:8000/api/v1/collections/{schema}.{table}/items/create
```
#### Parameters:

* `schema=schema`

* `table=table`

## Item
Get geojson for one item of a collection.

```curl
  http://localhost:8000/api/v1/collections/{schema}.{table}/items/{id}
```
#### Parameters:

* `schema=schema`

* `table=table`

* `id=id`

* `properties=properties`

* `return_geometry=return_geometry`

* `srid=srid`

## Update Item
Update an item in a collection.

```curl
  http://localhost:8000/api/v1/collections/{schema}.{table}/items/{id}
```
#### Parameters:

* `schema=schema`

* `table=table`

* `id=id`

## Modify Item
Modify an item in a collection.

```curl
  http://localhost:8000/api/v1/collections/{schema}.{table}/items/{id}
```
#### Parameters:

* `schema=schema`

* `table=table`

* `id=id`

## Delete Item
Delete an item in a collection.

```curl
  http://localhost:8000/api/v1/collections/{schema}.{table}/items/{id}
```
#### Parameters:

* `schema=schema`

* `table=table`

* `id=id`

## Tiles
Get tile information about a collection.

```curl
  http://localhost:8000/api/v1/collections/{schema}.{table}/tiles
```
#### Parameters:

* `schema=schema`

* `table=table`

## Tile
Get a vector tile for a given table.

```curl
  http://localhost:8000/api/v1/collections/{schema}.{table}/tiles/{tile_matrix_set_id}/{tile_matrix}/{tile_row}/{tile_col}
```
#### Parameters:

* `schema=schema`

* `table=table`

* `tile_matrix_set_id=tile_matrix_set_id`

* `tile_matrix=tile_matrix`

* `tile_row=tile_row`

* `tile_col=tile_col`

* `fields=fields`

* `cql_filter=cql_filter`

## Tiles Metadata
Get tile metadata for a given table.

```curl
  http://localhost:8000/api/v1/collections/{schema}.{table}/tiles/{tile_matrix_set_id}/metadata
```
#### Parameters:

* `schema=schema`

* `table=table`

* `tile_matrix_set_id=tile_matrix_set_id`

## Get Tile Cache Size
Get size of cache for table.

```curl
  http://localhost:8000/api/v1/collections/{schema}.{table}/tiles/cache_size
```
#### Parameters:

* `schema=schema`

* `table=table`

## Delete Tile Cache
Delete cache for a table.

```curl
  http://localhost:8000/api/v1/collections/{schema}.{table}/tiles/cache
```
#### Parameters:

* `schema=schema`

* `table=table`

## Statistics
Retrieve statistics for a table.

```curl
  http://localhost:8000/api/v1/collections/{schema}.{table}/statistics
```
#### Parameters:

* `schema=schema`

* `table=table`

## Bins
Retrieve a numerical column's bins for a table.

```curl
  http://localhost:8000/api/v1/collections/{schema}.{table}/bins
```
#### Parameters:

* `schema=schema`

* `table=table`

## Numeric Breaks
Retrieve a numerical column's breaks for a table.

```curl
  http://localhost:8000/api/v1/collections/{schema}.{table}/numeric_breaks
```
#### Parameters:

* `schema=schema`

* `table=table`

## Custom Break Values
Retrieve custom break values for a column for a table.

```curl
  http://localhost:8000/api/v1/collections/{schema}.{table}/custom_break_values
```
#### Parameters:

* `schema=schema`

* `table=table`

## Autocomplete
Retrieve distinct values for a column in a table.

```curl
  http://localhost:8000/api/v1/collections/{schema}.{table}/autocomplete/{column}/{q}
```
#### Parameters:

* `schema=schema`

* `table=table`

* `column=column`

* `q=q`

* `limit=limit`

## Closest Features
Get closest features to a latitude and longitude

```curl
  http://localhost:8000/api/v1/collections/{schema}.{table}/closest_features
```
#### Parameters:

* `schema=schema`

* `table=table`

* `latitude=latitude`

* `longitude=longitude`

* `limit=limit`

* `offset=offset`

* `properties=properties`

* `filter=filter`

* `srid=srid`

* `return_geometry=return_geometry`

## Add Column
Create a new column for a table.

```curl
  http://localhost:8000/api/v1/collections/{schema}.{table}/add_column
```
#### Parameters:

* `schema=schema`

* `table=table`

## Delete Column
Delete a column for a table.

```curl
  http://localhost:8000/api/v1/collections/{schema}.{table}/column/{column}
```
#### Parameters:

* `schema=schema`

* `table=table`

* `column=column`

## Download
Download data from a table.

```curl
  http://localhost:8000/api/v1/collections/{schema}.{table}/download
```
#### Parameters:

* `schema=schema`

* `table=table`

* `format=format`

* `file_name=file_name`

* `filter=filter`

* `limit=limit`

* `offset=offset`

* `properties=properties`



