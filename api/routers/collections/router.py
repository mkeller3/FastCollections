"""QwikGeo API - Collections"""

import datetime
import json
import os
import shutil
import subprocess
import zipfile
from typing import Literal, Optional

import asyncpg
import lark
from fastapi import APIRouter, HTTPException, Request, Response, status
from geojson_pydantic import Feature, FeatureCollection
from pygeofilter.parsers.ecql import parse
from starlette.responses import FileResponse

import api.routers.collections.models as models
from api import config, utilities
from api.filter.evaluate import to_sql_where

router = APIRouter()


@router.get(path="", response_model=models.Collections)
async def collections(request: Request):
    """
    Get a list of collections available to query.
    """

    url = str(request.base_url)

    db_tables = []

    tables = await utilities.get_tables_metadata(request.app)

    if len(tables) > 0:
        for table in tables:
            db_tables.append(
                {
                    "id": table["id"],
                    "title": table["id"],
                    "description": "",
                    "keywords": [],
                    "links": [
                        {
                            "type": "application/json",
                            "rel": "self",
                            "title": "This document as JSON",
                            "href": f"{url}api/v1/collections/{table['id']}",
                        }
                    ],
                    "geometry": await utilities.get_table_geometry_type(
                        schema=table["schema"], table=table["table"], app=request.app
                    ),
                    "extent": {
                        "spatial": {
                            "bbox": await utilities.get_table_bounds(
                                schema=table["schema"],
                                table=table["table"],
                                app=request.app,
                            ),
                            "crs": "http://www.opengis.net/def/crs/OGC/1.3/CRS84",
                        }
                    },
                    "itemType": "feature",
                }
            )

    return {
        "collections": db_tables,
        "links": [
            {
                "type": "application/json",
                "rel": "self",
                "title": "This document as JSON",
                "href": f"{url}api/v1/collections",
            }
        ],
        "timeStamp": f"{datetime.datetime.utcnow().isoformat()}Z",
        "numberMatched": len(db_tables),
        "numberReturned": len(db_tables),
    }


@router.get(path="/{schema}.{table}", response_model=models.Collection)
async def collection(schema: str, table: str, request: Request):
    """
    Get a collection.
    """

    url = str(request.base_url)

    tile_path = "{tile_matrix_set_id}/{tile_matrix}/{tile_row}/{tile_col}"

    return {
        "id": f"{schema}.{table}",
        "title": f"{schema}.{table}",
        "description": "",
        "keywords": [],
        "links": [
            {
                "type": "application/json",
                "rel": "self",
                "title": "This document as JSON",
                "href": f"{url}api/v1/collections/{schema}.{table}",
            },
            {
                "type": "application/geo+json",
                "rel": "items",
                "title": "Items as GeoJSON",
                "href": f"{url}api/v1/collections/{schema}.{table}items",
            },
            {
                "type": "application/json",
                "rel": "queryables",
                "title": "Queryables for this collection as JSON",
                "href": f"{url}api/v1/collections/{schema}.{table}/queryables",
            },
            {
                "type": "application/json",
                "rel": "tiles",
                "title": "Tiles as JSON",
                "href": f"{url}api/v1/collections/{schema}.{table}/tiles",
            },
            {
                "type": "application/geo+json",
                "rel": "item",
                "title": "Item for this collection",
                "href": f"{url}api/v1/collections/{schema}.{table}/item/{id}",
            },
            {
                "type": "application/json",
                "rel": "tiles",
                "title": "Tiles as JSON",
                "href": f"{url}api/v1/collections/{schema}.{table}/tiles",
            },
            {
                "type": "application/vnd.mapbox-vector-tile",
                "rel": "tile",
                "title": "Tiles as JSON",
                "href": f"{url}api/v1/collections/{schema}.{table}/tiles/{tile_path}",
            },
            {
                "type": "application/json",
                "rel": "cache_size",
                "title": "Size of cache",
                "href": f"{url}api/v1/collections/{schema}.{table}/tiles/cache_size",
            },
        ],
        "geometry": await utilities.get_table_geometry_type(
            schema=schema, table=table, app=request.app
        ),
        "extent": {
            "spatial": {
                "bbox": await utilities.get_table_bounds(
                    schema=schema, table=table, app=request.app
                ),
                "crs": "http://www.opengis.net/def/crs/OGC/1.3/CRS84",
            }
        },
        "itemType": "feature",
    }


@router.get(path="/{schema}.{table}/queryables", response_model=models.Queryables)
async def queryables(schema: str, table: str, request: Request):
    """
    Get queryable information about a collection.
    """

    url = str(request.base_url)

    queryable = {
        "$id": f"{url}api/v1/collections/{schema}.{table}/queryables",
        "title": f"{schema}.{table}",
        "type": "object",
        "$schema": "http://json-schema.org/draft/2019-09/schema",
        "properties": {},
    }

    pool = request.app.state.database

    async with pool.acquire() as con:
        sql_field_query = f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{table}'
            AND table_schema = '{schema}'
            AND column_name != 'geom';
        """

        db_fields = await con.fetch(sql_field_query)

        for field in db_fields:
            data_type = "string"
            if field["data_type"] in config.NUMERIC_FIELDS:
                data_type = "numeric"
            queryable["properties"][field["column_name"]] = {
                "title": field["column_name"],
                "type": data_type,
            }

        return queryable


@router.get(path="/{schema}.{table}/items", response_model=models.Items)
async def items(
    schema: str,
    table: str,
    request: Request,
    bbox: str = None,
    limit: int = 10,
    offset: int = 0,
    properties: str = "*",
    sortby: str = "gid",
    sortdesc: int = 1,
    cql_filter: str = None,
    srid: int = 4326,
    return_geometry: bool = True,
):
    """
    Get geojson from a collection.
    """

    url = str(request.base_url)

    blacklist_query_parameters = [
        "bbox",
        "limit",
        "offset",
        "properties",
        "sortby",
        "sortdesc",
        "cql_filter",
        "srid",
    ]

    new_query_parameters = []

    for query in request.query_params:
        if query not in blacklist_query_parameters:
            new_query_parameters.append(query)

    column_where_parameters = ""

    pool = request.app.state.database

    async with pool.acquire() as con:
        sql_field_query = f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table}'
            AND table_schema = '{schema}';
        """

        db_fields = await con.fetch(sql_field_query)

        fields = []

        for field in db_fields:
            fields.append(field["column_name"])

        if properties == "*":
            properties = ""
            for field in db_fields:
                column = field["column_name"]
                properties += f'"{column}",'
            properties = properties[:-1]
        else:
            if len(properties) > 0:
                for property in properties.split(","):
                    if property not in fields:
                        raise HTTPException(
                            status_code=400,
                            detail=f"""Column: {property} is not a column for {schema}.{table}.""",
                        )

        if new_query_parameters:
            for field in db_fields:
                if field["column_name"] in new_query_parameters:
                    if len(column_where_parameters) != 0:
                        column_where_parameters += " AND "
                    column_where_parameters += f""" {field['column_name']} = '{request.query_params[field['column_name']]}' """

        if cql_filter is not None:
            field_mapping = {}

            for field in db_fields:
                field_mapping[field["column_name"]] = field["column_name"]
            try:
                ast = parse(cql_filter)
            except Exception as exc:
                raise HTTPException(
                    status_code=400, detail="Invalid operator used in cql_filter."
                ) from exc
            try:
                cql_filter = to_sql_where(ast, field_mapping)
            except KeyError as exc:
                raise HTTPException(
                    status_code=400,
                    detail=f"""Invalid column in cql_filter parameter for {schema}.{table}.""",
                ) from exc

        if cql_filter is not None and column_where_parameters != "":
            cql_filter += f" AND {column_where_parameters}"
        else:
            if column_where_parameters != "":
                cql_filter = f"{column_where_parameters}"

        results = await utilities.get_table_geojson(
            schema=schema,
            table=table,
            limit=limit,
            offset=offset,
            properties=properties,
            sortby=sortby,
            sortdesc=sortdesc,
            bbox=bbox,
            cql_filter=cql_filter,
            srid=srid,
            return_geometry=return_geometry,
            app=request.app,
        )

        results["id"] = f"{schema}.{table}"

        results["title"] = f"{schema}.{table}"

        results["timeStamp"] = f"{datetime.datetime.utcnow().isoformat()}Z"

        results["links"] = [
            {
                "type": "application/geo+json",
                "rel": "self",
                "title": "This document as GeoJSON",
                "href": request.url._url,
            },
            {
                "type": "application/json",
                "title": f"{schema}.{table}",
                "rel": "collection",
                "href": f"{url}api/v1/collections/{schema}.{table}",
            },
        ]

        extra_params = ""

        for param in request.query_params:
            if param != "offset":
                extra_params += f"&{param}={request.query_params[param]}"

        if (results["numberReturned"] + offset) < results["numberMatched"]:
            href = (
                f"{str(request.base_url)[:-1]}{request.url.path}?offset={offset+limit}"
            )
            if len(extra_params) > 0:
                href += extra_params
            results["links"].append(
                {
                    "type": "application/geo+json",
                    "rel": "next",
                    "title": "items (next)",
                    "href": href,
                }
            )

        if (offset - limit) > -1:
            href = (
                f"{str(request.base_url)[:-1]}{request.url.path}?offset={offset-limit}"
            )
            if len(extra_params) > 0:
                href += extra_params
            results["links"].append(
                {
                    "type": "application/geo+json",
                    "rel": "prev",
                    "title": "items (prev)",
                    "href": href,
                }
            )

        return results


@router.post(path="/{schema}.{table}/items", response_model=models.Items)
async def post_items(
    schema: str, table: str, request: Request, info: models.ItemsModel
):
    """
    Get geojson from a collection.
    """

    url = str(request.base_url)

    pool = request.app.state.database

    async with pool.acquire() as con:
        sql_field_query = f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table}'
            AND table_schema = '{schema}'
            AND column_name != 'geom';
        """

        db_fields = await con.fetch(sql_field_query)

        fields = []

        for field in db_fields:
            fields.append(field["column_name"])

        if info.properties == "*":
            info.properties = ""
            for field in db_fields:
                column = field["column_name"]
                info.properties += f'"{column}",'
            info.properties = info.properties[:-1]
        else:
            if len(info.properties) > 0:
                for property in info.properties.split(","):
                    if property not in fields:
                        raise HTTPException(
                            status_code=400,
                            detail=f"""Column: {property} is not a column for {schema}.{table}.""",
                        )

        if info.cql_filter is not None:
            field_mapping = {}

            for field in db_fields:
                field_mapping[field["column_name"]] = field["column_name"]
            try:
                ast = parse(info.cql_filter)
            except lark.exceptions.UnexpectedToken as exc:
                raise HTTPException(
                    status_code=400, detail="Invalid operator used in cql_filter."
                ) from exc
            try:
                info.cql_filter = to_sql_where(ast, field_mapping)
            except KeyError as exc:
                raise HTTPException(
                    status_code=400,
                    detail=f"""Invalid column in cql_filter parameter for {schema}.{table}.""",
                ) from exc

        results = await utilities.get_table_geojson(
            schema=schema,
            table=table,
            limit=info.limit,
            offset=info.offset,
            properties=info.properties,
            sortby=info.sortby,
            sortdesc=info.sortdesc,
            bbox=info.bbox,
            cql_filter=info.cql_filter,
            srid=info.srid,
            return_geometry=info.return_geometry,
            app=request.app,
        )

        results["id"] = f"{schema}.{table}"

        results["title"] = f"{schema}.{table}"

        results["timeStamp"] = f"{datetime.datetime.utcnow().isoformat()}Z"

        results["links"] = [
            {
                "type": "application/geo+json",
                "rel": "self",
                "title": "This document as GeoJSON",
                "href": request.url._url,
            },
            {
                "type": "application/json",
                "title": f"{schema}.{table}",
                "rel": "collection",
                "href": f"{url}api/v1/collections/{schema}.{table}",
            },
        ]

        extra_params = ""

        for param in request.query_params:
            if param != "offset":
                extra_params += f"&{param}={request.query_params[param]}"

        if (results["numberReturned"] + info.offset) < results["numberMatched"]:
            href = f"{str(request.base_url)[:-1]}{request.url.path}?offset={info.offset+info.limit}"
            if len(extra_params) > 0:
                href += extra_params
            results["links"].append(
                {
                    "type": "application/geo+json",
                    "rel": "next",
                    "title": "items (next)",
                    "href": href,
                }
            )

        if (info.offset - info.limit) > -1:
            href = f"{str(request.base_url)[:-1]}{request.url.path}?offset={info.offset-info.limit}"
            if len(extra_params) > 0:
                href += extra_params
            results["links"].append(
                {
                    "type": "application/geo+json",
                    "rel": "prev",
                    "title": "items (prev)",
                    "href": href,
                }
            )

        return results


@router.post(path="/{schema}.{table}/items/create", response_model=models.Item)
async def create_item(schema: str, table: str, info: Feature, request: Request):
    """
    Create a new item in a collection.
    """

    pool = request.app.state.database

    async with pool.acquire() as con:
        sql_field_query = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = '{table}'
        AND table_schema = '{schema}'
        AND column_name != 'geom'
        AND column_name != 'gid';
        """

        db_fields = await con.fetch(sql_field_query)

        db_columns = []

        db_column_types = {}

        for field in db_fields:
            db_columns.append(field["column_name"])
            db_column_types[field["column_name"]] = {
                "used": False,
                "type": field["data_type"],
            }

        string_columns = ",".join(db_columns)

        input_columns = ""
        values = ""

        for column in info.properties:
            if column not in db_columns:
                raise HTTPException(
                    status_code=400,
                    detail=f"""Column: {column} is not a column for {schema}.{table}.
                    Please use one of the following columns. {string_columns}""",
                )
            input_columns += f""""{column}","""
            if db_column_types[column]["type"] in config.NUMERIC_FIELDS:
                values += f"""{float(info.properties[column])},"""
            else:
                values += f"""'{info.properties[column]}',"""

            db_column_types[column]["used"] = True

        for column in db_column_types:
            if db_column_types[column]["used"] is False:
                raise HTTPException(
                    status_code=400,
                    detail=f"""Column {column} was not used. Add {column} to your properties.""",
                )

        input_columns = input_columns[:-1]
        values = values[:-1]

        query = f"""
            INSERT INTO "{schema}"."{table}" ({input_columns})
            VALUES ({values})
            RETURNING gid;
        """

        result = await con.fetch(query)

        geojson = {
            "type": info.geometry.type,
            "coordinates": json.loads(json.dumps(info.geometry.coordinates)),
        }

        geom_query = f"""
            UPDATE "{schema}"."{table}"
            SET geom = ST_GeomFromGeoJSON('{json.dumps(geojson)}')
            WHERE gid = {result[0]['gid']};
        """

        await con.fetch(geom_query)

        if os.path.exists(f"{os.getcwd()}/cache/{schema}_{table}"):
            shutil.rmtree(f"{os.getcwd()}/cache/{schema}_{table}")

        info.properties["id"] = result[0]["gid"]

        info.id = result[0]["id"]

        return info


@router.get(path="/{schema}.{table}/items/{id}", response_model=models.Item)
async def item(
    schema: str,
    table: str,
    id: str,
    request: Request,
    properties: str = "*",
    return_geometry: bool = True,
    srid: int = 4326,
):
    """
    Get geojson for one item of a collection.
    """

    url = str(request.base_url)

    pool = request.app.state.database

    async with pool.acquire() as con:
        sql_field_query = f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table}'
            AND table_schema = '{schema}'
            AND column_name != 'geom';
        """

        db_fields = await con.fetch(sql_field_query)

        if properties == "*":
            properties = ""
            for field in db_fields:
                column = field["column_name"]
                properties += f'"{column}",'
            properties = properties[:-1]

        results = await utilities.get_table_geojson(
            schema=schema,
            table=table,
            cql_filter=f"gid = '{id}'",
            properties=properties,
            return_geometry=return_geometry,
            srid=srid,
            app=request.app,
        )

        results["features"][0]["links"] = [
            {
                "type": "application/geo+json",
                "rel": "self",
                "title": "This document as GeoJSON",
                "href": request.url._url,
            },
            {
                "type": "application/geo+json",
                "title": "items as GeoJSON",
                "rel": "items",
                "href": f"{url}api/v1/collections/{schema}.{table}/items",
            },
            {
                "type": "application/json",
                "title": "collection as JSON",
                "rel": "collection",
                "href": f"{url}api/v1/collections/{schema}.{table}",
            },
        ]

        return results["features"][0]


@router.put(path="/{schema}.{table}/items/{id}", response_model=models.Item)
async def update_item(
    schema: str, table: str, id: int, info: Feature, request: Request
):
    """
    Update an item in a collection.
    """

    pool = request.app.state.database

    async with pool.acquire() as con:
        sql_field_query = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = '{table}'
        AND table_schema = '{schema}'
        AND column_name != 'geom'
        AND column_name != 'gid';
        """

        db_fields = await con.fetch(sql_field_query)

        db_columns = []

        db_column_types = {}

        for field in db_fields:
            db_columns.append(field["column_name"])
            db_column_types[field["column_name"]] = {
                "type": field["data_type"],
                "used": False,
            }

        string_columns = ",".join(db_columns)

        exist_query = f"""
        SELECT count(*)
        FROM "{schema}"."{table}"
        WHERE gid = {id}
        """

        exists = await con.fetchrow(exist_query)

        if exists["count"] == 0:
            raise HTTPException(
                status_code=400, detail=f"""Item {info.id} does not exist."""
            )

        query = f"""
            UPDATE "{schema}"."{table}"
            SET 
        """

        for column in info.properties:
            if column not in db_columns:
                raise HTTPException(
                    status_code=400,
                    detail=f"""Column: {column} is not a column for {schema}.{table}.
                    Please use one of the following columns. {string_columns}""",
                )
            if db_column_types[column]["type"] in config.NUMERIC_FIELDS:
                query += f"{column} = {info.properties[column]},"
            else:
                query += f"{column} = '{info.properties[column]}',"

            db_column_types[column]["used"] = True

        for column in db_column_types:
            if db_column_types[column]["used"] is False:
                raise HTTPException(
                    status_code=400,
                    detail=f"""Column {column} was not used. Add {column} to your properties.""",
                )

        query = query[:-1]

        query += f" WHERE gid = {info.id};"

        await con.fetch(query)

        geojson = {
            "type": info.geometry.type,
            "coordinates": json.loads(json.dumps(info.geometry.coordinates)),
        }

        geom_query = f"""
            UPDATE "{schema}"."{table}"
            SET geom = ST_GeomFromGeoJSON('{json.dumps(geojson)}')
            WHERE gid = {id};
        """

        await con.fetch(geom_query)

        if os.path.exists(f"{os.getcwd()}/cache/{schema}_{table}"):
            shutil.rmtree(f"{os.getcwd()}/cache/{schema}_{table}")

        return info


@router.patch(path="/{schema}.{table}/items/{id}", response_model=models.Item)
async def modify_item(
    schema: str, table: str, id: int, info: models.Feature, request: Request
):
    """
    Modify an item in a collection.
    """

    pool = request.app.state.database

    async with pool.acquire() as con:
        sql_field_query = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = '{table}'
        AND table_schema = '{schema}'
        AND column_name != 'geom'
        AND column_name != 'gid';
        """

        db_fields = await con.fetch(sql_field_query)

        db_columns = []

        db_column_types = {}

        for field in db_fields:
            db_columns.append(field["column_name"])
            db_column_types[field["column_name"]] = {"type": field["data_type"]}

        string_columns = ",".join(db_columns)

        exist_query = f"""
        SELECT count(*)
        FROM "{schema}"."{table}"
        WHERE gid = {id}
        """

        exists = await con.fetchrow(exist_query)

        if exists["count"] == 0:
            raise HTTPException(
                status_code=400, detail=f"""Item {info.id} does not exist."""
            )

        query = f"""
            UPDATE "{schema}"."{table}"
            SET 
        """

        for column in info.properties:
            if column not in db_columns:
                raise HTTPException(
                    status_code=400,
                    detail=f"""Column: {column} is not a column for {schema}.{table}.
                    Please use one of the following columns. {string_columns}""",
                )
            if db_column_types[column]["type"] in config.NUMERIC_FIELDS:
                query += f"{column} = {info.properties[column]},"
            else:
                query += f"{column} = '{info.properties[column]}',"

        query = query[:-1]

        query += f" WHERE gid = {id};"

        await con.fetch(query)

        geojson = {
            "type": info.geometry.type,
            "coordinates": json.loads(json.dumps(info.geometry.coordinates)),
        }

        geom_query = f"""
            UPDATE "{schema}"."{table}"
            SET geom = ST_GeomFromGeoJSON('{json.dumps(geojson)}')
            WHERE gid = {id};
        """

        await con.fetch(geom_query)

        if os.path.exists(f"{os.getcwd()}/cache/{schema}_{table}"):
            shutil.rmtree(f"{os.getcwd()}/cache/{schema}_{table}")

        return info


@router.delete(
    path="/{schema}.{table}/items/{id}",
    responses={
        200: {
            "description": "Successful Response",
            "content": {"application/json": {"example": {"status": True}}},
        }
    },
)
async def delete_item(schema: str, table: str, id: int, request: Request):
    """
    Delete an item in a collection.
    """

    pool = request.app.state.database

    async with pool.acquire() as con:
        query = f"""
            DELETE FROM "{schema}"."{table}"
            WHERE gid = {id};
        """

        await con.fetch(query)

        if os.path.exists(f"{os.getcwd()}/cache/{schema}_{table}"):
            shutil.rmtree(f"{os.getcwd()}/cache/{schema}_{table}")

        return {"status": True}


@router.get(path="/{schema}.{table}/tiles", response_model=models.TileMatrixSetRef)
async def tiles(schema: str, table: str, request: Request):
    """
    Get tile information about a collection.
    """

    url = str(request.base_url)

    mvt_path = "{tile_matrix_set_id}/{tile_matrix}/{tile_row}/{tile_col}"

    tile_info = {
        "id": f"{schema}.{table}",
        "title": f"{schema}.{table}",
        "description": "",
        "links": [
            {
                "type": "application/json",
                "rel": "self",
                "title": "This document as JSON",
                "href": f"{url}api/v1/collections/{schema}.{table}/tiles",
            },
            {
                "type": "application/vnd.mapbox-vector-tile",
                "rel": "item",
                "title": "This collection as Mapbox vector tiles",
                "href": f"{url}api/v1/collections/{schema}.{table}/tiles/{mvt_path}",
                "templated": True,
            },
            {
                "type": "application/json",
                "rel": "describedby",
                "title": "Metadata for this collection in the TileJSON format",
                "href": f"{url}api/v1/collections/{schema}.{table}/tiles/{{tile_matrix_set_id}}/metadata",
                "templated": True,
            },
        ],
        "tileMatrixSetLinks": [
            {
                "tileMatrixSet": "WorldCRS84Quad",
                "tileMatrixSetURI": "http://schemas.opengis.net/tms/1.0/json/examples/WorldCRS84Quad.json",
            }
        ],
    }

    return tile_info


@router.get(
    path="/{schema}.{table}/tiles/{tile_matrix_set_id}/{tile_matrix}/{tile_row}/{tile_col}",
    responses={
        200: {
            "description": "Successful Response",
            "content": {"application/vnd.mapbox-vector-tile": {}},
        },
        204: {
            "description": "No Content",
            "content": {"application/vnd.mapbox-vector-tile": {}},
        },
    },
)
async def tile(
    schema: str,
    table: str,
    tile_matrix_set_id: str,
    tile_matrix: int,
    tile_row: int,
    tile_col: int,
    request: Request,
    fields: Optional[str] = None,
    cql_filter: Optional[str] = None,
):
    """
    Get a vector tile for a given table.
    """

    pbf, tile_cache = await utilities.get_tile(
        schema=schema,
        table=table,
        tile_matrix_set_id=tile_matrix_set_id,
        z=tile_matrix,
        x=tile_row,
        y=tile_col,
        fields=fields,
        cql_filter=cql_filter,
        app=request.app,
    )

    response_code = status.HTTP_200_OK

    max_cache_age = config.CACHE_AGE_IN_SECONDS

    if fields is not None and cql_filter is not None:
        max_cache_age = 0

    if tile_cache:
        return FileResponse(
            path=f"{os.getcwd()}/cache/{schema}_{table}/{tile_matrix_set_id}/{tile_matrix}/{tile_row}/{tile_col}",
            media_type="application/vnd.mapbox-vector-tile",
            status_code=response_code,
            headers={"Cache-Control": f"max-age={max_cache_age}", "tile-cache": "true"},
        )

    if pbf == b"":
        response_code = status.HTTP_204_NO_CONTENT

    return Response(
        content=bytes(pbf),
        media_type="application/vnd.mapbox-vector-tile",
        status_code=response_code,
        headers={"Cache-Control": f"max-age={max_cache_age}", "tile-cache": "false"},
    )


@router.get(
    path="/{schema}.{table}/tiles/{tile_matrix_set_id}/metadata",
    response_model=models.TileJSON,
)
async def tiles_metadata(
    schema: str, table: str, tile_matrix_set_id: str, request: Request
):
    """
    Get tile metadata for a given table.
    """

    url = str(request.base_url)

    mvt_path = f"{tile_matrix_set_id}/{{tile_matrix}}/{{tile_row}}/{{tile_col}}"

    table_bounds = await utilities.get_table_bounds(
        schema=schema, table=table, app=request.app
    )

    metadata = {
        "tilejson": "3.0.0",
        "name": f"{schema}.{table}",
        "tiles": [f"{url}api/v1/collections/{schema}.{table}/tiles/{mvt_path}"],
        "minzoom": "0",
        "maxzoom": "22",
        "scheme": "xyz",
        "bounds": table_bounds[0],
        "attribution": None,
        "description": "",
        "vector_layers": [
            {
                "id": f"{schema}.{table}",
                "description": "",
                "minzoom": 0,
                "maxzoom": 22,
                "fields": {},
            }
        ],
    }

    pool = request.app.state.database

    async with pool.acquire() as con:
        sql_field_query = f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{table}'
            AND table_schema = '{schema}'
            AND column_name != 'geom';
        """

        db_fields = await con.fetch(sql_field_query)

        for field in db_fields:
            data_type = "string"
            if field["data_type"] in config.NUMERIC_FIELDS:
                data_type = "numeric"
            metadata["vector_layers"][0]["fields"][field["column_name"]] = data_type

        return metadata


@router.get(
    path="/{schema}.{table}/tiles/cache_size",
    responses={
        200: {
            "description": "Successful Response",
            "content": {"application/json": {"example": {"size_in_gigabytes": 0}}},
        }
    },
)
async def get_tile_cache_size(schema: str, table: str):
    """
    Get size of cache for table.
    """

    size = 0

    cache_path = f"{os.getcwd()}/cache/{schema}_{table}"

    if os.path.exists(cache_path):
        for path, dirs, files in os.walk(cache_path):
            for file in files:
                file_path = os.path.join(path, file)
                size += os.path.getsize(file_path)

    return {"size_in_gigabytes": size * 0.000000001}


@router.delete(
    path="/{schema}.{table}/tiles/cache",
    responses={
        200: {
            "description": "Successful Response",
            "content": {"application/json": {"example": {"status": "deleted"}}},
        }
    },
)
async def delete_tile_cache(
    schema: str,
    table: str,
):
    """
    Delete cache for a table.
    """

    utilities.delete_tile_cache(schema=schema, table=table)

    return {"status": "deleted"}


@router.post(
    path="/{schema}.{table}/statistics", response_model=models.StatisticsResponseModel
)
async def statistics(
    schema: str, table: str, info: models.StatisticsModel, request: Request
):
    """
    Retrieve statistics for a table.
    """

    pool = request.app.state.database

    async with pool.acquire() as con:
        final_results = {}
        cols = []
        col_names = []
        distinct = False
        general_stats = False

        for aggregate in info.aggregate_columns:
            if aggregate.type == "distinct":
                distinct = True
            else:
                general_stats = True
                cols.append(f"""
                {aggregate.type }("{aggregate.column}") as {aggregate.type}_{aggregate.column}
                """)
                col_names.append(f"{aggregate.type}_{aggregate.column}")

        if general_stats:
            formatted_columns = ",".join(cols)
            query = f"""
                SELECT {formatted_columns}
                FROM "{schema}"."{table}"
            """

            query += await utilities.generate_where_clause(
                schema=schema, table=table, info=info, con=con
            )

            try:
                data = await con.fetchrow(query)

            except asyncpg.exceptions.UndefinedColumnError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"One of the columns used does not exist for {schema}.{table}.",
                )

            for col in col_names:
                final_results[col] = data[col]

        if distinct:
            for aggregate in info.aggregate_columns:
                if aggregate.type == "distinct":
                    query = f"""
                    SELECT DISTINCT("{aggregate.column}"), {aggregate.group_method}("{aggregate.group_column}") 
                    FROM "{schema}"."{table}" """

                    query += await utilities.generate_where_clause(
                        schema=schema, table=table, info=info, con=con
                    )

                    query += f"""
                    GROUP BY "{aggregate.column}"
                    ORDER BY "{aggregate.group_method}" DESC"""

                    try:
                        results = await con.fetch(query)

                    except asyncpg.exceptions.UndefinedColumnError:
                        raise HTTPException(
                            status_code=status.HTTP_400_BAD_REQUEST,
                            detail=f"One of the columns used does not exist for {schema}.{table}.",
                        )

                    final_results[
                        f"""distinct_{aggregate.column}_{aggregate.group_method}_{aggregate.group_column}"""
                    ] = [dict(r.items()) for r in results]

        return {"results": final_results}


@router.post(path="/{schema}.{table}/bins", response_model=models.BreaksResponseModel)
async def bins(schema: str, table: str, info: models.BinsModel, request: Request):
    """
    Retrieve a numerical column's bins for a table.
    """

    pool = request.app.state.database

    async with pool.acquire() as con:
        results = []
        query = f"""
            SELECT MIN("{info.column}"),MAX("{info.column}")
            FROM "{schema}"."{table}"
        """

        query += await utilities.generate_where_clause(
            schema=schema, table=table, info=info, con=con
        )

        try:
            data = await con.fetchrow(query)

        except asyncpg.exceptions.UndefinedColumnError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Column: {info.column} does not exist for {schema}.{table}.",
            ) from exc

        group_size = (data["max"] - data["min"]) / info.number_of_bins

        for group in range(info.number_of_bins):
            if group == 0:
                minimum = data["min"]
                maximum = group_size
            else:
                minimum = group * group_size
                maximum = (group + 1) * group_size
            query = f"""
                SELECT COUNT(*)
                FROM "{schema}"."{table}"
                WHERE "{info.column}" > {minimum}
                AND "{info.column}" <= {maximum}
            """

            query += await utilities.generate_where_clause(
                schema=schema, table=table, info=info, con=con, no_where=True
            )

            data = await con.fetchrow(query)

            results.append({"min": minimum, "max": maximum, "count": data["count"]})

        return {"results": results}


@router.post(
    path="/{schema}.{table}/numeric_breaks", response_model=models.BreaksResponseModel
)
async def numeric_breaks(
    schema: str, table: str, info: models.NumericBreaksModel, request: Request
):
    """
    Retrieve a numerical column's breaks for a table.
    """

    pool = request.app.state.database

    async with pool.acquire() as con:
        results = []

        if info.break_type == "quantile":
            query = f"""
                SELECT {info.break_type}_bins(array_agg(CAST("{info.column}" AS integer)), {info.number_of_breaks}) 
                FROM "{schema}"."{table}"
            """
        else:
            query = f"""
                SELECT {info.break_type}_bins(array_agg("{info.column}"), {info.number_of_breaks}) 
                FROM "{schema}"."{table}"
            """

        query += await utilities.generate_where_clause(
            schema=schema, table=table, info=info, con=con
        )

        try:
            break_points = await con.fetchrow(query)

        except asyncpg.exceptions.UndefinedColumnError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Column: {info.column} does not exist for {schema}.{table}.",
            ) from exc

        min_query = f"""
            SELECT MIN("{info.column}")
            FROM "{schema}"."{table}"
        """

        min_query += await utilities.generate_where_clause(
            schema=schema, table=table, info=info, con=con
        )

        min_number = await con.fetchrow(min_query)

        max_query = f"""
            SELECT MAX("{info.column}")
            FROM "{schema}"."{table}"
        """

        max_query += await utilities.generate_where_clause(
            schema=schema, table=table, info=info, con=con
        )

        max_table_number = await con.fetchrow(max_query)

        for index, max_number in enumerate(break_points[f"{info.break_type}_bins"]):
            if index == 0:
                minimum = min_number["min"]
                maximum = max_number
            elif index + 1 == len(break_points[f"{info.break_type}_bins"]):
                minimum = break_points[f"{info.break_type}_bins"][index - 1]
                maximum = max_table_number["max"]
            else:
                minimum = break_points[f"{info.break_type}_bins"][index - 1]
                maximum = max_number
            query = f"""
                SELECT COUNT(*)
                FROM "{schema}"."{table}"
                WHERE "{info.column}" > {minimum}
                AND "{info.column}" <= {maximum}
            """

            query += await utilities.generate_where_clause(
                schema=schema, table=table, info=info, con=con, no_where=True
            )

            data = await con.fetchrow(query)

            results.append({"min": minimum, "max": maximum, "count": data["count"]})

        return {"results": results}


@router.post(
    path="/{schema}.{table}/custom_break_values",
    response_model=models.BreaksResponseModel,
)
async def custom_break_values(
    schema: str, table: str, info: models.CustomBreaksModel, request: Request
):
    """
    Retrieve custom break values for a column for a table.
    """

    pool = request.app.state.database

    async with pool.acquire() as con:
        results = []

        for break_range in info.breaks:
            minimum = break_range.min
            maximum = break_range.max

            query = f"""
                SELECT COUNT(*)
                FROM "{schema}"."{table}"
                WHERE "{info.column}" > {minimum}
                AND "{info.column}" <= {maximum}
            """

            query += await utilities.generate_where_clause(
                schema=schema, table=table, info=info, con=con, no_where=True
            )

            try:
                data = await con.fetchrow(query)

            except asyncpg.exceptions.UndefinedColumnError as exc:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Column: {info.column} does not exist for {schema}.{table}.",
                ) from exc

            results.append({"min": minimum, "max": maximum, "count": data["count"]})

        return {"results": results}


@router.get(
    path="/{schema}.{table}/autocomplete/{column}/{q}",
    response_model=models.AutocompleteModel,
)
async def autocomplete(
    schema: str,
    table: str,
    column: str,
    q: str,
    request: Request,
    limit: int = 10,
):
    """
    Retrieve distinct values for a column in a table.
    """

    pool = request.app.state.database

    async with pool.acquire() as con:
        results = []

        query = f"""
            SELECT distinct("{column}")
            FROM "{schema}"."{table}"
            WHERE "{column}" ILIKE '%{q}%'
            ORDER BY "{column}"
            LIMIT {limit}
        """

        try:
            data = await con.fetch(query)

        except asyncpg.exceptions.UndefinedColumnError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Column: {column} does not exist for {schema}.{table}.",
            )

        for row in data:
            results.append(row[column])

        return {"values": results}


@router.get(path="/{schema}.{table}/closest_features", response_model=FeatureCollection)
async def closest_features(
    schema: str,
    table: str,
    request: Request,
    latitude: float,
    longitude: float,
    limit: int = 10,
    offset: int = 0,
    properties: str = "*",
    cql_filter: str = None,
    srid: int = 4326,
    return_geometry: bool = True,
):
    """
    Get closest features to a latitude and longitude
    """

    properties += f", (geom <-> ST_SetSRID(ST_MakePoint( {longitude}, {latitude} ), 4326)) * 1000 AS distance_in_kilometers"

    if cql_filter is not None:
        db_fields = await utilities.get_table_columns(
            schema=schema, table=table, app=request.app
        )

        field_mapping = {}

        for field in db_fields:
            field_mapping[field] = field
        try:
            ast = parse(cql_filter)
        except lark.exceptions.UnexpectedToken as exc:
            raise HTTPException(
                status_code=400, detail="Invalid operator used in cql_filter."
            ) from exc
        try:
            cql_filter = to_sql_where(ast, field_mapping)
        except KeyError as exc:
            raise HTTPException(
                status_code=400,
                detail=f"""Invalid column in cql_filter parameter for {schema}.{table}.""",
            ) from exc

    results = await utilities.get_table_geojson(
        schema=schema,
        table=table,
        limit=limit,
        offset=offset,
        properties=properties,
        sortby="distance_in_kilometers",
        sortdesc=1,
        bbox=None,
        cql_filter=cql_filter,
        srid=srid,
        return_geometry=return_geometry,
        app=request.app,
    )

    return results


@router.post(
    path="/{schema}.{table}/add_column",
    responses={
        200: {
            "description": "Successful Response",
            "content": {"application/json": {"example": {"status": True}}},
        }
    },
)
async def add_column(request: Request, schema: str, table: str, info: models.AddColumn):
    """
    Create a new column for a table.
    """

    pool = request.app.state.database

    async with pool.acquire() as con:
        query = f"""
            ALTER TABLE "{schema}"."{table}"
            ADD COLUMN "{info.column_name}" {info.column_type};
        """

        await con.fetch(query)

        if os.path.exists(f"{os.getcwd()}/cache/{schema}_{table}"):
            shutil.rmtree(f"{os.getcwd()}/cache/{schema}_{table}")

        return {"status": True}


@router.delete(
    path="/{schema}.{table}/column/{column}",
    responses={
        200: {
            "description": "Successful Response",
            "content": {"application/json": {"example": {"status": True}}},
        }
    },
)
async def delete_column(request: Request, schema: str, table: str, column: str):
    """
    Delete a column for a table.
    """

    pool = request.app.state.database

    async with pool.acquire() as con:
        query = f"""
            ALTER TABLE "{schema}"."{table}"
            DROP COLUMN IF EXISTS "{column}";
        """

        await con.fetch(query)

        if os.path.exists(f"{os.getcwd()}/cache/{schema}_{table}"):
            shutil.rmtree(f"{os.getcwd()}/cache/{schema}_{table}")

        return {"status": True}


@router.get(
    path="/{schema}.{table}/download",
    responses={
        200: {"description": "Successful Response", "content": {"application/zip": {}}}
    },
)
async def download(
    request: Request,
    schema: str,
    table: str,
    format: Literal["csv", "kml", "geojson", "xlsx", "gml", "shp", "tab", "gpkg"],
    file_name: str,
    cql_filter: str = None,
    limit: int = 10,
    offset: int = 0,
    properties: str = "*",
):
    """
    Download data from a table.
    """

    formats = {
        "csv": "CSV",
        "xlsx": "XLSX",
        "geojson": "geojson",
        "kml": "KML",
        "gml": "GML",
        "shp": "ESRI Shapefile",
        "tab": "MapInfo File",
        "gpkg": "GPKG",
    }

    path = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

    folder_name = f"{os.getcwd()}/downloads/{path}"

    if os.path.exists(f"{os.getcwd()}/downloads/") is False:
        os.mkdir(f"{os.getcwd()}/downloads/")

    if os.path.exists(folder_name) is False:
        os.mkdir(folder_name)

    query = f"SELECT {properties} FROM {schema}.{table} "

    if cql_filter is not None:
        field_mapping = {}

        sql_field_query = f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{table}'
            AND table_schema = '{schema}'
            AND column_name != 'geom';
        """

        pool = request.app.state.database

        async with pool.acquire() as con:
            db_fields = await con.fetch(sql_field_query)

            for field in db_fields:
                field_mapping[field["column_name"]] = field["column_name"]
            try:
                ast = parse(cql_filter)
            except lark.exceptions.UnexpectedToken as exc:
                raise HTTPException(
                    status_code=400, detail="Invalid operator used in cql_filter."
                ) from exc
            try:
                cql_filter = to_sql_where(ast, field_mapping)
            except KeyError as exc:
                raise HTTPException(
                    status_code=400,
                    detail=f"""Invalid column in cql_filter parameter for {schema}.{table}.""",
                ) from exc

            query += f"WHERE {cql_filter} "

    query += f"OFFSET {offset} LIMIT {limit}"

    sql_query = f"""ogr2ogr -f "{formats[format]}" {folder_name}/{file_name}.{format} PG:"host={config.DB_HOST} dbname={config.DB_DATABASE} user={config.DB_USERNAME} password={config.DB_PASSWORD}" -sql "{query}" """

    subprocess.call(sql_query, shell=True)

    zip_data = zipfile.ZipFile(f"{folder_name}/{file_name}.zip", "w")

    for subdir, dirs, files in os.walk(folder_name):
        for filename in files:
            filepath = subdir + os.sep + filename
            if ".zip" not in filename:
                new_path = subdir.replace(folder_name, "")
                zip_data.write(filepath, f"{new_path}/{filename}")
    zip_data.close()

    for subdir in os.listdir(f"{os.getcwd()}/downloads/"):
        if subdir[0:16] <= (
            datetime.datetime.today() - datetime.timedelta(minutes=1)
        ).strftime("%Y_%m_%d_%H_%M"):
            shutil.rmtree(f"{os.getcwd()}/downloads/{subdir}")

    return FileResponse(path=f"{folder_name}/{file_name}.zip")
