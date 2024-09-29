"""QwikGeo API - Utilities"""

import os
import json
import random
import re
import string
import uuid
import shutil
from functools import reduce
from fastapi.security import OAuth2PasswordBearer
from fastapi import Depends, FastAPI, HTTPException, status
from pygeofilter.backends.sql import to_sql_where
from pygeofilter.parsers.ecql import parse
import asyncpg

from . import config

import_processes = {}

oauth2_scheme = OAuth2PasswordBearer(tokenUrl='token')

async def get_tables_metadata(app: FastAPI) -> list:
    """
    Method used to get tables metadata.
    """
    tables_metadata = []

    pool = app.state.database

    async with pool.acquire() as con:
        tables_query = """
        SELECT schemaname, tablename
        FROM pg_catalog.pg_tables
        WHERE schemaname not in ('pg_catalog','information_schema', 'topology')
        AND tablename != 'spatial_ref_sys'; 
        """
        tables = await con.fetch(tables_query)
        for table in tables:
            tables_metadata.append(
                {
                    "name" : table['tablename'],
                    "schema" : table['schemaname'],
                    "table" : table['tablename'],
                    "type" : "table",
                    "id" : f"{table['schemaname']}.{table['tablename']}"
                }
            )

    return tables_metadata

async def get_tile(
    schema: str,
    table: str,
    tile_matrix_set_id: str,
    z: int,
    x: int,
    y: int,
    fields: str,
    cql_filter: str,
    app: FastAPI
) -> bytes:
    """
    Method to return vector tile from database.

    """

    cache_file = f'{os.getcwd()}/cache/{schema}_{table}/{tile_matrix_set_id}/{z}/{x}/{y}'

    if os.path.exists(cache_file):
        return '', True

    pool = app.state.database

    async with pool.acquire() as con:


        sql_field_query = f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{table}'
        AND table_schema = '{schema}'
        AND column_name != 'geom';
        """

        field_mapping = {}

        db_fields = await con.fetch(sql_field_query)

        for field in db_fields:
            field_mapping[field['column_name']] = field['column_name']

        if fields is None:
            field_list = ""

            for field in db_fields:
                column = field['column_name']
                field_list += f', "{column}"'
        else:
            field_list = f',"{fields}"'

        sql_vector_query = f"""
        SELECT ST_AsMVT(tile, '{schema}.{table}', 4096)
        FROM (
            WITH
            bounds AS (
                SELECT ST_TileEnvelope({z}, {x}, {y}) as geom
            )
            SELECT
                ST_AsMVTGeom(
                    ST_Transform("table".geom, 3857)
                    ,bounds.geom
                ) AS mvtgeom {field_list}
            FROM "{schema}"."{table}" as "table", bounds
            WHERE ST_Intersects(
                ST_Transform("table".geom, 4326),
                ST_Transform(bounds.geom, 4326)
            )

        """
        if cql_filter:
            ast = parse(cql_filter)
            where_statement = to_sql_where(ast, field_mapping)
            sql_vector_query += f" AND {where_statement}"

        sql_vector_query += f"LIMIT {config.MAX_FEATURES_PER_TILE}) as tile"

        tile = await con.fetchval(sql_vector_query)

        if fields is None and cql_filter is None and config.CACHE_AGE_IN_SECONDS > 0:

            cache_file_dir = f'{os.getcwd()}/cache/{schema}_{table}/{tile_matrix_set_id}/{z}/{x}'

            if not os.path.exists(cache_file_dir):
                try:
                    os.makedirs(cache_file_dir)
                except OSError:
                    pass

            with open(cache_file, "wb") as file:
                file.write(tile)
                file.close()

        return tile, False

async def get_table_geometry_type(
    schema: str,
    table: str,
    app: FastAPI
) -> list:
    """
    Method used to retrieve the geometry type for a given table.

    """

    pool = app.state.database

    async with pool.acquire() as con:
        geometry_query = f"""
        SELECT ST_GeometryType(geom) as geom_type
        FROM "{schema}"."{table}"
        """
        try:
            geometry_type = await con.fetchval(geometry_query)
        except asyncpg.exceptions.UndefinedTableError:
            return "unknown"
        
        if geometry_type is None:
            return "unknown"

        geom_type = 'point'

        if 'Polygon' in geometry_type:
            geom_type = 'polygon'
        elif 'Line' in geometry_type:
            geom_type = 'line'

        return geom_type

async def get_table_center(
    schema: str,
    table: str,
    app: FastAPI
) -> list:
    """
    Method used to retrieve the table center for a given table.

    """

    pool = app.state.database

    async with pool.acquire() as con:
        query = f"""
        SELECT ST_X(ST_Centroid(ST_Union(geom))) as x,
        ST_Y(ST_Centroid(ST_Union(geom))) as y
        FROM "{schema}"."{table}"
        """
        center = await con.fetch(query)

        return [center[0][0],center[0][1]]

async def generate_where_clause(
    info: object,
    con,
    no_where: bool=False
) -> str:
    """
    Method to generate where clause.

    """

    query = ""

    if info.filter:
        sql_field_query = f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{info.table}'
            AND table_schema = '{info.schema}'
            AND column_name != 'geom';
        """

        db_fields = await con.fetch(sql_field_query)

        field_mapping = {}

        for field in db_fields:
            field_mapping[field['column_name']] = field['column_name']

        ast = parse(info.filter)
        filter = to_sql_where(ast, field_mapping)

        if no_where is False:
            query += " WHERE "
        else:
            query += " AND "
        query += f" {filter}"

    if info.coordinates and info.geometry_type and info.spatial_relationship:
        if info.filter:
            query += " AND "
        else:
            if no_where is False:
                query += " WHERE "
        if info.geometry_type == 'POLYGON':
            query += f"{info.spatial_relationship}(ST_GeomFromText('{info.geometry_type}(({info.coordinates}))',4326) ,{info.table}.geom)"
        else:
            query += f"{info.spatial_relationship}(ST_GeomFromText('{info.geometry_type}({info.coordinates})',4326) ,{info.table}.geom)"

    return query

async def get_table_columns(
    schema: str,
    table: str,
    app: FastAPI,
    new_table_name: str=None
) -> list:
    """
    Method to return a list of columns for a table.

    """

    pool = app.state.database

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
            if new_table_name:
                column_name = field['column_name']
                fields.append(f"{new_table_name}.{column_name}")
            else:
                fields.append(field['column_name'])

        return fields

async def get_table_geojson(
    schema: str,
    table: str,
    app: FastAPI,
    filter: str=None,
    bbox :str=None,
    limit: int=200000,
    offset: int=0,
    properties: str="*",
    sortby: str="gid",
    sortdesc: int=1,
    srid: int=4326,
    return_geometry: bool=True
) -> object:
    """
    Method used to retrieve the table geojson.

    """

    pool = app.state.database

    async with pool.acquire() as con:
        if return_geometry:
            query = """
            SELECT
            json_build_object(
                'type', 'FeatureCollection',
                'features', json_agg(ST_AsGeoJSON(t.*)::json)
            )
            FROM (
            """

            if properties != '*' and properties != "":
                query += f"SELECT {properties},ST_Transform(geom,{srid})"
            else:
                query += f"SELECT ST_Transform(geom,{srid}), gid"
        
        else:
            if properties != '*' and properties != "":
                query = f"SELECT {properties}, gid"
            else:
                query = f"SELECT gid"
        
        query += f""" FROM "{schema}"."{table}" """

        count_query = f"""SELECT COUNT(*) FROM "{schema}"."{table}" """
        if filter is not None:
            query += f"WHERE {filter}"
            count_query += f"WHERE {filter}"

        if bbox is not None:
            if filter is not None:
                query += " AND "
                count_query += " AND "
            else:
                query += " WHERE "
                count_query += " WHERE "
            coords = bbox.split(',')
            query += f" ST_INTERSECTS(geom,ST_MakeEnvelope({coords[0]}, {coords[1]}, {coords[2]}, {coords[3]}, 4326)) "
            count_query += f" ST_INTERSECTS(geom,ST_MakeEnvelope({coords[0]}, {coords[1]}, {coords[2]}, {coords[3]}, 4326)) "

        if sortby != "gid":
            sort = "asc"
            if sortdesc != 1:
                sort = "desc"
            query += f" ORDER BY {sortby} {sort}"

        query += f" OFFSET {offset} LIMIT {limit}"

        if return_geometry:

            query += ") AS t;"
        try:
            if return_geometry:
                geojson = await con.fetchrow(query)
            else:
                featuresJson = await con.fetch(query)
        except asyncpg.exceptions.InvalidTextRepresentationError as error:
            raise HTTPException(
                status_code=400,
                detail=str(error)
            )
        except asyncpg.exceptions.UndefinedFunctionError as error:
            raise HTTPException(
                status_code=400,
                detail=str(error)
            )
        count = await con.fetchrow(count_query)

        if return_geometry:

            formatted_geojson = json.loads(geojson['json_build_object'])

            if formatted_geojson['features'] is not None:
                for feature in formatted_geojson['features']:
                    if 'st_transform' in feature['properties']:
                        del feature['properties']['st_transform']
                    if 'geom' in feature['properties']:
                        del feature['properties']['geom']
                    feature['id'] = feature['properties']['gid']
                    if properties == "":
                        feature['properties'].pop("gid")
            else:
                formatted_geojson['features'] = []
        else:

            formatted_geojson = {
                "type": "FeatureCollection",
                "features": []
            }

            for feature in featuresJson:
                geojsonFeature = {
                    "type": "Feature",
                    "geometry": None,
                    "properties": {},
                    "id": feature['gid']
                }
                featureProperties = dict(feature)
                for property in featureProperties:
                    if property not in ['geom', 'st_transform']:
                        geojsonFeature['properties'][property] = featureProperties[property]
                if properties == "":
                    geojsonFeature['properties'].pop("gid")
                formatted_geojson['features'].append(geojsonFeature)

        formatted_geojson['numberMatched'] = count['count']
        formatted_geojson['numberReturned'] = 0
        if formatted_geojson['features'] is not None:
            formatted_geojson['numberReturned'] = len(formatted_geojson['features'])

        return formatted_geojson

async def get_table_bounds(
    schema: str,
    table: str,
    app: FastAPI
) -> list:
    """
    Method used to retrieve the bounds for a given table.

    """

    pool = app.state.database

    async with pool.acquire() as con:

        query = f"""
        SELECT ST_Extent(geom)
        FROM "{schema}"."{table}"
        """

        table_extent = []

        try:
            extent = await con.fetchval(query)
        except asyncpg.exceptions.UndefinedTableError:
            return []
        
        if extent is None:
            return []

        extent = extent.replace('BOX(','').replace(')','')

        for corner in extent.split(','):
            table_extent.append(float(corner.split(' ')[0]))
            table_extent.append(float(corner.split(' ')[1]))

        return [table_extent]

def delete_tile_cache(
    schema: str,
    table: str
) -> None:
    """
    Method to remove tile cache for a user's table    

    """

    if os.path.exists(f'{os.getcwd()}/cache/{schema}_{table}'):
        shutil.rmtree(f'{os.getcwd()}/cache/{schema}_{table}')
