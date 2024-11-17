"""FastCollections App"""

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from api import models
from api.db import close_db_connection, connect_to_db
from api.routers.collections import router as collections_router
from api.version import __version__

DESCRIPTION = """
A lightweight python api to serve collections from PostGIS.
"""

app = FastAPI(
    title="FastCollections",
    description=DESCRIPTION,
    version=__version__,
    contact={
        "name": "Michael Keller",
        "email": "michaelkeller03@gmail.com",
    },
    license_info={
        "name": "The MIT License (MIT)",
        "url": "https://mit-license.org/",
    },
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(
    collections_router.router,
    prefix="/api/v1/collections",
    tags=["Collections"],
)


# Register Start/Stop application event handler to setup/stop the database connection
@app.on_event("startup")
async def startup_event():
    """Application startup: register the database connection and create table list."""
    await connect_to_db(app)


@app.on_event("shutdown")
async def shutdown_event():
    """Application shutdown: de-register the database connection."""
    await close_db_connection(app)


@app.get("/api/v1/", response_model=models.Landing)
def landing_page(request: Request):
    url = str(request.base_url)

    return {
        "links": [
            {
                "rel": "service-desc",
                "type": "application/vnd.oai.openapi+json;version=3.0",
                "title": "The OpenAPI definition as JSON",
                "href": f"{url}openapi.json",
            },
            {
                "rel": "conformance",
                "type": "application/json",
                "title": "Conformance",
                "href": f"{url}conformance",
            },
            {
                "rel": "data",
                "type": "application/json",
                "title": "Collections",
                "href": f"{url}api/v1/collections",
            },
        ],
        "title": "FastCollection",
    }


@app.get("/conformance", response_model=models.Conformance)
def conformance():
    return {
        "conformsTo": [
            "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/core",
            "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/json",
            "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/landing-page",
            "http://www.opengis.net/spec/ogcapi-common-1/1.0/conf/oas30",
            "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/core",
            "http://www.opengis.net/spec/ogcapi-features-1/1.0/req/oas30",
            "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/geojson",
            "http://www.opengis.net/spec/ogcapi-features-2/1.0/conf/crs",
            "http://www.opengis.net/spec/ogcapi-features-3/1.0/conf/queryables",
            "http://www.opengis.net/spec/ogcapi-features-3/1.0/conf/queryables-query-parameters",
            "http://www.opengis.net/spec/ogcapi-features-4/1.0/conf/create-replace-delete",
            "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/core",
            "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/geodata-tilesets",
            "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/mvt",
            "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/tileset",
            "http://www.opengis.net/spec/ogcapi-tiles-1/1.0/conf/tilesets-list",
        ]
    }


@app.get(
    "/api/v1/health_check", tags=["Health"], response_model=models.HealthCheckResponse
)
async def health():
    """
    Method used to verify server is healthy.
    """

    return {"status": "UP"}
