"""FastCollections - Models"""

from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Tuple

from geojson_pydantic.features import Feature, FeatureCollection
from pydantic import BaseModel, Field, model_validator
from typing_extensions import Annotated


class AddColumn(BaseModel):
    """Model for adding a column to a table"""

    column_name: str
    column_type: Literal[
        "text", "integer", "bigint", "double precision", "boolean", "time", "uuid"
    ]


class DeleteColumn(BaseModel):
    """Model for deleting a column from a table"""

    column_name: str


class ItemsModel(BaseModel):
    """Model for items endpoint"""

    bbox: str = None
    limit: int = 10
    offset: int = 0
    properties: str = "*"
    sortby: str = "gid"
    sortdesc: int = 1
    cql_filter: str = None
    srid: int = 4326
    return_geometry: bool = True


class AggregateModel(BaseModel):
    """Model for aggregating data on a numerical column for a table"""

    type: Literal["distinct", "avg", "count", "sum", "max", "min"] = None
    column: str
    group_column: Optional[str] = None
    group_method: Optional[str] = None


class StatisticsModel(BaseModel):
    """Model for performing statistics on a numerical column for a table"""

    aggregate_columns: List[AggregateModel]
    cql_filter: str = None


class StatisticsResponseModel(BaseModel):
    """Model for statistics response"""

    results: Dict[str, Any]


class BinsModel(BaseModel):
    """Model for creating bins on a numerical column for a table"""

    cql_filter: str = None
    number_of_bins: int = 10
    column: str


class BreakModel(BaseModel):
    """Model for break"""

    min: float
    max: float
    count: int


class BreaksResponseModel(BaseModel):
    """Model for breaks response"""

    results: List[BreakModel]


class NumericBreaksModel(BaseModel):
    """Model for creating numerical breaks on a numerical column for a table"""

    cql_filter: str = None
    number_of_breaks: int
    column: str
    break_type: Literal["equal_interval", "head_tail", "quantile", "jenk"]


class BinModel(BaseModel):
    """Model for creating bins"""

    min: float
    max: float


class CustomBreaksModel(BaseModel):
    """Model for creating custom breaks on a numerical column for a table"""

    cql_filter: str = None
    column: str
    breaks: List[BinModel]


class AutocompleteModel(BaseModel):
    """Response model for autocomplete"""

    values: List[str]


class Spatial(BaseModel):
    """Spatial Extent model.

    Ref: http://schemas.opengis.net/ogcapi/features/part1/1.0/openapi/schemas/extent.yaml

    """

    # Bbox
    # One or more bounding boxes that describe the spatial extent of the dataset.
    # The first bounding box describes the overall spatial
    # extent of the data. All subsequent bounding boxes describe
    # more precise bounding boxes, e.g., to identify clusters of data.
    bbox: List[List[float]]
    crs: str = "http://www.opengis.net/def/crs/OGC/1.3/CRS84"


class Extent(BaseModel):
    """Extent model.

    Ref: http://schemas.opengis.net/ogcapi/features/part1/1.0/openapi/schemas/extent.yaml

    """

    spatial: Optional[Spatial] = None


class MediaType(str, Enum):
    """Responses Media types formerly known as MIME types."""

    json = "application/json"
    geojson = "application/geo+json"
    mvt = "application/vnd.mapbox-vector-tile"


class Link(BaseModel):
    """Link model.

    Ref: https://github.com/opengeospatial/ogcapi-tiles/blob/master/openapi/schemas/common-core/link.yaml

    Code generated using https://github.com/koxudaxi/datamodel-code-generator/
    """

    href: Annotated[
        str,
        Field(
            json_schema_extra={
                "description": "Supplies the URI to a remote resource (or resource fragment).",
                "examples": ["https://fastcollections/api/v1"],
            }
        ),
    ]
    rel: Annotated[
        str,
        Field(
            json_schema_extra={
                "description": "The type or semantics of the relation.",
                "examples": ["alternate"],
            }
        ),
    ]
    type: Annotated[
        Optional[MediaType],
        Field(
            json_schema_extra={
                "description": "A hint indicating what the media type of the result of dereferencing the link should be.",
                "examples": ["application/geo+json"],
            }
        ),
    ] = None

    title: Annotated[
        Optional[str],
        Field(
            json_schema_extra={
                "description": "Used to label the destination of a link such that it can be used as a human-readable identifier.",
                "examples": ["public.states"],
            }
        ),
    ] = None

    model_config = {"use_enum_values": True}


class Collection(BaseModel):
    """Collection model.

    Note: `CRS` is the list of CRS supported by the service not the CRS of the collection

    Ref: http://schemas.opengis.net/ogcapi/features/part1/1.0/openapi/schemas/collection.yaml

    """

    id: str
    title: Optional[str] = None
    description: Optional[str] = None
    links: List[Link]
    extent: Optional[Extent] = None
    itemType: str = "feature"
    crs: List[str] = ["http://www.opengis.net/def/crs/OGC/1.3/CRS84"]

    model_config = {"extra": "ignore"}


class Collections(BaseModel):
    """
    Collections model.

    Ref: http://beta.schemas.opengis.net/ogcapi/common/part2/0.1/collections/openapi/schemas/collections.yaml

    """

    links: List[Link]
    timeStamp: Optional[str] = None
    numberMatched: Optional[int] = None
    numberReturned: Optional[int] = None
    collections: List[Collection]

    model_config = {"extra": "allow"}


class Queryables(BaseModel):
    """Queryables model.

    Ref: https://docs.ogc.org/DRAFTS/19-079r1.html#filter-queryables

    """

    title: str
    properties: Dict[str, Dict[str, str]]
    type: str = "object"
    schema_name: Annotated[str, Field(alias="$schema")] = (
        "https://json-schema.org/draft/2019-09/schema"
    )
    link: Annotated[str, Field(alias="$id")]

    model_config = {"populate_by_name": True}


class Item(Feature):
    """Item model

    Ref: http://schemas.opengis.net/ogcapi/features/part1/1.0/openapi/schemas/featureGeoJSON.yaml

    """

    links: Optional[List[Link]] = None

    model_config = {"arbitrary_types_allowed": True}


class Items(FeatureCollection):
    """Items model

    Ref: http://schemas.opengis.net/ogcapi/features/part1/1.0/openapi/schemas/featureCollectionGeoJSON.yaml

    """

    id: str
    title: Optional[str] = None
    description: Optional[str] = None
    keywords: Optional[List[str]] = None
    features: List[Item]
    links: Optional[List[Link]] = None
    timeStamp: Optional[str] = None
    numberMatched: Optional[int] = None
    numberReturned: Optional[int] = None

    model_config = {"arbitrary_types_allowed": True}


class TileMatrixSetLink(BaseModel):
    """
    TileMatrixSetLink model.
    Based on http://docs.opengeospatial.org/per/19-069.html#_tilematrixsets
    """

    href: str
    rel: str = "http://www.opengis.net/def/rel/ogc/1.0/tiling-schemes"
    type: MediaType = MediaType.json

    model_config = {"use_enum_values": True}


class TileMatrixSetRef(BaseModel):
    """
    TileMatrixSetRef model.
    Based on http://docs.opengeospatial.org/per/19-069.html#_tilematrixsets
    """

    id: str
    title: Optional[str] = None
    links: List[TileMatrixSetLink]


class LayerJSON(BaseModel):
    """
    https://github.com/mapbox/tilejson-spec/tree/master/3.0.0#33-vector_layers
    """

    id: str
    fields: Annotated[Dict, Field(default_factory=dict)]
    description: Optional[str] = None
    minzoom: Optional[int] = None
    maxzoom: Optional[int] = None


class TileJSON(BaseModel):
    """
    TileJSON model.
    Based on https://github.com/mapbox/tilejson-spec/tree/master/2.2.0
    """

    tilejson: str = "3.0.0"
    name: Optional[str] = None
    description: Optional[str] = None
    version: str = "1.0.0"
    attribution: Optional[str] = None
    template: Optional[str] = None
    legend: Optional[str] = None
    scheme: Literal["xyz", "tms"] = "xyz"
    tiles: List[str]
    vector_layers: Optional[List[LayerJSON]] = None
    grids: Optional[List[str]] = None
    data: Optional[List[str]] = None
    minzoom: int = Field(0)
    maxzoom: int = Field(30)
    fillzoom: Optional[int] = None
    bounds: List[float] = [180, -85.05112877980659, 180, 85.0511287798066]
    center: Optional[Tuple[float, float, int]] = None

    @model_validator(mode="after")
    def compute_center(self):
        """Compute center if it does not exist."""
        bounds = self.bounds
        if not self.center:
            self.center = (
                (bounds[0] + bounds[2]) / 2,
                (bounds[1] + bounds[3]) / 2,
                self.minzoom,
            )
        return self
