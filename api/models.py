from enum import Enum
from typing import Annotated, List, Optional

from pydantic import BaseModel, Field


class HealthCheckResponse(BaseModel):
    """FastCollections - HealthCheckResponse"""

    status: bool


class MediaType(str, Enum):
    """Responses Media types formerly known as MIME types."""

    json = "application/json"
    openapi30_json = "application/vnd.oai.openapi+json;version=3.0"


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


class Conformance(BaseModel):
    """Conformance model."""

    conformsTo: List[str]


class Landing(BaseModel):
    """Landing model."""

    title: Optional[str] = None
    description: Optional[str] = None
    links: List[Link]
