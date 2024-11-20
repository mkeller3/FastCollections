def test_collections(app):
    """
    Test the collections endpoint.
    """
    response = app.get(url="/api/v1/collections")
    body = response.json()
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    assert body["links"]
    assert body["collections"]


def test_collection(app):
    """
    Test the collection endpoint.
    """
    response = app.get(url="/api/v1/collections/public.states")
    body = response.json()
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    assert body["links"]
    assert body["title"] == "public.states"
    assert body["id"] == "public.states"


def test_queryables(app):
    """
    Test the queryables endpoint.
    """
    response = app.get(url="/api/v1/collections/public.states/queryables")
    body = response.json()
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    assert body["title"] == "public.states"
    assert body["properties"]


def test_get_items(app):
    """
    Test the items endpoint.
    """

    # Test cql_filter
    response = app.get(
        url="/api/v1/collections/public.states/items?cql_filter=state_name='New York'"
    )
    body = response.json()
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    assert body["links"]
    assert body["features"]

    # Test properties
    response2 = app.get(
        url="/api/v1/collections/public.states/items?properties=state_name&state_name=New York"
    )
    body2 = response2.json()
    assert response2.status_code == 200
    assert response2.headers["content-type"] == "application/json"
    assert body2["links"]
    assert body2["features"]

    # Test invalid properties
    response3 = app.get(
        url="/api/v1/collections/public.states/items?properties=state_names&cql_filter=state_name='New York'"
    )
    assert response3.status_code == 400

    # Test invalid operator
    response4 = app.get(
        url="/api/v1/collections/public.states/items?properties=state_name&cql_filter==state_name='New York'"
    )
    assert response4.status_code == 400
    assert response4.json() == {"detail": "Invalid operator used in cql_filter."}

    # Test invalid column
    response5 = app.get(
        url="/api/v1/collections/public.states/items?properties=state_name&cql_filter=state_names='New York'"
    )
    assert response5.status_code == 400
    assert response5.json() == {
        "detail": "Invalid column in cql_filter parameter for public.states."
    }

    # Test pagination
    response6 = app.get(url="/api/v1/collections/public.states/items?offset=5&limit=1")
    body6 = response6.json()
    assert response6.status_code == 200
    assert response6.headers["content-type"] == "application/json"
    assert body6["links"]
    assert body6["features"]


def test_post_items(app):
    """
    Test the post items endpoint.
    """

    # Test the cql_filter
    response = app.post(
        url="/api/v1/collections/public.states/items",
        json={"cql_filter": "state_name='New York'"},
    )
    body = response.json()
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    assert body["links"]
    assert body["features"]

    # Test the properties
    response2 = app.post(
        url="/api/v1/collections/public.states/items", json={"properties": "state_name"}
    )
    body2 = response2.json()
    assert response2.status_code == 200
    assert response2.headers["content-type"] == "application/json"
    assert body2["links"]

    # Test invalid properties
    response3 = app.post(
        url="/api/v1/collections/public.states/items",
        json={"properties": "state_names"},
    )
    assert response3.status_code == 400

    # Test invalid operator
    response4 = app.post(
        url="/api/v1/collections/public.states/items",
        json={"cql_filter": "state_name=='New York'"},
    )
    assert response4.status_code == 400
    assert response4.json() == {"detail": "Invalid operator used in cql_filter."}

    # Test invalid column
    response5 = app.post(
        url="/api/v1/collections/public.states/items",
        json={"cql_filter": "state_names='New York'"},
    )
    assert response5.status_code == 400
    assert response5.json() == {
        "detail": "Invalid column in cql_filter parameter for public.states."
    }

    # Test pagination
    response6 = app.post(
        url="/api/v1/collections/public.states/items", json={"offset": 5, "limit": 1}
    )
    body6 = response6.json()
    assert response6.status_code == 200
    assert response6.headers["content-type"] == "application/json"
    assert body6["links"]
    assert body6["features"]


def test_get_item(app):
    """
    Test the item endpoint.
    """
    response = app.get(url="/api/v1/collections/public.states/items/1")
    body = response.json()
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    assert body["links"]
    assert body["properties"]


def test_tiles(app):
    """
    Test the tiles endpoint.
    """
    response = app.get(url="/api/v1/collections/public.states/tiles")
    body = response.json()
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    assert body["links"]
    assert body["tileMatrixSetLinks"]


def test_tile(app):
    """
    Test the tile endpoint.
    """
    response = app.get(
        url="/api/v1/collections/public.states/tiles/WorldCRS84Quad/0/0/0"
    )
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/vnd.mapbox-vector-tile"

    empty_response = app.get(
        url="/api/v1/collections/public.states/tiles/WorldCRS84Quad/4/8/5"
    )
    assert empty_response.status_code == 204
    assert (
        empty_response.headers["content-type"] == "application/vnd.mapbox-vector-tile"
    )


def test_tiles_metadata(app):
    """
    Test the tiles metadata endpoint.
    """
    response = app.get(
        url="/api/v1/collections/public.states/tiles/WorldCRS84Quad/metadata"
    )
    body = response.json()
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    assert body["vector_layers"]


def test_cache_size(app):
    """
    Test the cache size endpoint.
    """
    response = app.get(url="/api/v1/collections/public.states/tiles/cache_size")
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"


def test_delete_tile_cache(app):
    """
    Test the delete tile cache endpoint.
    """
    response = app.delete(url="/api/v1/collections/public.states/tiles/cache")
    assert response.status_code == 200


def test_statistics(app):
    """
    Test the statistics endpoint.
    """

    # Test cql_filter
    response = app.post(
        url="/api/v1/collections/public.states/statistics",
        json={
            "cql_filter": "state_name='New York'",
            "aggregate_columns": [{"column": "state_name", "type": "count"}],
        },
    )
    body = response.json()
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    assert body["results"]["count_state_name"]

    # Test distinct
    response2 = app.post(
        url="/api/v1/collections/public.states/statistics",
        json={
            "aggregate_columns": [
                {
                    "column": "state_name",
                    "type": "distinct",
                    "group_method": "count",
                    "group_column": "state_name",
                }
            ]
        },
    )
    assert response2.status_code == 200
    assert response2.headers["content-type"] == "application/json"

    # Test invalid properties
    response3 = app.post(
        url="/api/v1/collections/public.states/statistics",
        json={"aggregate_columns": [{"column": "state_names", "type": "count"}]},
    )
    assert response3.status_code == 400
    assert response3.json() == {
        "detail": "One of the columns used does not exist for public.states."
    }

    # Test invalid column name with distinct
    response4 = app.post(
        url="/api/v1/collections/public.states/statistics",
        json={
            "aggregate_columns": [
                {
                    "column": "state_names",
                    "type": "distinct",
                    "group_method": "count",
                    "group_column": "state_name",
                }
            ]
        },
    )
    assert response4.status_code == 400
    assert response4.json() == {
        "detail": "One of the columns used does not exist for public.states."
    }

def test_bins(app):
    """
    Test the bins endpoint.
    """
    response = app.post(url="/api/v1/collections/public.states/bins", json={
        "column": "population",
        "number_of_breaks": 10
    })
    body = response.json()
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    assert body["results"]

    response2 = app.post(url="/api/v1/collections/public.states/bins", json={
        "column": "populations",
        "number_of_breaks": 10
    })
    assert response2.status_code == 400

def test_numeric_breaks(app):
    """
    Test the numeric breaks endpoint.
    """
    response = app.post(url="/api/v1/collections/public.states/numeric_breaks", json={
        "column": "population",
        "break_type": "equal_interval",
        "number_of_breaks": 10
    })
    body = response.json()
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    assert body["results"]

    response2 = app.post(url="/api/v1/collections/public.states/numeric_breaks", json={
        "column": "population",
        "break_type": "quantile",
        "number_of_breaks": 10
    })
    body = response2.json()
    assert response2.status_code == 200
    assert response2.headers["content-type"] == "application/json"
    assert body["results"]

    response3 = app.post(url="/api/v1/collections/public.states/numeric_breaks", json={
        "column": "populations",
        "break_type": "equal_interval",
        "number_of_breaks": 10
    })
    assert response3.status_code == 400
    assert response3.json() == {
        "detail": "Column: populations does not exist for public.states."
    }

def test_custom_break_values(app):
    """
    Test the custom break values endpoint.
    """
    response = app.post(url="/api/v1/collections/public.states/custom_break_values", json={
        "column": "population",
        "breaks": [
            {
                "min": 0,
                "max": 100
            },
            {
                "min": 100,
                "max": 200
            }
        ]
    })
    body = response.json()
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    assert body["results"]

    response2 = app.post(url="/api/v1/collections/public.states/custom_break_values", json={
        "column": "populations",
        "breaks": [
            {
                "min": 0,
                "max": 100
            },
            {
                "min": 100,
                "max": 200
            }
        ]
    })
    assert response2.status_code == 400
    assert response2.json() == {
        "detail": "Column: populations does not exist for public.states."
    }

def test_autocomplete(app):
    """
    Test the autocomplete endpoint.
    """
    response = app.get(url="/api/v1/collections/public.states/autocomplete/state_name/New")
    body = response.json()
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    assert body["values"]

    response2 = app.get(url="/api/v1/collections/public.states/autocomplete/state_names/New")
    assert response2.status_code == 400
    assert response2.json() == {
        "detail": "Column: state_names does not exist for public.states."
    }

def test_closest_features(app):
    """
    Test the closest features endpoint.
    """
    response = app.get(url="/api/v1/collections/public.states/closest_features", params={
        "latitude": 40.7128,
        "longitude": -74.006,
        "limit": 1,
        "cql_filter": "state_name='New York'"
    })
    body = response.json()
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    assert body["features"]

    response2 = app.get(url="/api/v1/collections/public.states/closest_features", params={
        "latitude": 40.7128,
        "longitude": -74.006,
        "limit": 1,
        "cql_filter": "state_names='New York'"
    })
    assert response2.status_code == 400
    assert response2.json() == {
        "detail": "Invalid column in cql_filter parameter for public.states."
    }

    response3 = app.get(url="/api/v1/collections/public.states/closest_features", params={
        "latitude": 40.7128,
        "longitude": -74.006,
        "limit": 1,
        "cql_filter": "state_name LI 'New York'"
    })
    assert response3.status_code == 400
    assert response3.json() == {
        "detail": "Invalid operator used in cql_filter."
    }

def test_download(app):
    """
    Test the download endpoint.
    """
    response = app.get(url="/api/v1/collections/public.states/download", params={
        "cql_filter": "state_name='New York'",
        "format": "csv",
        "file_name": "test"
    })
    assert response.status_code == 200

    response2 = app.get(url="/api/v1/collections/public.states/download", params={
        "cql_filter": "state_names='New York'",
        "format": "csv",
        "file_name": "test"
    })
    assert response2.status_code == 400 
    assert response2.json() == {
        "detail": "Invalid column in cql_filter parameter for public.states."
    }

    response3 = app.get(url="/api/v1/collections/public.states/download", params={
        "cql_filter": "state_name LI 'New York'",
        "format": "csv",
        "file_name": "test"
    })
    assert response3.status_code == 400 
    assert response3.json() == {
        "detail": "Invalid operator used in cql_filter."
    }