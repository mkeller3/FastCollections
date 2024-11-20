def test_health_check(app):
    response = app.get(url="/api/v1/health_check")
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    assert response.json() == {"status": "UP"}


def test_landing_page(app):
    response = app.get(url="/api/v1/")
    body = response.json()
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    assert body["links"]
    assert body["title"]


def test_conformance(app):
    response = app.get(url="/conformance")
    body = response.json()
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    assert len(body["conformsTo"]) == 16
