import dagster as dg
from dagster_duckdb import DuckDBResource
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dagster_dlt import DagsterDltResource
from dagster_sling import SlingConnectionResource, SlingResource


class NASAResource(dg.ConfigurableResource):
    api_key: str

    def get_near_earth_asteroids(self, start_date: str, end_date: str):
        url = "https://api.nasa.gov/neo/rest/v1/feed"
        params = {
            "start_date": start_date,
            "end_date": end_date,
            "api_key": self.api_key,
        }

        # Retries
        session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=0.5,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("https://", adapter)

        resp = session.get(url, params=params)
        return resp.json()["near_earth_objects"][start_date]

source = SlingConnectionResource(
    name="MY_POSTGRES",
    type="postgres",
    host="localhost",
    port=5432,
    database="test_db",
    user="test_user",
    password="test_pass",
)

destination = SlingConnectionResource(
    name="MY_DUCKDB",
    type="duckdb",
    connection_string="duckdb:///var/tmp/duckdb.db",
)

sling = SlingResource(
    connections=[
        source,
        destination,
    ]
)

@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "nasa": NASAResource(
                api_key=dg.EnvVar("NASA_API_KEY"),
            ),
            "database": DuckDBResource(
                database="data/staging/data.duckdb",
            ),
            "dlt": DagsterDltResource(),
            "sling": sling,
        },
    )