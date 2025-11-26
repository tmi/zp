import fire
import httpx
import json
import sys

def _get_datasource_id_by_name(grafana_url: str, grafana_user: str, grafana_password: str, datasource_name: str) -> int | None:
    """
    Gets the ID of a Grafana data source by its name.
    """
    api_endpoint = f"{grafana_url}/api/datasources/name/{datasource_name}"
    try:
        response = httpx.get(api_endpoint, auth=(grafana_user, grafana_password))
        response.raise_for_status()
        return response.json().get("id")
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            return None  # Datasource not found
        raise
    except httpx.RequestError:
        raise

def _delete_datasource_by_id(grafana_url: str, grafana_user: str, grafana_password: str, datasource_id: int):
    """
    Deletes a Grafana data source by its ID.
    """
    api_endpoint = f"{grafana_url}/api/datasources/{datasource_id}"
    try:
        response = httpx.delete(api_endpoint, auth=(grafana_user, grafana_password))
        response.raise_for_status()
        print(f"Successfully deleted existing Grafana data source with ID: {datasource_id}")
    except httpx.RequestError:
        raise

def _get_dashboard_uid_by_title(grafana_url: str, grafana_user: str, grafana_password: str, dashboard_title: str) -> str | None:
    """
    Gets the UID of a Grafana dashboard by its title.
    """
    api_endpoint = f"{grafana_url}/api/search?query={dashboard_title}"
    try:
        response = httpx.get(api_endpoint, auth=(grafana_user, grafana_password))
        response.raise_for_status()
        dashboards = response.json()
        for dashboard in dashboards:
            if dashboard.get("title") == dashboard_title:
                return dashboard.get("uid")
        return None
    except httpx.RequestError:
        raise

def _delete_dashboard_by_uid(grafana_url: str, grafana_user: str, grafana_password: str, dashboard_uid: str):
    """
    Deletes a Grafana dashboard by its UID.
    """
    api_endpoint = f"{grafana_url}/api/dashboards/uid/{dashboard_uid}"
    try:
        response = httpx.delete(api_endpoint, auth=(grafana_user, grafana_password))
        response.raise_for_status()
        print(f"Successfully deleted existing Grafana dashboard with UID: {dashboard_uid}")
    except httpx.RequestError:
        raise

def configure_grafana(
    grafana_host: str = "grafana",
    grafana_port: int = 3000,
    grafana_user: str = "admin",
    grafana_password: str = "admin",
    db_host: str = "postgres",
    db_port: int = 5432,
    db_name: str = "postgres",
    db_user: str = "postgres",
    db_password: str = "pgt",
    postgres_datasource_name: str = "PostgreSQL_Kafka_Messages",
    postgres_dashboard_title: str = "Kafka Messages Table",
    kafka_brokers: str = "kafka:9092",
    kafka_topic: str = "t1-json",
    kafka_datasource_name: str = "Kafka_t1_json",
    kafka_dashboard_title: str = "Latest 100 Kafka Messages"
):
    """
    Configures a PostgreSQL data source and creates a dashboard in Grafana.

    Args:
        grafana_host: Grafana hostname.
        grafana_port: Grafana port.
        grafana_user: Grafana username.
        grafana_password: Grafana password.
        db_host: PostgreSQL hostname.
        db_port: PostgreSQL port.
        db_name: PostgreSQL database name.
        db_user: PostgreSQL username.
        db_password: PostgreSQL password.
        datasource_name: Name for the Grafana data source.
        dashboard_title: Title for the Grafana dashboard.
    """

    grafana_url = f"http://{grafana_host}:{grafana_port}"
    headers = {
        "Content-Type": "application/json"
    }

    # --- Datasource Configuration (PostgreSQL) ---
    try:
        existing_datasource_id = _get_datasource_id_by_name(grafana_url, grafana_user, grafana_password, postgres_datasource_name)
        if existing_datasource_id:
            print(f"Data source '{postgres_datasource_name}' already exists. Deleting it...")
            _delete_datasource_by_id(grafana_url, grafana_user, grafana_password, existing_datasource_id)
    except Exception as e:
        print(f"Error checking or deleting existing PostgreSQL data source: {e}")
        sys.exit(1)

    datasource_config = {
        "name": postgres_datasource_name,
        "type": "postgres",
        "access": "proxy",
        "isDefault": True,
        "url": f"{db_host}:{db_port}",
        "database": db_name,
        "user": db_user,
        "secureJsonData": {
            "password": db_password
        },
        "jsonData": {
            "sslmode": "disable",
            "timescaledb": False,
            "database": db_name
        }
    }

    try:
        print(f"Attempting to configure Grafana PostgreSQL data source at {grafana_url}/api/datasources...")
        response = httpx.post(
            f"{grafana_url}/api/datasources",
            headers=headers,
            json=datasource_config,
            auth=(grafana_user, grafana_password)
        )
        response.raise_for_status()

        print("Grafana PostgreSQL data source configured successfully!")
        print(response.json())

    except httpx.HTTPStatusError as e:
        print(f"Error configuring Grafana PostgreSQL data source: {e.response.status_code} - {e.response.text}")
        sys.exit(1)
    except httpx.RequestError as e:
        print(f"An error occurred while requesting {e.request.url!r}: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred during PostgreSQL datasource configuration: {e}")
        sys.exit(1)

    # --- Dashboard Configuration (PostgreSQL) ---
    try:
        existing_dashboard_uid = _get_dashboard_uid_by_title(grafana_url, grafana_user, grafana_password, postgres_dashboard_title)
        if existing_dashboard_uid:
            print(f"Dashboard '{postgres_dashboard_title}' already exists. Deleting it...")
            _delete_dashboard_by_uid(grafana_url, grafana_user, grafana_password, existing_dashboard_uid)
    except Exception as e:
        print(f"Error checking or deleting existing PostgreSQL dashboard: {e}")
        sys.exit(1)

    dashboard_config = {
        "dashboard": {
            "id": None,
            "uid": None,
            "title": postgres_dashboard_title,
            "panels": [
                {
                    "datasource": {
                        "type": "postgres",
                        "name": postgres_datasource_name
                    },
                    "fieldConfig": {
                        "defaults": {
                            "custom": {
                                "align": "auto",
                                "displayMode": "auto"
                            },
                            "mappings": [],
                            "thresholds": {
                                "mode": "absolute",
                                "steps": [
                                    {
                                        "color": "rgba(50, 172, 45, 0.97)",
                                        "value": None
                                    },
                                    {
                                        "color": "rgba(237, 129, 40, 0.89)",
                                        "value": 80
                                    }
                                ]
                            },
                            "unit": "none"
                        },
                        "overrides": []
                    },
                    "gridPos": {
                        "h": 9,
                        "w": 12,
                        "x": 0,
                        "y": 0
                    },
                    "id": 1,
                    "options": {
                        "colorMode": "cell",
                        "footer": {
                            "countRows": False,
                            "reducer": [
                                "sum"
                            ]
                        },
                        "showHeader": True,
                        "sortBy": [],
                        "viewMode": "table"
                    },
                    "pluginVersion": "10.4.1",
                    "targets": [
                        {
                            "datasource": {
                                "type": "postgres",
                                "name": postgres_datasource_name
                            },
                            "format": "table",
                            "group": [],
                            "rawSql": "SELECT timestamp, key, value FROM kafka_messages ORDER BY timestamp DESC",
                            "refId": "A",
                            "timeColumn": "timestamp",
                            "timeColumnType": "timestamp",
                            "schema": "public",
                            "table": "kafka_messages"
                        }
                    ],
                    "title": "Kafka Messages",
                    "type": "table"
                }
            ],
            "schemaVersion": 39,
            "style": "dark",
            "tags": [],
            "templating": {
                "list": []
            },
            "time": {
                "from": "now-6h",
                "to": "now"
            },
            "timepicker": {
                "refresh_intervals": [
                  "100ms",
                  "1s",
                  "5s",
                  "10s",
                ]
            },
            "timezone": "",
            "version": 1
        },
        "folderId": 0,
        "overwrite": True
    }




    try:
        print(f"Attempting to create Grafana dashboard at {grafana_url}/api/dashboards/db...")
        response = httpx.post(
            f"{grafana_url}/api/dashboards/db",
            headers=headers,
            json=dashboard_config,
            auth=(grafana_user, grafana_password)
        )
        response.raise_for_status()

        print("Grafana dashboard created successfully!")
        print(response.json())

    except httpx.HTTPStatusError as e:
        print(f"Error creating Grafana dashboard: {e.response.status_code} - {e.response.text}")
        sys.exit(1)
    except httpx.RequestError as e:
        print(f"An error occurred while requesting {e.request.url!r}: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred during dashboard creation: {e}")
        sys.exit(1)


    # --- Datasource Configuration (Kafka) ---
    try:
        existing_datasource_id = _get_datasource_id_by_name(grafana_url, grafana_user, grafana_password, kafka_datasource_name)
        if existing_datasource_id:
            print(f"Data source '{kafka_datasource_name}' already exists. Deleting it...")
            _delete_datasource_by_id(grafana_url, grafana_user, grafana_password, existing_datasource_id)
    except Exception as e:
        print(f"Error checking or deleting existing Kafka data source: {e}")
        sys.exit(1)

    kafka_datasource_config = {
        "name": kafka_datasource_name,
        "type": "hamedkarbasi93-kafka-datasource",
        "access": "proxy",
        "isDefault": False, # Not setting as default to keep Postgres as default
        "jsonData": {
            "bootstrapServers": kafka_brokers,
            "defaultTopic": kafka_topic,
            "connectionMaxIdleMs": 300000,
            "timeoutMs": 10000,
            "tls": False,
            "sasl": False
        },
        "secureJsonData": {}
    }

    try:
        print(f"Attempting to configure Grafana Kafka data source at {grafana_url}/api/datasources...")
        response = httpx.post(
            f"{grafana_url}/api/datasources",
            headers=headers,
            json=kafka_datasource_config,
            auth=(grafana_user, grafana_password)
        )
        response.raise_for_status()

        print("Grafana Kafka data source configured successfully!")
        print(response.json())

    except httpx.HTTPStatusError as e:
        print(f"Error configuring Grafana Kafka data source: {e.response.status_code} - {e.response.text}")
        sys.exit(1)
    except httpx.RequestError as e:
        print(f"An error occurred while requesting {e.request.url!r}: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred during Kafka datasource configuration: {e}")
        sys.exit(1)

    # --- Dashboard Configuration (Kafka) ---
    try:
        existing_dashboard_uid = _get_dashboard_uid_by_title(grafana_url, grafana_user, grafana_password, kafka_dashboard_title)
        if existing_dashboard_uid:
            print(f"Dashboard '{kafka_dashboard_title}' already exists. Deleting it...")
            _delete_dashboard_by_uid(grafana_url, grafana_user, grafana_password, existing_dashboard_uid)
    except Exception as e:
        print(f"Error checking or deleting existing Kafka dashboard: {e}")
        sys.exit(1)

    kafka_dashboard_config = {
        "dashboard": {
            "id": None,
            "uid": None,
            "title": kafka_dashboard_title,
            "panels": [
                {
                    "datasource": {
                        "type": "hamedkarbasi93-kafka-datasource",
                        "name": kafka_datasource_name
                    },
                    "fieldConfig": {
                        "defaults": {
                            "custom": {},
                            "mappings": [],
                            "thresholds": {
                                "mode": "absolute",
                                "steps": [
                                    {
                                        "color": "rgba(50, 172, 45, 0.97)",
                                        "value": None
                                    },
                                    {
                                        "color": "rgba(237, 129, 40, 0.89)",
                                        "value": 80
                                    }
                                ]
                            },
                            "unit": "none"
                        },
                        "overrides": []
                    },
                    "gridPos": {
                        "h": 9,
                        "w": 12,
                        "x": 0,
                        "y": 0
                    },
                    "id": 1,
                    "options": {
                        "colorMode": "value",
                        "footer": {
                            "countRows": False,
                            "reducer": [
                                "sum"
                            ]
                        },
                        "showHeader": True,
                        "sortBy": [],
                        "viewMode": "table"
                    },
                    "pluginVersion": "12.2.1",
                    "targets": [
                        {
                            "autoOffsetReset": "lastN",
                            "datasource": {
                                "name": kafka_datasource_name,
                                "type": "hamedkarbasi93-kafka-datasource"
                            },
                            "lastN": 100,
                            "limit": 100,
                            "partition": "all",
                            "queryType": "streaming",
                            "refId": "A",
                            "timestampMode": "message",
                            "topic": kafka_topic,
                            "topicName": kafka_topic,
                            "valueType": "string"
                        }
                    ],
                    "title": "Latest Kafka Messages",
                    "type": "table"
                }
            ],
            "schemaVersion": 39,
            "style": "dark",
            "tags": [],
            "templating": {
                "list": []
            },
            "time": {
                "from": "now-5m", # Display messages from the last 5 minutes
                "to": "now"
            },
            "timepicker": {
                "refresh_intervals": [
                  "100ms",
                  "1s",
                  "5s",
                  "10s",
                ]
            },
            "timezone": "",
            "version": 1
        },
        "folderId": 0,
        "overwrite": True
    }

    try:
        print(f"Attempting to create Grafana Kafka dashboard at {grafana_url}/api/dashboards/db...")
        response = httpx.post(
            f"{grafana_url}/api/dashboards/db",
            headers=headers,
            json=kafka_dashboard_config,
            auth=(grafana_user, grafana_password)
        )
        response.raise_for_status()

        print("Grafana Kafka dashboard created successfully!")
        print(response.json())

    except httpx.HTTPStatusError as e:
        print(f"Error creating Grafana Kafka dashboard: {e.response.status_code} - {e.response.text}")
        sys.exit(1)
    except httpx.RequestError as e:
        print(f"An error occurred while requesting {e.request.url!r}: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred during Kafka dashboard creation: {e}")
        sys.exit(1)

if __name__ == '__main__':
    fire.Fire(configure_grafana)
