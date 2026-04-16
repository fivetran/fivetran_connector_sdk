"""
Integration test fixtures using testcontainers.

Fixtures are module-scoped so that each test module starts the container once
and all tests within the module share it. Containers are skipped cleanly when
Docker is unavailable (CI without Docker, or developer machines without a
running Docker daemon).
"""

import time

import pytest
import requests

# ── Docker availability check ─────────────────────────────────────────────────

def _docker_available() -> bool:
    """Return True if Docker daemon is reachable."""
    try:
        import docker
        docker.from_env().ping()
        return True
    except Exception:
        return False


DOCKER_AVAILABLE = _docker_available()

_docker_skip = pytest.mark.skipif(
    not DOCKER_AVAILABLE,
    reason="Docker daemon is not available — skipping container-based integration test",
)


# ── Wait helper ───────────────────────────────────────────────────────────────

def _wait_for_http(url: str, timeout: int = 90, interval: float = 2.0) -> None:
    """Poll *url* with GET until HTTP 200 is returned or *timeout* seconds elapse."""
    deadline = time.time() + timeout
    last_exc = None
    while time.time() < deadline:
        try:
            r = requests.get(url, timeout=5)
            if r.status_code == 200:
                return
        except Exception as exc:
            last_exc = exc
        time.sleep(interval)
    raise TimeoutError(
        f"Service at {url} did not become healthy within {timeout}s "
        f"(last error: {last_exc})"
    )


# ── Elasticsearch fixture ─────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def es_config():
    """
    Start an Elasticsearch 8.17.0 single-node container and return a config
    dict compatible with connector.update().

    Skipped automatically when Docker is not available.
    """
    if not DOCKER_AVAILABLE:
        pytest.skip("Docker daemon is not available")

    from testcontainers.elasticsearch import ElasticSearchContainer

    image = "docker.elastic.co/elasticsearch/elasticsearch:8.17.0"
    container = ElasticSearchContainer(image=image)
    # The ElasticSearchContainer already sets xpack.security.enabled=false for v8
    # and discovery.type is not needed for single-node when security is off,
    # but set it explicitly to be safe.
    container.with_env("discovery.type", "single-node")
    container.with_env("xpack.security.enabled", "false")

    container.start()

    host_ip = container.get_container_host_ip()
    port = container.get_exposed_port(9200)
    host_url = f"http://{host_ip}:{port}"

    try:
        _wait_for_http(host_url, timeout=120)
    except TimeoutError as e:
        container.stop()
        pytest.skip(f"Elasticsearch container did not become healthy: {e}")

    config = {
        "host": host_url,
        "auth_method": "basic_auth",
        "username": "elastic",
        "password": "",
        "indices": "all",
        "enable_delete_detection": "false",
    }

    yield config

    container.stop()


# ── OpenSearch fixture ────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def opensearch_config():
    """
    Start an OpenSearch 2.18.0 single-node container and return a config dict.

    Skipped automatically when Docker is not available.
    """
    if not DOCKER_AVAILABLE:
        pytest.skip("Docker daemon is not available")

    from testcontainers.core.container import DockerContainer

    container = (
        DockerContainer("opensearchproject/opensearch:2.18.0")
        .with_env("discovery.type", "single-node")
        .with_env("DISABLE_SECURITY_PLUGIN", "true")
        .with_exposed_ports(9200)
    )

    container.start()

    host_ip = container.get_container_host_ip()
    port = container.get_exposed_port(9200)
    host_url = f"http://{host_ip}:{port}"

    try:
        _wait_for_http(host_url, timeout=120)
    except TimeoutError as e:
        container.stop()
        pytest.skip(f"OpenSearch container did not become healthy: {e}")

    config = {
        "host": host_url,
        "auth_method": "basic_auth",
        "username": "admin",
        "password": "",
        "indices": "all",
        "enable_delete_detection": "false",
    }

    yield config

    container.stop()
