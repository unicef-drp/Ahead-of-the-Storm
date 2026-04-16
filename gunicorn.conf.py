# Gunicorn configuration for SPCS deployment.
# Settings here supplement the CMD flags in the Dockerfile.
# This file is picked up automatically when gunicorn is invoked as `app:server`.


def post_worker_init(worker):
    """Pre-warm Dash server setup before request threads start.

    Dash's _setup_server (called on the first request) both iterates and
    writes to callback_map.  With --threads 8, multiple threads can hit
    the first request simultaneously and race.  Running it once here, in
    the worker's main thread before any request thread exists, marks the
    server as set up so the race never happens.
    """
    from app import server as flask_app

    with flask_app.test_request_context("/"):
        try:
            flask_app.preprocess_request()
        except Exception:
            pass  # Errors here are expected (no real request context); ignore
