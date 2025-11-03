"""
Gunicorn configuration file for optimal Azure App Service performance
This file can be referenced in startup.sh with --config gunicorn.conf.py
"""

import multiprocessing
import os

# Server socket
bind = "0.0.0.0:8000"
backlog = 2048

# Worker processes
# Optimal for Azure: 2-4 workers depending on tier
# Lower workers = less memory, faster startup
workers = int(os.getenv('WEBAPP_WORKERS', 2))
worker_class = 'sync'
worker_connections = 1000
threads = 2  # Threads per worker for I/O-bound operations
timeout = 300  # 5 minutes for long-running data operations
keepalive = 65

# Process naming
proc_name = 'aos-dash-app'

# Server mechanics
preload_app = True  # Load app before forking workers (faster startup)
daemon = False
pidfile = None
umask = 0
user = None
group = None
tmp_upload_dir = None

# Logging
accesslog = '-'
errorlog = '-'
loglevel = os.getenv('LOG_LEVEL', 'info')
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s" %(D)s'

# Process management
max_requests = 1000  # Restart worker after N requests (prevents memory leaks)
max_requests_jitter = 50  # Add randomness to prevent all workers restarting at once

# Server hooks
def on_starting(server):
    """Called just before the master process is initialized."""
    server.log.info("Starting Ahead of the Storm application on Azure")

def when_ready(server):
    """Called just after the server is started."""
    server.log.info(f"Server is ready. Spawning {workers} workers")

def on_exit(server):
    """Called just before exiting Gunicorn."""
    server.log.info("Shutting down: Master process is exiting")

