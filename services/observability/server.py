import socket
import sys
import os
import uvicorn

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(('127.0.0.1', 8000))
    sock.listen(128)
    
    config = uvicorn.Config(
        "services.observability.main:app",
        log_level="info",
        access_log=False
    )
    server = uvicorn.Server(config)
    server.run(sockets=[sock])

if __name__ == "__main__":
    main()
