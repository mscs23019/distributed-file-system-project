MASTER_PORT = 4531
CHUNK_SIZE = 8
REPLICATION_FACTOR = 2
CHUNK_SERVERS = {
    0: {"host": "localhost", "port": 8010},
    1: {"host": "localhost", "port": 8020},
    2: {"host": "localhost", "port": 8030},
}
HEARTBEAT_INTERVAL = 5  # in seconds
