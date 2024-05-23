import socket
import time
import threading
import config

def heartbeat():
    while True:
        for loc_id, chunk_server in config.CHUNK_SERVERS.items():
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((chunk_server['host'], chunk_server['port']))
                s.send("heartbeat".encode())
                response = s.recv(1024).decode()
                s.close()
                print(f"Heartbeat response from Chunk Server {loc_id}: {response}")
            except:
                print(f"Chunk Server {loc_id} at {chunk_server['host']}:{chunk_server['port']} is down.")
        time.sleep(config.HEARTBEAT_INTERVAL)

if __name__ == "__main__":
    heartbeat_thread = threading.Thread(target=heartbeat)
    heartbeat_thread.start()
