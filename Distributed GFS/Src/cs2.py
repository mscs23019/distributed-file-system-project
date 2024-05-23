import os
import rpyc
from rpyc.utils.server import ThreadedServer

DATA_DIR = os.path.expanduser("~/gfs_root/cs2")

class GFSChunkService(rpyc.Service):
    class exposed_GFSChunkServer:
        def exposed_write_data(self, chunk_id, data):
            local_filename = self.chunk_filename(chunk_id)
            with open(local_filename, "w") as file:
                file.write(data)
            print(f"Data written to chunk {chunk_id}")

        def exposed_get_data(self, chunk_id):
            local_filename = self.chunk_filename(chunk_id)
            with open(local_filename, "r") as file:
                data = file.read()
            return data

        def exposed_delete_data(self, chunk_id):
            local_filename = self.chunk_filename(chunk_id)
            if os.path.isfile(local_filename):
                os.remove(local_filename)
                print(f"Data deleted from chunk {chunk_id}")

        def chunk_filename(self, chunk_id):
            return os.path.join(DATA_DIR, f"{chunk_id}.gfs")

if __name__ == "__main__":
    print("GFSChunkServer is Running on port 8020!")
    if not os.path.isdir(DATA_DIR):
        os.makedirs(DATA_DIR)
    t = ThreadedServer(GFSChunkService, port=8020)
    t.start()
