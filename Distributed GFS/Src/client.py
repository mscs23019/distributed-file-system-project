import sys
import functools
from urllib.parse import urlparse
import rpyc
import logging

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)

class GFSClient:
    def __init__(self, master):
        self.master = master
        self.chunk_servers = self.master.get_chunk_servers()

    def __get_host_port(self, loc_id):
        chunk_server = self.chunk_servers[loc_id]
        host = chunk_server["host"]
        port = chunk_server["port"]
        return host, port

    def __num_of_chunks(self, file_size):
        chunk_size = self.master.get_chunk_size()
        return (file_size // chunk_size) + (1 if file_size % chunk_size > 0 else 0)

    def __write_chunks(self, chunk_ids, data):
        chunk_size = self.master.get_chunk_size()
        chunk_data = [data[x : x + chunk_size] for x in range(0, len(data), chunk_size)]
        for i, chunk_id in enumerate(chunk_ids):
            loc_ids = self.master.get_loc_ids(chunk_id)
            for loc_id in loc_ids:
                host, port = self.__get_host_port(loc_id)
                try:
                    con = rpyc.connect(host, port=port)
                    chunk_server = con.root.GFSChunkServer()
                    chunk_server.write_data(chunk_id, chunk_data[i])
                except EnvironmentError:
                    log.info(f"Cannot establish connection with Chunk Server at {host}:{port}")

    def create(self, file_name, data):
        if self.master.check_exists(file_name):
            raise Exception(f"Write Error: File {file_name} already exists")
        num_chunks = self.__num_of_chunks(len(data))
        chunk_ids = self.master.alloc(file_name, num_chunks)
        self.__write_chunks(chunk_ids, data)

    def append(self, file_name, data):
        if not self.master.check_exists(file_name):
            print("(404) File not found")
            raise Exception(f"Append Error: File {file_name} does not exist")
        num_append_chunks = self.__num_of_chunks(len(data))
        append_chunk_ids = self.master.alloc_append(file_name, num_append_chunks)
        self.__write_chunks(append_chunk_ids, data)

    def read(self, file_name):
        if not self.master.check_exists(file_name):
            print("(404) File not found")
            raise Exception(f"Read Error: File {file_name} does not exist")

        chunks = []
        chunk_ids = self.master.get_chunk_ids(file_name)
        for chunk_id in chunk_ids:
            loc_ids = self.master.get_loc_ids(chunk_id)
            for loc_id in loc_ids:
                host, port = self.__get_host_port(loc_id)
                try:
                    con = rpyc.connect(host, port=port)
                    chunk_server = con.root.GFSChunkServer()
                    chunk = chunk_server.get_data(chunk_id)
                    chunks.append(chunk)
                    break
                except EnvironmentError:
                    log.info(f"Cannot establish connection with Chunk Server at {host}:{port}")

        data = functools.reduce(lambda a, b: a + b, chunks)  # reassembling in order
        print(data)

    def delete(self, file_name):
        chunk_ids = self.master.get_chunk_ids(file_name)
        for chunk_id in chunk_ids:
            loc_ids = self.master.get_loc_ids(chunk_id)
            for loc_id in loc_ids:
                host, port = self.__get_host_port(loc_id)
                try:
                    con = rpyc.connect(host, port=port)
                    chunk_server = con.root.GFSChunkServer()
                    chunk_server.delete_data(chunk_id)
                except EnvironmentError:
                    log.info(f"Cannot establish connection with Chunk Server at {host}:{port}")
            self.master.delete_chunk(chunk_id)
        self.master.delete_file(file_name)

    def list(self):
        files = self.master.list_files()
        print("-------------- Files in the GFS --------------")
        for file in files:
            print(file)

def help_on_usage():
    print("-------------- Help on Usage --------------")
    print("-> To create or overwrite: client.py create filename data")
    print("-> To read: client.py read filename")
    print("-> To append: client.py append filename data")
    print("-> To delete: client.py delete filename")
    print("-> To list: client.py list")

def run(args):
    try:
        con = rpyc.connect("localhost", port=4531)
        client = GFSClient(con.root.GFSMaster())
    except EnvironmentError:
        print("Cannot establish connection with GFSMaster")
        print("Connection Error: Please start master.py and try again")
        sys.exit(1)

    if len(args) == 0:
        help_on_usage()
        return
    if args[0] == "create":
        client.create(args[1], args[2])
    elif args[0] == "read":
        client.read(args[1])
    elif args[0] == "append":
        client.append(args[1], args[2])
    elif args[0] == "delete":
        client.delete(args[1])
    elif args[0] == "list":
        client.list()
    else:
        print("Incorrect Command")
        help_on_usage()

if __name__ == "__main__":
    run(sys.argv[1:])
