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
            raise Exception(f"Append Error: File {file_name} does not exist")
        num_append_chunks = self.__num_of_chunks(len(data))
        append_chunk_ids = self.master.alloc_append(file_name, num_append_chunks)
        self.__write_chunks(append_chunk_ids, data)

    def read(self, file_name):
        if not self.master.check_exists(file_name):
            raise Exception(f"Read Error: File {file_name} does not exist")

        chunks = []
        chunk_ids = self.master.get_chunk_ids(file_name)
        for chunk_id in chunk_ids:
            found_chunk = False
            loc_ids = self.master.get_loc_ids(chunk_id)
            for loc_id in loc_ids:
                host, port = self.__get_host_port(loc_id)
                try:
                    con = rpyc.connect(host, port=port)
                    chunk_server = con.root.GFSChunkServer()
                    chunk = chunk_server.get_data(chunk_id)
                    if chunk:
                        chunks.append(chunk)
                        found_chunk = True
                        break
                except EnvironmentError:
                    log.info(f"Cannot establish connection with Chunk Server at {host}:{port}")
            if not found_chunk:
                log.error(f"Chunk {chunk_id} could not be found on any chunk server")

        data = functools.reduce(lambda a, b: a + b, chunks) if chunks else ""
        return data

    def delete(self, file_name):
        chunk_ids = self.master.get_chunk_ids(file_name)
        for chunk_id in chunk_ids:
            loc_ids = self.master.get_loc_ids(chunk_id)
            if loc_ids:
                loc_id = loc_ids[0]  # Only delete from the first chunk server
                host, port = self.__get_host_port(loc_id)
                try:
                    con = rpyc.connect(host, port=port)
                    chunk_server = con.root.GFSChunkServer()
                    chunk_server.delete_data(chunk_id)
                    log.info(f"Chunk {chunk_id} deleted from server {host}:{port}")
                except EnvironmentError:
                    log.info(f"Cannot establish connection with Chunk Server at {host}:{port}")

    def list_files(self):
        return self.master.list_files()
