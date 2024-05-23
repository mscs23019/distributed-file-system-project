import uuid
import signal
import random
import pickle
import sys
import os
import rpyc
from rpyc.utils.server import ThreadedServer
import config

def int_handler(signal, frame):
    pickle.dump(
        (
            GFSMasterService.exposed_GFSMaster.file_table,
            GFSMasterService.exposed_GFSMaster.handle_table,
        ),
        open("gfs.img", "wb"),
    )
    sys.exit(0)

def load_backup():
    if os.path.isfile("gfs.img"):
        (
            GFSMasterService.exposed_GFSMaster.file_table,
            GFSMasterService.exposed_GFSMaster.handle_table,
        ) = pickle.load(open("gfs.img", "rb"))

class GFSMasterService(rpyc.Service):
    class exposed_GFSMaster:
        replication_factor = config.REPLICATION_FACTOR
        chunk_size = config.CHUNK_SIZE
        file_table = {}
        handle_table = {}
        chunk_servers = config.CHUNK_SERVERS

        def exposed_list_files(self):
            return list(self.file_table.keys())

        def exposed_get_chunk_size(self):
            return self.__class__.chunk_size

        def exposed_check_exists(self, file_name):
            return file_name in self.__class__.file_table

        def exposed_get_chunk_ids(self, file_name):
            return self.__class__.file_table[file_name]

        def exposed_get_loc_ids(self, chunk_id):
            return self.__class__.handle_table[chunk_id]

        def exposed_get_chunk_servers(self):
            return self.__class__.chunk_servers

        def exposed_delete_chunk(self, chunk_id):
            del self.__class__.handle_table[chunk_id]

        def exposed_delete_file(self, file_name):
            del self.__class__.file_table[file_name]

        def exposed_alloc(self, file_name, num_chunks):
            chunk_ids = self.alloc_chunks(num_chunks)
            self.__class__.file_table[file_name] = chunk_ids
            self.print_tables()
            return chunk_ids

        def exposed_alloc_append(self, file_name, num_append_chunks):
            append_chunk_ids = self.alloc_chunks(num_append_chunks)
            self.__class__.file_table[file_name].extend(append_chunk_ids)
            return append_chunk_ids

        def alloc_chunks(self, num_chunks):
            chunk_ids = []
            for _ in range(num_chunks):
                chunk_id = str(uuid.uuid4())
                loc_ids = random.sample(
                    list(self.__class__.chunk_servers.keys()),
                    self.__class__.replication_factor,
                )
                self.__class__.handle_table[chunk_id] = loc_ids
                chunk_ids.append(chunk_id)
            self.print_tables()
            return chunk_ids

        def print_tables(self):
            print("\n-------------- File Table --------------")
            for file, chunks in self.__class__.file_table.items():
                print(f"{file}: {chunks}")
            print("\n-------------- Handle Table --------------")
            for chunk, locs in self.__class__.handle_table.items():
                print(f"{chunk}: {locs}")

if __name__ == "__main__":
    load_backup()
    signal.signal(signal.SIGINT, int_handler)
    print("GFSMaster is Running!")
    t = ThreadedServer(GFSMasterService, port=config.MASTER_PORT)
    t.start()
