import hashlib
import os
import time
from pathlib import Path
from collections import defaultdict

from itertools import groupby
from operator import itemgetter
from multiprocessing.pool import Pool


class FileDeduplicator:

    def __init__(self, *paths):
        self.paths = paths

    @staticmethod
    def calculate_checksum(file_name):
        h = hashlib.sha256()
        b = bytearray(128 * 1024)
        mv = memoryview(b)
        n = file_name
        with open(n, 'rb', buffering=0) as f:
            while n := f.readinto(mv):
                h.update(mv[:n])
        return file_name, h.hexdigest()

    @staticmethod
    def print_time(start, end):
        hours, rem = divmod(end - start, 3600)
        minutes, seconds = divmod(rem, 60)
        return "{:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds)

    @staticmethod
    def pretty_print_memory(b):
        b = float(b)
        kb = float(1024)
        mb = float(kb ** 2)
        gb = float(kb ** 3)
        tb = float(kb ** 4)

        if b < kb:
            return '{0} {1}'.format(b, 'Bytes' if 0 == b > 1 else 'Byte')
        elif kb <= b < mb:
            return '{0:.2f} KB'.format(b / kb)
        elif mb <= b < gb:
            return '{0:.2f} MB'.format(b / mb)
        elif gb <= b < tb:
            return '{0:.2f} GB'.format(b / gb)
        elif tb <= b:
            return '{0:.2f} TB'.format(b / tb)

    def calculate_duplicates(self, remove_files=False, go_parallel=False):

        global_start_time = file_calculation_start_time = time.time()
        search_space = []
        print('calculating files to deduplicate ...', flush=True)

        for p in self.paths:
            path = Path(p).rglob('*')
            for f in path:
                if os.access(f, os.R_OK) and f.is_file() and os.path.getsize(f) > 0:
                    search_space.append(f)

        print(f'\tfinished in {FileDeduplicator.print_time(file_calculation_start_time, time.time())}!', flush=True)
        print("calculating file size index ...", flush=True)

        file_size_index_start_time = time.time()
        lookup = defaultdict(list)
        i = 0

        for artifact in search_space:
            with open(artifact) as f:
                info = artifact.stat()
                print('\r', str(i) + '/' + str(len(search_space)) + " " + str(f.name), end='', flush=True)
                lookup[info.st_size].append(artifact)
                i += 1

        print(
            f'\n\tfinished calculating file size index for {i} files in {FileDeduplicator.print_time(file_size_index_start_time, time.time())}!',
            flush=True)

        dedup_start_time = time.time()
        duplicates = []
        lookup = {key: value for (key, value) in lookup.items() if len(value) > 1}

        for key, values in lookup.items():
            if go_parallel:
                with Pool(4) as pool:
                    deep_candidates = pool.map(FileDeduplicator.calculate_checksum, lookup[key])
            else:
                deep_candidates = list(map(lambda x: FileDeduplicator.calculate_checksum(x), lookup[key]))

            deep_candidates.sort(key=itemgetter(1))
            groups = groupby(deep_candidates, itemgetter(1))
            for group_key, data in groups:
                data = list(data)
                if len(data) > 1:
                    candidates = list(map(lambda x: x[0], data))
                    print(f'found dup with {candidates[0]} and {candidates[1:]}', flush=True)
                    duplicates.extend(candidates[1:])

        savings = sum([duplicate.stat().st_size for duplicate in duplicates])
        print(f'finished performing deduplication in {FileDeduplicator.print_time(dedup_start_time, time.time())}!', flush=True)
        print(f'total calculation time was {FileDeduplicator.print_time(global_start_time, time.time())}', flush=True)
        print(f'{len(duplicates)} potential dupes for a potential savings of {FileDeduplicator.pretty_print_memory(savings)}!', flush=True)

        if remove_files:
            val = input("Are you sure you want to permanently delete the files shown above ? (Y/N): ")
            if val == 'Y':
                for d in duplicates:
                    print(f'removing : {d}', flush=True)
                    d.unlink()
            else:
                print("Skipping the delete!")


if __name__ == '__main__':
    d = FileDeduplicator("d:\\")
    d.calculate_duplicates(remove_files=False, go_parallel=True)
