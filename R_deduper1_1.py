import os
import sys
from math import log
import pickle
import gzip
from concurrent.futures import ProcessPoolExecutor, as_completed
from progressbar import ProgressBar
from collections import Counter
import shutil
import ctypes
import ctypes.wintypes
import time
import matplotlib.pyplot as plt

global block_size, prefix_len, wait_time
# Block size of bytes to dedupe.
block_size = 100
# Prefix length for custom writer. 3 Should work for smaller (20MB) files and will work if bigger files are actually working.
prefix_len = 3
# Time of reading/deduping file before giving up if no deduping has happened after wait_time
wait_time = 7


class LoadedFile:
    def __init__(self, file_name):
        self.data = open(file_name, 'rb').read()
        self.pos = 0

    def read(self, num=None):
        if not num:
            return self.data[self.pos: -1]
        self.pos += num
        if self.pos > len(self.data):
            return self.data[self.pos-num: -1]
        return self.data[self.pos-num: self.pos]


def print_bytes(nbytes: int, roundto: int = 4):
    '''
    Prints number of bytes in more readable format.

    Parameters
    ----------
    nbytes : int
        Number of bytes.
    roundto : int, optional
        What to round the final numbers to. The default is 4.

    Returns
    -------
    str
        The representation of bytes in new format.

    '''
    num = 0
    prefixes = ('', 'K', 'M', 'G', 'T', 'P')
    while abs(nbytes / 1024) > 1:
        nbytes /= 1024
        num += 1
    return f'{round(nbytes, roundto)} {prefixes[num]}B'


def get_files(folder=None):
    '''
    Retreives the files in directory and subdirectory of folder.

    Parameters
    ----------
    folder : str, optional
        The directory to get the files from. The default is None.

    Returns
    -------
    list
        list of files from directory.

    '''
    folder = folder or os.getcwd()
    return [os.path.join(path, file) for path, _, files in os.walk(folder) for file in files]


def get_file_sizes(files):
    '''
    Returns the filesize of input files.

    Parameters
    ----------
    files : Sequence[str] or str
        Files to get size of.

    Returns
    -------
    int
        sum of the file sizes in bytes.

    '''
    if isinstance(files, str):
        files = (files,)
    return sum(os.path.getsize(file) for file in files)


def writes(file, d, block_size):
    '''
    Dedupes "file" according to dictionary "d".

    Parameters
    ----------
    file : str
        File to dedupe.
    d : dict
        Dictionary where the keys are the bytes to dedupe and the value is the key for which retreival will happen.

    Returns
    -------
    None

    '''
    keys = d.keys()
    nfile = file + '.deduped'  # New file name
    # f = LoadedFile(file)
    # with open(nfile, 'wb') as w:
    with open(file, 'rb') as f, open(nfile, 'wb') as w:
        data = f.read(block_size)
        t = time.perf_counter()  # Time to ensure it doesn't take forever for nothing.
        while 1:
            if data[-block_size:] in keys:  # If most recent data == a key
                key = d[data[-block_size:]]
                data = data[:-block_size]
                prefix = len(data).to_bytes(prefix_len, 'big')
                w.write(prefix + data)
                prefix = len(key).to_bytes(prefix_len, 'big')
                w.write(prefix + key)
                data = f.read(block_size)
                t = time.perf_counter()  # Starting over time to show that it is being productive.
            else:
                reads = f.read(1)
                if not reads:  # If there is no more file
                    if len(data):
                        prefix = len(data).to_bytes(prefix_len, 'big')
                        w.write(prefix + data)
                    break
                else:
                    if time.perf_counter() - t > wait_time:  # Checks to see if its been a while since productive
                        reads = reads + f.read()
                    data += reads
                    if log(len(data), 256) > prefix_len:
                        return nfile, Exception(f'Prefix length is not large enough for the file {file}')

    # Copy file metadata to new file to ensure dates stay the same.
    creation_date = os.path.getctime(file)
    # The numbers are the way they are because thats how they are
    timestamp = int((creation_date * 10000000) + 116444736000000000)
    ctime = ctypes.wintypes.FILETIME(timestamp & 0xFFFFFFFF, timestamp >> 32)
    handle = ctypes.windll.kernel32.CreateFileW(nfile, 256, 0, None, 3, 128, None)
    ctypes.windll.kernel32.SetFileTime(handle, ctypes.byref(ctime), None, None)
    ctypes.windll.kernel32.CloseHandle(handle)
    shutil.copystat(file, nfile)
    os.remove(file)


def reads(file, block_size):
    '''
    Reads "file" in blocks of size "block_size" to determine number of unique sequential bytes.

    Parameters
    ----------
    file : str
        File to read.
    block_size : int
        Block size to read bytes.

    Returns
    -------
    measured : collections.Counter
        Basically a dictionary of unique sequential bytes and with quantity.

    '''
    # measured = Counter()  # Initialize counter
    # f = LoadedFile(file)
    f = open(file, 'rb').read()
    return Counter((f[i:i+block_size] for i in range(0, len(f), block_size)))
    # with open(file, 'rb') as f:
    # while True:
    #     data = f.read(block_size)
    #     if not data:  # check to see if there is no more data
    #         break
    #     measured.update((data,))
    # return measured


def save_metadata(d):
    '''
    Saves dictionary and prefix length.

    Parameters
    ----------
    d : dict
        Dictionary where the keys are the bytes to dedupe and the value is the key for which retreival will happen.

    Returns
    -------
    None.

    '''
    # Writes the prefix len in front of gzipped pickled dictionary. Could be optimized with my custom pickler.
    with open('DeTable.pickle', 'wb') as f:
        f.write(prefix_len.to_bytes(1, 'big')+gzip.compress(pickle.dumps(d)))


def read_for_size(files, block_size):
    '''
    Estimates savings of deduping with current settings. Returns deduping canidates.

    Parameters
    ----------
    files : Sequence[str]
        Files to read and try.

    Returns
    -------
    int
        Estimation of bytes saved if deduped.
    nums : dict
        dictionary of byte sequences and number of appearances in files > 1.

    '''
    measured = Counter()
    with ProcessPoolExecutor() as ex:
        # Discovered threads don't work but didn't change variable names, get over it.
        threads = [ex.submit(reads, file, block_size) for file in files]
        bar = ProgressBar(len(threads), lr=.001)
        for thread in as_completed(threads):
            measured.update(thread.result())  # Updates the Counter with Counter from process
            bar.update()

    # dictionary of all items that appear more than once.
    nums = {k: v for k, v in measured.items() if v > 1}
    return sum(nums.values())*(block_size-2*prefix_len-1) - (block_size+prefix_len+1)*len(nums), nums


def dedupe(folder=None, num_testing=None):
    '''
    Dedupes "folder" with given settings.

    Parameters
    ----------
    folder : str, optional
        Folder to dedupe. Defaults to current directory.
    num_testing : int, optional (not yet implimented)
        Number of tests to run to determine optimal settings. The default is 3.

    Returns
    -------
    None.

    '''
    folder = folder or os.getcwd()
    num_testing = num_testing or 69
    files = get_files()
    prev_size = get_file_sizes(files)
    global block_size
    files = [file for file in files if not file.endswith(
        ('.py', '.deduped', 'DeTable.pickle'))]  # Ensuring files aren't going to be anything that could break the program.
    # Could use os.path.exists, but didn't feel like trying. Would only make program 10^-6 secs faster anyway.
    if 'DeTable.pickle' in os.listdir(folder):
        print('Reading from previous compression...')
        with open('DeTable.pickle', 'rb') as f:
            global prefix_len
            prefix_len = int.from_bytes(f.read(1), 'big')
            d = pickle.loads(gzip.decompress(f.read()))
        new = False  # Shows that that metadata doesn't need to be written again.
    else:
        # Uses pattern search / directional search
        # a_(n+1) = max(a_n ± search) if max > a_n else search /= 2
        # Generally very effective
        saved = {}
        search = 64
        saved[block_size], _ = read_for_size(files, block_size)
        cur = (block_size, saved[block_size])
        for _ in range(num_testing):
            if block_size-search not in saved:
                saved[block_size-search], _ = read_for_size(files, block_size-search)
            if block_size+search not in saved:
                saved[block_size+search], _ = read_for_size(files, block_size+search)
            lower = (block_size-search, saved[block_size-search])
            upper = (block_size+search, saved[block_size+search])
            if (n := sorted((lower, cur, upper), key=lambda x: x[1])[-1]) != cur:
                block_size = n[0]
                cur = n
                print(f'New block_size: {block_size}')
            elif search <= 1:
                break
            else:
                search = search // 2
            plt.cla()
            plt.scatter(*zip(*tuple(saved.items())))
            plt.xlabel('Block Size')
            plt.ylabel('Estimated Bytes Saved')
            plt.pause(.0001)
        if block_size in saved:
            block_size = [k for k, _ in sorted(saved.items(), key=lambda x: x[1])][-1]
        est_saved, nums = read_for_size(files, block_size)
        print(f'\nEstimated Savings: {print_bytes(est_saved)}')
        keys = nums.keys()
        d = {k: v.to_bytes(int(log(v+1, 256))+1, 'big') for v, k in enumerate(keys)}
        new = True
    print('Writing...')
    with ProcessPoolExecutor() as ex:
        if new:
            ex.submit(save_metadata, d)
        threads = [ex.submit(writes, file, d, block_size) for file in files]
        # Learning rate should be low because of the crazy variation in completion times.
        bar = ProgressBar(len(threads), lr=.0001)
        bar.show()  # Just to show user that it is actually working.
        for future in as_completed(threads):
            bar.update()
            # Check to see if error was thrown.
            res = future.result()
            if res:
                os.remove(res[0])
                print('\n', res[1], end='')
    # For debugging.
    # for file in files:
    #     writes(file, d, block_size)
    files = get_files()
    new_size = get_file_sizes(files)
    print(f'\nMemory Saved: {print_bytes(prev_size - new_size)}')
    input(f"Compression Ratio: %{round(new_size/prev_size*100, 4)}")


if __name__ == '__main__':
    if len(sys.argv) - 1:  # if len(sys.argv) > 1
        folder = sys.argv[1]
        num_testing = int(sys.argv[2])
        dedupe(folder, num_testing)
    else:
        dedupe()
