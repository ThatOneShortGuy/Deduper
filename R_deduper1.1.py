import os
from math import log
import pickle
import gzip
from concurrent.futures import ProcessPoolExecutor, as_completed
from progressbar import ProgressBar
from collections import Counter
import shutil
import ctypes
import ctypes.wintypes
import matplotlib.pyplot as plt
import time

global block_size, prefix_len, wait_time
block_size = 128
prefix_len = 3
wait_time = 7

def print_bytes(nbytes, roundto=4):
    num=0
    prefixes=('','K','M','G','T','P')
    while abs(nbytes / 1024) > 1:
        nbytes /= 1024
        num+=1
    return f'{round(nbytes, roundto)} {prefixes[num]}B'

def get_files(folder=None):
    folder = folder or os.getcwd()
    return [os.path.join(path, file) for path, _, files in os.walk(folder) for file in files]

def get_file_sizes(files):
    if isinstance(files, str):
        files = (files,)
    return sum(os.path.getsize(file) for file in files)

def writes(file, d):
    keys = d.keys()
    nfile = file + '.deduped'
    with open(file, 'rb') as f, open(nfile, 'wb') as w:
        data = f.read(block_size)
        t = time.perf_counter()
        while 1:
            if (key:=data[-block_size:]) in keys:
                key = d[key]
                data = data[:-block_size]
                prefix = len(data).to_bytes(prefix_len, 'big')
                w.write(prefix + data)
                prefix = len(key).to_bytes(prefix_len, 'big')
                w.write(prefix + key)
                data = f.read(block_size)
                t = time.perf_counter()
            else:
                if not (reads:=f.read(1)):
                    prefix = len(data).to_bytes(prefix_len, 'big')
                    w.write(prefix + data)
                    break
                else:
                    if time.perf_counter() - t > wait_time:
                        reads = reads + f.read()
                    data += reads
                    if log(len(data), 256) > prefix_len:
                        return nfile, Exception(f'Prefix length is not large enough for the file {file}')
            
    creation_date = os.path.getctime(file)
    timestamp = int((creation_date * 10000000) + 116444736000000000)
    ctime = ctypes.wintypes.FILETIME(timestamp & 0xFFFFFFFF, timestamp >> 32)
    handle = ctypes.windll.kernel32.CreateFileW(nfile, 256, 0, None, 3, 128, None)
    ctypes.windll.kernel32.SetFileTime(handle, ctypes.byref(ctime), None, None)
    ctypes.windll.kernel32.CloseHandle(handle)
    shutil.copystat(file, nfile)
    os.remove(file)

def reads(file, block_size):
    measured = Counter()
    with open(file, 'rb') as f:
        while True:
            data = f.read(block_size)
            if not data:
                break
            measured.update((data,))
    return measured

def save_metadata(d):
    with open('DeTable.pickle', 'wb') as f:
        f.write(prefix_len.to_bytes(1, 'big')+gzip.compress(pickle.dumps(d)))

def read_for_size(files):
    measured = Counter()
    with ProcessPoolExecutor() as ex:
        threads = [ex.submit(reads, file, block_size) for file in files]
        bar = ProgressBar(len(threads), lr=.0001)
        for thread in as_completed(threads):
            measured.update(thread.result())
            bar.update()
    
    nums = tuple((k, v) for k, v in measured.items() if v > 1)
    return sum(tuple(zip(*nums))[1])*(block_size-prefix_len-1) - (block_size+prefix_len+1)*len(nums), nums

def dedupe(folder=None, num_testing=3):
    folder = folder or os.getcwd()
    files = get_files()
    prev_size = get_file_sizes(files)
    global block_size, fill_size
    files = [file for file in files if not file.endswith(('.py', '.deduped', 'DeSizes.json', 'DeTable.pickle'))]
    if 'DeTable.pickle' in os.listdir(folder):
        print('Reading from previous compression...')
        with open('DeTable.pickle', 'rb') as f:
            global prefix_len
            prefix_len = int.from_bytes(f.read(1), 'big')
            d = pickle.loads(gzip.decompress(f.read()))
        new = False
    else:
        # saved = []
        # for i in range(5):
        #     block_size = 256+256*i
        #     est_saved, nums = read_for_size(files)
        #     saved.append((block_size, est_saved))
        # plt.scatter(*tuple(zip(*saved)))
        # plt.show()
        est_saved, nums = read_for_size(files)
        print(f'\nEstimated Savings: {print_bytes(est_saved)}')
        keys = tuple(zip(*nums))[0]
        d = {k: v.to_bytes(int(log(v+1, 256))+1, 'big') for k, v in zip(keys, range(len(keys)))}
        new = True
    print('Writing')
    with ProcessPoolExecutor() as ex:
        if new:
            ex.submit(save_metadata, d)
        threads = [ex.submit(writes, file, d) for file in files]
        bar = ProgressBar(len(threads), lr=.0001)
        bar.show()
        for future in as_completed(threads):
            bar.update()
            if res:=future.result():
                os.remove(res[0])
                print('\n', res[1], end='')
    # for file in files:
    #     writes(file, d)
    files = get_files()
    new_size = get_file_sizes(files)
    print(f'\nMemory Saved: {print_bytes(prev_size - new_size)}')
    input(f"Compression Ratio: %{round(new_size/prev_size*100, 4)}")

if __name__ == '__main__':
    dedupe()
