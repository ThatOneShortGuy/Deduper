import os
import pickle
import gzip
from concurrent.futures import ProcessPoolExecutor, as_completed
from progressbar import ProgressBar
import shutil
import ctypes
import ctypes.wintypes


def writer(file: str, table: dict, prefix_len: int) -> None:
    '''
    Undedupes "file" with dict "table" using prefix length "prefix_len"

    Parameters
    ----------
    file : str
        File name.
    table : dict
        dictionary with keys as keys and values as sequences of bytes.
    prefix_len : int
        Prefix length.

    Returns
    -------
    None.

    '''
    nfile = file[:-8] # New file name will be the original file name
    with open(file, 'rb') as f, open(nfile, 'wb') as w:
        nbytes = f.read(prefix_len)
        while True:
            read_length = int.from_bytes(nbytes, 'big')
            w.write(f.read(read_length))
            read_length = int.from_bytes(f.read(prefix_len), 'big')
            nbytes = f.read(read_length)
            if not nbytes:
                break
            w.write(table[nbytes])
            nbytes = f.read(prefix_len)
            if not nbytes:
                break
    # Copies metadata to allow for correct original file.
    creation_date = os.path.getctime(file)
    timestamp = int((creation_date * 10000000) + 116444736000000000) # Again, numbers are numbers
    ctime = ctypes.wintypes.FILETIME(timestamp & 0xFFFFFFFF, timestamp >> 32)
    handle = ctypes.windll.kernel32.CreateFileW(nfile, 256, 0, None, 3, 128, None)
    ctypes.windll.kernel32.SetFileTime(handle, ctypes.byref(ctime), None, None)
    ctypes.windll.kernel32.CloseHandle(handle)
    shutil.copystat(file, nfile)
    os.remove(file)


def undupe(folder: str=None) -> None:
    '''
    Undedupes folder

    Parameters
    ----------
    folder : str, optional
        Folder to undedupe. The default is None.

    Returns
    -------
    None.

    '''
    folder = folder or os.getcwd()
    files = [os.path.join(path, file) for path, _, files in os.walk(folder)
             for file in files if file.endswith('.deduped')]
    prev_size = sum(os.path.getsize(f) for f in files)

    with open('DeTable.pickle', 'rb') as f:
        prefix_len = int.from_bytes(f.read(1), 'big')
        table = pickle.loads(gzip.decompress(f.read()))
    table = {v: k for k, v in table.items()} # Invert table so keys are values and values are keys.

    with ProcessPoolExecutor() as ex:
        threads = [ex.submit(writer, file, table, prefix_len) for file in files]
        bar = ProgressBar(len(threads), lr=.01)
        for thread in as_completed(threads):
            bar.update()
    os.remove('DeTable.pickle')
    # For debugging
    # for file in files:
    #     writer(file, table, prefix_len)
    files = [os.path.join(path, file) for path, _, files in os.walk(folder)
             for file in files if file.endswith('.deduped')]
    print(sum(os.path.getsize(f) for f in files)/prev_size) #Usually never seen, but is there.


if __name__ == '__main__':
    undupe()
