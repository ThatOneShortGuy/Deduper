import os
import sys
import pickle
import gzip
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
    nfile = file[:-8]  # New file name will be the original file name
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
    timestamp = int((creation_date * 10000000) + 116444736000000000)  # Again, numbers are numbers
    ctime = ctypes.wintypes.FILETIME(timestamp & 0xFFFFFFFF, timestamp >> 32)
    handle = ctypes.windll.kernel32.CreateFileW(nfile, 256, 0, None, 3, 128, None)
    ctypes.windll.kernel32.SetFileTime(handle, ctypes.byref(ctime), None, None)
    ctypes.windll.kernel32.CloseHandle(handle)
    shutil.copystat(file, nfile)
    os.remove(file)


def loader(folder: str) -> tuple(dict, int):
    '''
    Loads in metadata by going back folders until metadata is found.

    Parameters
    ----------
    folder : str
        Starting folder to check for metadata.

    Returns
    -------
    dict, int
        Tuple of (Dictionary of keys and their byte sequences, Prefix length).

    '''
    global block_size, fill_size, table, keys
    while 'DeTable.pickle' not in os.listdir(folder):  # Could be replaced with os.path.exists.
        folder = os.path.split(folder)[0]
    os.chdir(folder)
    with open('DeTable.pickle', 'rb') as f:
        prefix_len = int.from_bytes(f.read(1), 'big')
        table = pickle.loads(gzip.decompress(f.read()))
    table = {v: k for k, v in table.items()}
    return table, prefix_len


def undeduper(file: str, folder: str=None) -> None:
    '''
    Undedupes single file without removing deduping metadata.

    Parameters
    ----------
    file : str
        File to undedupe.
    folder : str, optional
        Starting folder to check for undeduping metadata. Defaults to directory of "file".

    Returns
    -------
    None.

    '''
    folder = folder or os.path.split(file)[0]
    table, prefix_len = loader(folder)
    writer(file, table, prefix_len)


if __name__ == '__main__':
    file = sys.argv[1]
    if len(sys.argv) > 2:
        folder = sys.argv[2]
    else:
        folder = None
    undeduper(file, folder)
