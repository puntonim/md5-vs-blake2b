"""
Create 2 big files in macOS:
$ mkfile 1g 1Gtmp
$ mkfile 100g 100Gtmp

Create 2 big files in ubuntu:
$ fallocate -l 1G 1Gtmp
$ fallocate -l 100G 100Gtmp
"""
import hashlib
import math
import timeit
from pathlib import Path
from typing import Union
import concurrent.futures
from functools import lru_cache


#@lru_cache
def hash_clean(file_path: Union[str, Path], chunk_num_blocks: int = 128, hash_fn=hashlib.md5, actual_checksum=None) -> str:
    h = hash_fn()
    with open(file_path, "rb") as file_to_hash:
        for chunk in iter(lambda: file_to_hash.read(chunk_num_blocks * h.block_size), b""):
            h.update(chunk)
    result = h.hexdigest()
    if actual_checksum:
        assert result == actual_checksum
    return result


def hash_optimized(file_path: Union[str, Path], hash_fn=hashlib.md5, actual_checksum=None):
    # Src: https://stackoverflow.com/a/44873382
    h = hash_fn()
    b = bytearray(128*1024)
    mv = memoryview(b)
    with open(file_path, "rb", buffering=0) as f:
        for n in iter(lambda: f.readinto(mv), 0):
            h.update(mv[:n])
    result = h.hexdigest()
    if actual_checksum:
        assert result == actual_checksum
    return result


def _hash_clean_thread1(file_path: Union[str, Path], chunk_num_blocks: int = 128, hash_fn=hashlib.md5, actual_checksum=None) -> str:

    h = hash_fn()
    with open(file_path, "rb") as file_to_hash:
        i = 1
        for chunk in iter(lambda: file_to_hash.read(chunk_num_blocks * h.block_size), b""):
            if i >= (1310721/2):
                break
            h.update(chunk)
            i += 1
    result = h.hexdigest()
    print(f"steps1 {i}")
    if actual_checksum:
        assert result == actual_checksum
    return result


def _hash_clean_thread2(file_path2: Union[str, Path], chunk_num_blocks: int = 128, hash_fn=hashlib.md5, actual_checksum=None) -> str:
    h = hash_fn()
    with open(file_path2, "rb") as file_to_hash2:
        i = 1
        file_to_hash2.seek(math.floor(10737418240/2)+1000)

        while True:
            chunk = file_to_hash2.read(chunk_num_blocks * h.block_size)
            if not chunk:
                break

        # for chunk in iter(lambda: file_to_hash2.read(chunk_num_blocks * h.block_size), b""):
        #     if i >= (1310721/2):
        #         break
            h.update(chunk)
            i += 1
    result = h.hexdigest()
    print(f"steps2 {i}")
    if actual_checksum:
        assert result == actual_checksum
    return result


def threaded():
    hashed = ""
    # with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
    with concurrent.futures.ProcessPoolExecutor(max_workers=20) as executor:
        # Docs: https://docs.python.org/3.8/library/concurrent.futures.html
        # Only `num_worker_threads` are run concurrently.
        file_path_10g = Path() / "10Gtmp"
        futures = {executor.submit(_hash_clean_thread1, file_path_10g): "first"}
        futures[executor.submit(_hash_clean_thread2, file_path_10g)] = "second"

        for future in concurrent.futures.as_completed(futures):  # yield a future as soon as it completes.
            hashed += f"|{futures[future]}|" + future.result()

    print(hashed)
    print("FOO-END")
    return hashed


def main():
    print("START")
    file_path_1g = Path() / "1Gtmp"
    file_path_10g = Path() / "10Gtmp"
    file_path_100g = Path() / "100Gtmp"
    max_repetitions = 10  # Note: the time take and shown is the total for th # repetitions.

    # md5
    result = timeit.timeit(lambda: hash_clean(file_path_1g, actual_checksum="cd573cfaace07e7949bc0c46028904ff"), number=max_repetitions)
    print(f"md5_clean 1Gb, {max_repetitions}x: {result}")  # md5_clean 1Gb, 10x: 16.043754983

    result = timeit.timeit(lambda: hash_optimized(file_path_1g, actual_checksum="cd573cfaace07e7949bc0c46028904ff"), number=max_repetitions)
    print(f"md5_optimized 1Gb, {max_repetitions}x: {result}")  # md5_optimized 1Gb, 10x: 14.432847937999998

    result = timeit.timeit(lambda: hash_clean(file_path_100g, actual_checksum="09cd755eb35bc534487a5796d781a856"), number=1)
    print(f"md5_clean 100Gb, 1x: {result}")  # md5_clean 100Gb, 1x: 184.011319865

    result = timeit.timeit(lambda: hash_optimized(file_path_100g, actual_checksum="09cd755eb35bc534487a5796d781a856"), number=1)
    print(f"md5_optimized 100Gb, 1x: {result}")  # md5_optimized 100Gb, 1x: 157.78056194899997

    # blake2b
    result = timeit.timeit(lambda: hash_clean(file_path_1g, hash_fn=hashlib.blake2b, actual_checksum="9ba5dba8be8c8ab1474e7dbe5c7d2fb29c8d161beb5a5d4410b342445c60ab1dd895062c3561d3b128e96938a11a1c89a80169b3e3654dbf76b6eed50dc5e1c6"), number=max_repetitions)
    print(f"blake2b_clean 1Gb, {max_repetitions}x: {result}")  # blake2b_clean 1Gb, 5x: 6.483333770999991

    result = timeit.timeit(lambda: hash_optimized(file_path_1g, hash_fn=hashlib.blake2b, actual_checksum="9ba5dba8be8c8ab1474e7dbe5c7d2fb29c8d161beb5a5d4410b342445c60ab1dd895062c3561d3b128e96938a11a1c89a80169b3e3654dbf76b6eed50dc5e1c6"), number=max_repetitions)
    print(f"blake2b_optimized 1Gb, {max_repetitions}x: {result}")  # blake2b_optimized 1Gb, 5x: 5.930278431999966

    result = timeit.timeit(lambda: hash_clean(file_path_100g, hash_fn=hashlib.blake2b, actual_checksum="8d07b2a7497b4caa271185c9a3e2b4c47c058e5af35adf00e3925edde6a82b6b90af3fb93b876d550b2b0d553dac5789c89e146f9006de5110b48293ea782119"), number=1)
    print(f"blake2b_clean 100Gb, 1x: {result}")  # blake2b_clean 100Gb, 1x: 146.378327968

    result = timeit.timeit(lambda: hash_optimized(file_path_100g, hash_fn=hashlib.blake2b, actual_checksum="8d07b2a7497b4caa271185c9a3e2b4c47c058e5af35adf00e3925edde6a82b6b90af3fb93b876d550b2b0d553dac5789c89e146f9006de5110b48293ea782119"), number=1)
    print(f"blake2b_optimized 100Gb, 1x: {result}")  # blake2b_optimized 100Gb, 1x: 135.13135883299998)

    print("END")


if __name__ == "__main__":
    main()
