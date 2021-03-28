# MD5 vs BLAKE2B

Quick test to measure time performance for the 2 algos used for content checksum in Python.

blake2b seems to be 15% faster than md5.
blake2b with the memory optimization strategy seems to be 25% faster than md5 w/out mem optimization.
Reading different chunks of the same file at the same time in diff threads is not
efficient: there is nothing faster than a sequential read (in the same thread).