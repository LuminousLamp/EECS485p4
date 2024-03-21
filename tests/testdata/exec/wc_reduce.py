#!/usr/bin/env python3
"""
Template reducer.

https://github.com/eecs485staff/madoop/blob/main/README_Hadoop_Streaming.md
# """
# import sys
# import itertools
# import collections


# def reduce_one_group(key, group):
#     """Reduce one group."""


# def keyfunc(line):
#     """Return the key from a TAB-delimited key-value pair."""
#     return line.partition("\t")[0]


# def main():
#     """Divide sorted lines into groups that share a key."""
#     # for key, group in itertools.groupby(sys.stdin, keyfunc):
#     #     print(key, group)
#     #     reduce_one_group(key, group)
#     word_count = collections.defaultdict(int)
#     for line in sys.stdin:
#         word, count = line.split()
#         word_count[word] += int(count)
#     for word, count in word_count.items():
#         print(word, count, sep="\t")


# if __name__ == "__main__":
#     main()


import sys
import itertools


def main():
    """Divide sorted lines into groups that share a key."""
    for key, group in itertools.groupby(sys.stdin, keyfunc):
        reduce_one_group(key, group)


def keyfunc(line):
    """Return the key from a TAB-delimited key-value pair."""
    return line.partition("\t")[0]


def reduce_one_group(key, group):
    """Reduce one group."""
    word_count = 0
    for line in group:
        count = line.partition("\t")[2]
        word_count += int(count)
    print(f"{key} {word_count}")


if __name__ == "__main__":
    main()