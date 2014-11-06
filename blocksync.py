#!/usr/bin/env python
"""
Synchronise block devices over the network

Copyright 2006-2008 Justin Azoff <justin@bouncybouncy.net>
Copyright 2011 Robert Coup <robert@coup.net.nz>
License: GPL

Getting started:

* Copy blocksync.py to the home directory on the remote host
* Make sure your remote user can either sudo or is root itself.
* Make sure your local user can ssh to the remote host
* Invoke:
    sudo python blocksync.py /dev/source user@remotehost /dev/dest
"""

import re
import sys
from hashlib import sha1
import subprocess
import time

SAME = "same\n"
DIFF = "diff\n"
MIBI = 1024*1024


def do_open(f, mode):
    f = open(f, mode)
    f.seek(0, 2)
    size = f.tell()
    f.seek(0)
    return f, size


def getblocks(f, blocksize):
    while 1:
        block = f.read(blocksize)
        if not block:
            break
        yield block


def server(dev, blocksize):
    f, size = do_open(dev, 'r+b')

    for block in getblocks(f, blocksize):
        print sha1(block).hexdigest()
        sys.stdout.flush()
        res = sys.stdin.readline()
        if res != SAME:
            newblock = sys.stdin.read(blocksize)
            f.seek(-len(block), 1)
            f.write(newblock)


def sync(srcdev, dsthost, dstdev, blocksize, compress, progress):

    print "Block size is %0.1f MB" % (float(blocksize) / MIBI)
    if compress:
        cmd = ['ssh', '-C', '-c', 'arcfour', dsthost, 'python', 'blocksync.py', 'server', dstdev, '-b', str(blocksize)]
    else:
        cmd = ['ssh',       '-c', 'arcfour', dsthost, 'python', 'blocksync.py', 'server', dstdev, '-b', str(blocksize)]
    print "Running: %s" % " ".join(cmd)

    p = subprocess.Popen(cmd, bufsize=0, stdin=subprocess.PIPE, stdout=subprocess.PIPE, close_fds=True)
    p_in, p_out = p.stdin, p.stdout

    try:
        f, size = do_open(srcdev, 'rb')
    except Exception, e:
        print "Error accessing source device! %s" % e
        sys.exit(1)

    same_blocks = diff_blocks = 0

    print "Starting sync..."
    t0 = time.time()
    t_last = t0
    size_blocks = size / blocksize
    for i, l_block in enumerate(getblocks(f, blocksize)):
        l_sum = sha1(l_block).hexdigest()
        r_sum = p_out.readline().strip()

        if l_sum == r_sum:
            p_in.write(SAME)
            p_in.flush()
            same_blocks += 1
        else:
            p_in.write(DIFF)
            p_in.flush()
            p_in.write(l_block)
            p_in.flush()
            diff_blocks += 1

        if progress:
            t1 = time.time()
            if t1 - t_last > 1 or (same_blocks + diff_blocks) >= size_blocks:
                rate = (i + 1.0) * blocksize / (MIBI * (t1 - t0))
                print "\rsame: %d, diff: %d, %d/%d, %5.1f MB/s" % (same_blocks, diff_blocks, same_blocks + diff_blocks, size_blocks, rate),
                t_last = t1

    print "\n\nCompleted in %d seconds" % (time.time() - t0)

    return same_blocks, diff_blocks

if __name__ == "__main__":
    from optparse import OptionParser
    parser = OptionParser(usage="%prog [options] file:///path/to/source user@remotehost file:///path/to/dest")
    parser.add_option("-b", "--blocksize", dest="blocksize", action="store", type="int", help="block size (bytes)", default=MIBI)
    parser.add_option("-c", "--compress",  dest="compress",  action="store_true", default=False, help="use compression")
    parser.add_option("-p", "--progress",  dest="progress",  action="store_true", default=False, help="display progress")
    (options, args) = parser.parse_args()

    if len(args) < 3:
        parser.print_help()
        print __doc__
        sys.exit(1)

    if args[0] == 'server':
        dstdev = args[1]
        server(dstdev, options.blocksize)
    else:
        uri = re.compile(r'file://(?P<path>.+)')
        try:
            srcdev = uri.match(args[0]).groupdict()['path']
            dsthost = args[1]
            dstdev = uri.match(args[2]).groupdict()['path']
        except:
            parser.print_help()
            sys.exit(1)
        sync(srcdev, dsthost, dstdev, options.blocksize, options.compress, options.progress)
