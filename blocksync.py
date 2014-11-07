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


def sync(src, dst, options):

    srcdev    = src['path']
    dsthost   = '%(user)s@%(host)s' % dst if dst['user'] else '%(host)s' % dst
    dstdev    = dst['path']
    blocksize = options.blocksize
    compress  = options.compress
    progress  = options.progress

    print "Block size is %0.1f MB" % (float(blocksize) / MIBI)

    args = dict(dst)
    args.update({'blocksize': options.blocksize})

    cmd = 'python blocksync.py -b %(blocksize)s server %(path)s' % args

    if dst['proto'] == 'ssh':
        args.update({
            'compress' : '-C' if options.compress else '',
            'user'     : '-l %s' % dst['user'] if dst['user'] else '',
        })
        cmd = 'ssh -c arcfour %(compress)s %(user)s %(host)s ' % args + cmd

    cmd = cmd.split()

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
    parser = OptionParser(usage="%prog [options] file://<source> ssh://[<user>@]<host>/<dest>")
    parser.add_option("-b", "--blocksize", dest="blocksize", action="store", type="int", help="block size (bytes)", default=MIBI)
    parser.add_option("-c", "--compress",  dest="compress",  action="store_true", default=False, help="use compression")
    parser.add_option("-p", "--progress",  dest="progress",  action="store_true", default=False, help="display progress")
    (options, args) = parser.parse_args()

    if args[0] == 'server':
        dstdev = args[1]
        server(dstdev, options.blocksize)
        sys.exit(0)

    uri = re.compile(r'(?P<proto>\w+)://(((?P<user>\w+)@)?(?P<host>[\w\.]+)/)?(?P<path>.+)')

    try:
        src=uri.match(args[0]).groupdict()
        dst=uri.match(args[1]).groupdict()
        if not (src['proto'] == 'file' and not src['user'] and not src['host'] and src['path']):
            raise Exception('invalid source')
        if not (dst['proto'] == 'ssh' and dst['host'] and dst['path']):
            raise Exception('invalid dest')
    except:
        parser.print_help()
        print __doc__
        sys.exit(1)

    sync(src, dst, options)
