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

SAME = "same"
DIFF = "diff"
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
    sys.stdout.write('%d\r\n' % size)
    sys.stdout.flush()

    for block in getblocks(f, blocksize):
        sum = sha1(block).hexdigest()
        sys.stdout.write(sum+'\r\n')
        sys.stdout.flush()
        res = sys.stdin.readline().strip()
        if res != SAME:
            newblock = sys.stdin.read(blocksize)
            f.seek(-len(block), 1)
            f.write(newblock)


def client(dev, blocksize):
    f, size = do_open(dev, 'rb')
    sys.stdout.write('%d\r\n' % size)
    sys.stdout.flush()

    for block in getblocks(f, blocksize):
        sum = sha1(block).hexdigest()
        sys.stdout.write(sum+'\r\n')
        sys.stdout.flush()
        res = sys.stdin.readline().strip()
        if res != SAME:
            sys.stdout.write(block)
            sys.stdout.flush()


def sync(src, dst, options):

    blocksize = options.blocksize
    compress  = options.compress
    progress  = options.progress

    print "Block size is %0.1f MB" % (float(blocksize) / MIBI)

    args = dict(dst)
    args.update({'blocksize': blocksize})

    cmd = 'python blocksync.py -b %(blocksize)s server %(path)s' % args

    if dst['proto'] == 'ssh':
        args.update({
            'compress' : '-C' if compress else '',
            'user'     : '-l %s' % dst['user'] if dst['user'] else '',
        })
        cmd = 'ssh -c arcfour %(compress)s %(user)s %(host)s ' % args + cmd

    cmd = cmd.split()

    print "Running: %s" % " ".join(cmd)

    p = subprocess.Popen(cmd, bufsize=0, stdin=subprocess.PIPE, stdout=subprocess.PIPE, close_fds=True)
    p_in, p_out = p.stdin, p.stdout

    p_size = p_out.readline().strip()
    p_size = int(p_size)

    args = dict(src)
    args.update({'blocksize': blocksize})

    cmd = 'python blocksync.py -b %(blocksize)s client %(path)s' % args

    if src['proto'] == 'ssh':
        args.update({
            'compress' : '-C' if compress else '',
            'user'     : '-l %s' % src['user'] if src['user'] else '',
        })
        cmd = 'ssh -c arcfour %(compress)s %(user)s %(host)s ' % args + cmd

    cmd = cmd.split()

    # print "Running: %s" % " ".join(cmd)

    # c = subprocess.Popen(cmd, bufsize=0, stdin=subprocess.PIPE, stdout=subprocess.PIPE, close_fds=True)
    # c_in, c_out = c.stdin, c.stdout

    # c_size = c_out.readline().strip()
    # c_size = int(c_size)

    try:
        f, size = do_open(src['path'], 'rb')
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
            p_in.write(SAME+'\r\n')
            p_in.flush()
            same_blocks += 1
        else:
            p_in.write(DIFF+'\r\n')
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
    parser = OptionParser(usage="%prog [options] file://<source> {file|ssh}://[[<user>@]<host>/]<dest>")
    parser.add_option("-b", "--blocksize", dest="blocksize", action="store", type="int", help="block size (bytes)", default=MIBI)
    parser.add_option("-c", "--compress",  dest="compress",  action="store_true", default=False, help="use compression")
    parser.add_option("-p", "--progress",  dest="progress",  action="store_true", default=False, help="display progress")
    (options, args) = parser.parse_args()

    if args[0] == 'client':
        srcdev = args[1]
        client(srcdev, options.blocksize)
        sys.exit(0)

    if args[0] == 'server':
        dstdev = args[1]
        server(dstdev, options.blocksize)
        sys.exit(0)

    umap = {'proto': None, 'user': None, 'host': None, 'path': None}
    uris = [ re.compile(r'(?P<proto>file)://(?P<path>.+)'),
             re.compile(r'(?P<proto>ssh)://((?P<user>\w+)@)?(?P<host>[\w\.]+)/(?P<path>.+)') ]

    try:
        src = dict(umap)
        for uri in uris:
            if uri.match(args[0]): break
        src.update(uri.match(args[0]).groupdict())
        dst = dict(umap)
        for uri in uris:
            if uri.match(args[1]): break
        dst.update(uri.match(args[1]).groupdict())
        # syntax check
        for arg in [src, dst]:
            if ( not arg['path'] ) or \
               ( not (arg['proto'] == 'file' or arg['proto'] == 'ssh')   ) or \
               ( arg['proto'] == 'file' and (arg['user'] or arg['host']) ) or \
               ( arg['proto'] == 'ssh'  and             not arg['host']  ):
                raise Exception('invalid uri')
        # check if supported by program
        if src['proto'] == 'ssh':
            raise Exception('unsupported source')
    except:
        parser.print_help()
        print __doc__
        sys.exit(1)

    sync(src, dst, options)
