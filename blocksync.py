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
ABORT= "abort"
EOL  = "\r\n"
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
    sys.stdout.write(('%d' % size)+EOL)
    sys.stdout.flush()

    for block in getblocks(f, blocksize):
        sum = sha1(block).hexdigest()
        sys.stdout.write(sum+EOL)
        sys.stdout.flush()
        res = sys.stdin.readline().strip()
        if res == DIFF:
            newblock = sys.stdin.read(blocksize)
            f.seek(-len(block), 1)
            f.write(newblock)
        if res == ABORT:
            sys.exit(0)


def client(dev, blocksize):
    f, size = do_open(dev, 'rb')
    sys.stdout.write(('%d' % size)+EOL)
    sys.stdout.flush()

    for block in getblocks(f, blocksize):
        sum = sha1(block).hexdigest()
        sys.stdout.write(sum+EOL)
        sys.stdout.flush()
        res = sys.stdin.readline().strip()
        if res == DIFF:
            sys.stdout.write(block)
            sys.stdout.flush()
        if res == ABORT:
            sys.exit(0)


def sync(src, dst, options):

    blocksize = options.blocksize
    compress  = options.compress
    progress  = options.progress
    verbose   = options.verbose

    if verbose:
        print "Block size is %0.1f MB" % (float(blocksize) / MIBI)

    # server
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

    if verbose:
        print "server: %s" % " ".join(cmd)

    s = subprocess.Popen(cmd, bufsize=0, stdin=subprocess.PIPE, stdout=subprocess.PIPE, close_fds=True)
    s_in, s_out = s.stdin, s.stdout

    s_size = s_out.readline().strip()
    s_size = int(s_size)

    # client
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

    if verbose:
        print "client: %s" % " ".join(cmd)

    c = subprocess.Popen(cmd, bufsize=0, stdin=subprocess.PIPE, stdout=subprocess.PIPE, close_fds=True)
    c_in, c_out = c.stdin, c.stdout

    c_size = c_out.readline().strip()
    c_size = int(c_size)

    if c_size != s_size:
        print "size mismatch"
        c_out.readline().strip()
        c_in.write(ABORT+EOL)
        c_in.flush()
        s_out.readline().strip()
        s_in.write(ABORT+EOL)
        s_in.flush()
        sys.exit(1)

    same_blocks = diff_blocks = 0

    print "Starting sync..."
    t0 = time.time()
    t_last = t0
    size_blocks = c_size / blocksize
    i = -1
    while True:
        i += 1
        c_sum = c_out.readline().strip()
        if c_sum == '':
            break
        s_sum = s_out.readline().strip()
        if s_sum == '':
            # TODO
            # this shouldn't happen for now
            sys.exit(1)
            pass
        if c_sum == s_sum:
            c_in.write(SAME+EOL)
            c_in.flush()
            s_in.write(SAME+EOL)
            s_in.flush()
            same_blocks += 1
        else:
            c_in.write(DIFF+EOL)
            c_in.flush()
            block = c_out.read(blocksize)
            s_in.write(DIFF+EOL)
            s_in.flush()
            s_in.write(block)
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
    parser = OptionParser(usage="%prog [options] {file|ssh}://[[<user>@]<host>/]<source> {file|ssh}://[[<user>@]<host>/]<dest>")
    parser.add_option("-b", "--blocksize", dest="blocksize", action="store", type="int", help="block size (bytes)", default=MIBI)
    parser.add_option("-c", "--compress",  dest="compress",  action="store_true", default=False, help="use compression")
    parser.add_option("-p", "--progress",  dest="progress",  action="store_true", default=False, help="display progress")
    parser.add_option("-v", "--verbose",   dest="verbose",   action="store_true", default=False, help="be chatty")
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
    except:
        parser.print_help()
        print __doc__
        sys.exit(1)

    sync(src, dst, options)
