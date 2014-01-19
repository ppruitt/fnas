#!/usr/bin/env python

import cPickle
import argparse
import logging
import sqlite3

# Directory entry indexes
(D_IDX_ID, 
 D_IDX_PARENT_ID, 
 D_IDX_PATH, 
 D_IDX_STAT, 
 D_IDX_CHILD_DIRS, 
 D_IDX_CHILD_FILES, 
 D_IDX_HASH, 
 D_IDX_TOTAL_SIZE) = range(8)

# File entry indexes
(F_IDX_PATH,
 F_IDX_ID,
 F_IDX_DIR_ID,
 F_IDX_STAT,
 F_IDX_HASH) = range(5)

def pretty_bytes(bytes):
    '''Print bytes in friendly units'''
    (MB, GB) = (1024**2, 1024**3)
    if bytes > GB:
        return '%0.2f GB' % (float(bytes) / GB)
    else:
        return '%0.2f MB' % (float(bytes) / MB)

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s', level = logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('--db', metavar = 'DB', dest = 'db', 
                        required = True,
                        help = 'Sqlite3 database')

    parser.add_argument('-p', metavar = 'pickle file', dest = 'pickle_file', 
                        required = True,
                        help = 'File containing pickled output from the make_tree_fingerprints command')

    args = parser.parse_args()
    
    logging.info('Connecting to DB: %s' % args.db)
    conn = sqlite3.connect(args.db)
    conn.text_factory = str

    logging.info('Loading pickled data structures from %s' % args.pickle_file)
    with open(args.pickle_file, 'rb') as infile:
        unpickler = cPickle.Unpickler(infile)
        logging.info('Loading dirents.')
        dir_ents = unpickler.load()
        
        logging.info('Loading hashes.')
        hashes = unpickler.load()
    
    logging.info('Creating duplicate list.')    
    dup_list = []
    for hash_str, paths in hashes.items():
        if len(paths) > 1:
            dup_list.append((paths, dir_ents[paths[0]][D_IDX_TOTAL_SIZE]))

    logging.info('Sorting duplicate list by size')
    dup_list.sort(key = lambda d: d[1], reverse = True)

    for paths, size in dup_list:
        print '%20s %s' % (pretty_bytes(size), ','.join(paths))
