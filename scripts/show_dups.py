#!/usr/bin/env python
"""Opens Pickled hash files and finds duplicative directory trees"""
 
import cPickle
import argparse
import logging
import os.path

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

    parser.add_argument('-p', metavar = 'pickle file', dest = 'pickle_file', 
                        required = True,
                        help = 'File containing pickled output from the make_tree_fingerprints command')

    parser.add_argument('-e', dest = 'check_exists',
                        action = "store_true",
                        default = False,
                        help = 'Check the existence of files/directories before listing')

    args = parser.parse_args()

    if args.check_exists:
        logging.info('Will check paths for existence.')

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
        if args.check_exists:
            # Keep only existing paths
            paths = [p for p in paths if os.path.exists(p)]

        if len(paths) > 1:
            print('%20s %s' % (pretty_bytes(size), paths[0]))
            for path in paths[1:] :
                print('%20s %s' % (' ', path))
