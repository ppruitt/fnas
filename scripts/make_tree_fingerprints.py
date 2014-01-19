#!/usr/bin/env python

import hashlib
import os
import sqlite3
import argparse
import stat
import cPickle
import logging

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

BYTES_PER_LOG_MESSAGE = 1024 * 1024 * 1024
FILES_PER_COMMIT = 1000
HASH_READ_BLOCKSIZE = 1024 * 1024

last_log_bytes = 0

def pretty_bytes(bytes):
    '''Print bytes in friendly units'''
    (MB, GB) = (1024**2, 1024**3)
    if bytes > GB:
        return '%0.2f GB' % (float(bytes) / GB)
    else:
        return '%0.2f MB' % (float(bytes) / MB)

def log_progress(cur_path, file_count, byte_count) :
    global last_log_bytes
    if (byte_count - last_log_bytes > BYTES_PER_LOG_MESSAGE):
        last_log_bytes = byte_count
        logging.info('%s bytes read, %d files, processing: %s' % (pretty_bytes(byte_count), file_count, cur_path))
 
def compute_dir_hashes(dir_entry, cursor, hash_dict):
    '''Computes hashes for directories'''
    if len(dir_entry[D_IDX_CHILD_DIRS]) > 0:
        # compute hashes for all children
        for child_dir in dir_entry[D_IDX_CHILD_DIRS]:
            logging.debug('Recurse %s' % child_dir[D_IDX_PATH])
            compute_dir_hashes(child_dir, cursor, hash_dict)

    # hash all child file and directory hashes
    h = hashlib.sha256()

    # Size of a directory is the recursive sum of all child files and child directories
    total_size = 0

    logging.debug('Hashing %s..' % dir_entry[D_IDX_PATH])
    
    # hash the hashes of all child files in alpha order of their names
    for file_ent in dir_entry[D_IDX_CHILD_FILES]:
        assert file_ent[F_IDX_HASH] is not None
        h.update(file_ent[F_IDX_HASH])
        total_size += file_ent[F_IDX_STAT].st_size

    # hash the hashes of all child dirs in alpha order of their names
    for child in dir_entry[D_IDX_CHILD_DIRS]:
        assert child[D_IDX_HASH] is not None
        h.update(child[D_IDX_HASH])
        total_size += child[D_IDX_TOTAL_SIZE]

    hash_str = h.hexdigest()
    dir_entry[D_IDX_HASH] = hash_str
    dir_entry[D_IDX_TOTAL_SIZE] = total_size
    logging.debug('Hash for %s is %s' % (dir_entry[D_IDX_PATH], dir_entry[D_IDX_HASH]))
    hash_dict.setdefault(hash_str, []).append(dir_entry[D_IDX_PATH]) 
    cursor.execute('update files set hash = ?, total_size = ? where id = ?',
                   (hash_str, total_size, dir_entry[D_IDX_ID]))

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s', level = logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('--db', metavar = 'DB', dest = 'db', 
                        required = True,
                        help = 'Sqlite3 database')

    parser.add_argument('--path', metavar = 'DIR', dest = 'path', 
                        required = True,
                        help = 'Root of the directory tree to hash')

    parser.add_argument('--exclude', metavar = 'DIR', dest = 'exclude', default = None,
                        help = 'Full path to exclude')
    
    args = parser.parse_args()
    
    logging.info('Connecting to DB: %s' % args.db)

    conn = sqlite3.connect(args.db)
    conn.text_factory = str

    cursor = conn.cursor()
    cursor.execute('''create table if not exists files 
                        (id     integer primary key asc autoincrement, 
                         parent integer references files (id),
                         path   text,
                         type   integer,
                         mode   integer,
                         uid    integer,
                         gid    integer,
                         nlink  integer,
                         hash   text,
                         size   integer,
                         mtime  real,
                         total_size integer)''')
    conn.commit()

    file_count = 0
    byte_count = 0
    last_commit_count = 0

    is_root = True

    dir_hashes = {}
    dir_data = {}

    logging.info('Scanning path: %s' % args.path)
    if args.exclude:
        logging.info('Excluding path: %s' % args.exclude)

    # Walk the directory tree in top-down order (directories visited after files)
    for dirpath, dirnames, filenames in os.walk(args.path, topdown = True):        
        file_count += 1

        # sort directory names (allowed with topdown visitation)
        dirnames.sort()

        if args.exclude and args.exclude == dirpath:
            logging.info('Skipping %s dirpath' % dirpath)
            del dirnames[:]
            continue

        if is_root:
            parent = None
            is_root = False
            parent_id = None
        else:
            parent_dir = os.path.dirname(dirpath)
            parent = dir_data[parent_dir]
            parent_id = parent[D_IDX_ID]

        # Insert Directory
        dstat = os.lstat(dirpath)
        cursor.execute('''insert into files (parent, path, type, mode, uid, gid, nlink, hash, size, mtime)
                              values (?,?,?,?,?,?,?,?,?,?)''',
                       (parent_id,
                        dirpath,
                        stat.S_IFMT(dstat.st_mode),
                        dstat.st_mode,
                        dstat.st_uid,
                        dstat.st_gid,
                        dstat.st_nlink,
                        None,
                        dstat.st_size,
                        dstat.st_mtime))

        dir_id = cursor.lastrowid
        file_ents = []
        dir_ents = []
        cur_dir_ent = [dir_id, parent_id, dirpath, dstat, dir_ents, file_ents, None, 0] # id, parent_id, stat info, dir children, file children, hash, total_size
        dir_data[dirpath] = cur_dir_ent
        # add directory to parent
        if parent:
            parent[D_IDX_CHILD_DIRS].append(cur_dir_ent)

        if parent_id is None:
            parent_id = -1

        log_progress(dirpath, file_count, byte_count)
                
        # visit the files in sorted order
        for fname in sorted(filenames):
            fullname = os.path.join(dirpath, fname)
            pstat = os.lstat(fullname)
            ftype = stat.S_IFMT(pstat.st_mode)
            
            h = hashlib.sha256()            
            # Only hash the contents regular files
            if stat.S_ISREG(pstat.st_mode):
                with open(fullname, 'rb') as f:
                    data = f.read(HASH_READ_BLOCKSIZE)
                    while len(data) > 0:
                       h.update(data)
                       byte_count += len(data)
                       data = f.read(HASH_READ_BLOCKSIZE)

            hash_str = h.hexdigest()
                       
            cursor.execute('''insert into files (parent, path, type, mode, uid, gid, nlink, hash, size, mtime)
                              values (?,?,?,?,?,?,?,?,?,?)''',
                    (dir_id,
                     fullname,
                     stat.S_IFMT(dstat.st_mode),
                     pstat.st_mode,
                     pstat.st_uid,
                     pstat.st_gid,
                     pstat.st_nlink,
                     hash_str,
                     pstat.st_size,
                     pstat.st_mtime))
 
            file_ents.append((fullname, cursor.lastrowid, dir_id, pstat, hash_str)) 
            file_count += 1
            if file_count - last_commit_count > FILES_PER_COMMIT:
                conn.commit()
                last_commit_count = file_count
            
            log_progress(fullname, file_count, byte_count)

    conn.commit()

    # Visit all directories and compute hashes
    logging.info('Computing directory hashes.')
    compute_dir_hashes(dir_data[args.path], cursor, dir_hashes)

    conn.commit()

    logging.info('Pickling.')
    with file('treedata.pickle', 'wb') as pfile:
        pickler = cPickle.Pickler(pfile)
        pickler.dump(dir_data)
        pickler.dump(dir_hashes)
    logging.info('Completed processing of %d files and %d bytes.' % (
              file_count, byte_count))
