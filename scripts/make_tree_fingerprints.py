#!/usr/bin/env python

import hashlib
import os
import sqlite3
import argparse
import stat
import cPickle

def compute_dir_hashes(dir_entry, cursor, hash_dict):
    '''Computes hashes for directories'''
    if len(dir_entry[4]) > 0:
        # sort child vector by directory name
        dir_entry[4].sort(key = lambda x: x[2])

        # compute hashes for all children
        for child_dir in dir_entry[4]:
            print 'Recurse %s' % child_dir[2]
            compute_dir_hashes(child_dir, cursor, hash_dict)

    # hash all child file and directory hashes
    h = hashlib.sha256()
    # hash dir name (without path)
    h.update(os.path.basename(dir_entry[2]))

    print 'Hashing %s..' % dir_entry[2]

    for file_ent in dir_entry[5]:
        assert file_ent[4] is not None
        h.update(file_ent[4])
        
    for child in dir_entry[4]:
        if child[6] is None:
            print child
        assert child[6] is not None
        h.update(child[6])

    hash_str = h.hexdigest()
    dir_entry[6] = hash_str

    print 'Hash for %s is %s' % (dir_entry[2], dir_entry[6])
    hash_dict.setdefault(hash_str, []).append(dir_entry[2]) 
    cursor.execute('update files set hash = ? where id = ?',
                   (hash_str, dir_entry[0]))
    
        
def pretty_bytes(bytes):
    '''Print bytes in friendly units'''
    (MB, GB) = (1024**2, 1024**3)
    if bytes > GB:
        return '%0.2f GB' % (float(bytes) / GB)
    else:
        return '%0.2f MB' % (float(bytes) / MB)


if __name__ == '__main__':
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
                         mtime  real)''')
    conn.commit()


    (FTYPE_FIFO, 
     FTYPE_CHR, 
     FTYPE_DIR,
     FTYPE_BLOCK, 
     FTYPE_REG, 
     FTYPE_LNK, 
     FTYPE_SOCK) = (0010000,
                    0020000,
                    0040000,
                    0060000,
                    0100000,
                    0120000,
                    0140000)

    file_count = 0
    byte_count = 0
    last_commit_count = 0

    files_per_commit = 1000
    count = 0
    blocksize = 1024 * 1024

    is_root = True

    dir_hashes = {}
    dir_data = {}

    for dirpath, dirnames, filenames in os.walk(args.path):        
        file_count += 1

        if args.exclude and args.exclude == dirpath:
            print 'Skipping %s dirpath' % dirpath
            continue

        if is_root:
            parent = None
            is_root = False
            parent_id = None
        else:
            parent_dir = os.path.dirname(dirpath)
            parent = dir_data[parent_dir]
            parent_id = parent[0]

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
        cur_dir_ent = [dir_id, parent_id, dirpath, dstat, dir_ents, file_ents, None] # id, parent_id, stat info, dir children, file children, hash
        dir_data[dirpath] = cur_dir_ent
        # add directory to parent
        if parent:
            parent[4].append(cur_dir_ent)

        if parent_id is None:
            parent_id = -1

        print 'Path %s, id = %d, parent = %d, total files = %d, total bytes = %s' % (
               dirpath, dir_id, parent_id, file_count, pretty_bytes(byte_count))
                
        # visit the files in sorted order
        for fname in sorted(filenames):
            fullname = os.path.join(dirpath, fname)
            pstat = os.lstat(fullname)
            ftype = stat.S_IFMT(pstat.st_mode)
            
            h = hashlib.sha256()            
            # hash our name (without path)
            h.update(fname)

            # Only hash the contents regular files
            if stat.S_ISREG(pstat.st_mode):
                with open(fullname, 'rb') as f:
                    data = f.read(blocksize)
                    while len(data) > 0:
                       h.update(data)
                       byte_count += len(data)
                       data = f.read(blocksize)

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
            if file_count - last_commit_count > files_per_commit:
                conn.commit()
                last_commit_count = file_count
            
    conn.commit()

    # Visit all directories and compute hashes
    print 'Computing directory hashes...'                   
    compute_dir_hashes(dir_data[args.path], cursor, dir_hashes)

    conn.commit()

    print 'Pickling...'
    with file('treedata.pickle', 'wb') as pfile:
        pickler = cPickle.Pickler(pfile)
        pickler.dump(dir_data)
        pickler.dump(dir_hashes)
    print 'Completed processing of %d files and %d bytes.' % (
              file_count, byte_count)
