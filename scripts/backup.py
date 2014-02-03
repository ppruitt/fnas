#!/usr/bin/env python
'''Backs up a zpool to another zpool(s), which are usually housed on 
   external disks
'''
 
import argparse
import logging
import datetime
import os.path
import subprocess
import sys

class BackupException(Exception):
    '''Exception thrown if an error is encuntered during backup'''
    def __init__(self, value):
        self.value = value
    
    def __str__(self) :
        return __str__(self.value)

def parse_args():
    '''Parses command line arguments and returns an argument object'''
    parser = argparse.ArgumentParser()

    parser.add_argument('-d', metavar = 'ZPOOL_NAME', dest = 'dest_zpools', 
                        action = 'append',
                        required = True,
                        help = 'Destination zpool. This option may be specified multiple times')


    parser.add_argument('-e', dest = 'use_existing_snapshots',
                        action = "store_true",
                        default = False,
                        help = 'Uses the latest snapshot rather than creating a new one')

    parser.add_argument('-v', dest = 'verbose',
                        action = "store_true",
                        default = False,
                        help = 'Produce verbose output')

    parser.add_argument('--dry-run', dest = 'dry_run',
                        action = "store_true",
                        default = False,
                        help = 'Don''t actually run commands')
    
    parser.add_argument('dataset', nargs='+')

    args = parser.parse_args()
    return args

def make_ts_str():
    '''Creates a timestamp string containing the date and time'''
    now = datetime.datetime.utcnow()
    return now.strftime('%Y%m%d-%H%m')

def make_zfs_snapshots(snapshot_names):
    cmd_list = ['zfs', 'snapshot'] + snapshot_names
    retval = subprocess.call(cmd_list)
    if retval != 0:
        raise BackupException('Failed to create zfs snapshots. Error code: %d' % retval)

def validate_datasets(datasets):
    '''Checks that the dataset exists'''
    cmd_list = ['zfs', 'list'] + datasets
    retval = subprocess.call(cmd_list)
    if retval != 0:
        raise BackupException('Failed to validate zfs datasets. Error code: %d' % retval)

def validate_zpools(pools):
    cmd_list = ['zpool', 'status', '-x'] + pools
    retval = subprocess.call(cmd_list)
    if retval != 0:
        raise BackupException('Failed to validate zfs pools. Error code: %d' % retval)
    
def existing_snapshots(dataset):
    '''Returns a list of snapshots for the dataset in ascending order of creation'''
    cmd_list = ['zfs', 'list', '-t', 'snapshot', '-s', 'creation', '-r', dataset]
    snapshot_str = subprocess.check_output(cmd_list)
    snapshot_list = [x.split(' ')[0] for x in snapshot_str.splitlines()[1:]]
    return snapshot_list

def strip_zpool(dataset):
    '''Removes the pool (first component) of the dataset path'''
    pathlist = dataset.split('/')
    return '/'.join(pathlist[1:])
    
def strip_snapshot(dataset):
    '''Removes snapshot "@xxx" from the dataset name'''
    comps = dataset.split('@')
    return comps[0]

def zpool(dataset):
    return dataset.split('/')[0]

def common_snapshots(a, b):
    '''Return a list of snapshots in common.
       Order is maintained'''
    ap = [strip_zpool(s) for s in a]
    bp = [strip_zpool(s) for s in b]
    return [s for s in ap if s in bp]

def run_shell_cmd(cmd_str, logger, dry_run = False):
    '''Run a shell command if a dry run is not specified'''
    if not args.dry_run:
        retval = subprocess.call(cmd_str, shell = True)
        if retval != 0:
            logger.error('zfs send/receive returned an error: %d' % retval)
        else:
            logger.info('Cmd: %s' % cmd_str)
 
if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(message)s', level = logging.INFO)
    logger = logging.getLogger('backup')

    args = parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)
        logger.debug('Verbose')

    try:
        validate_zpools(args.dest_zpools)
        validate_datasets(args.dataset)

        # Get existing snapshots by dataset
        snapshots = {}
        for dataset in args.dataset:
            snapshots[dataset] = existing_snapshots(dataset)
        
        if not args.use_existing_snapshots:
            snapshot_name = make_ts_str()            
            logger.info('Snapshot name is %s' % snapshot_name)
            
            # Add snapshots being created to end of list
            new_snapshots = []
            for d in args.dataset:
                cur_snapshot = '%s@%s' % (d, snapshot_name)                
                snapshots[d].append(cur_snapshot)
                new_snapshots.append(cur_snapshot)
            logger.debug('Creating ZFS snapshots: ' + ', '.join(snapshots))
            if not args.dry_run:
                make_zfs_snapshots(new_snapshots)
        else:
            logger.debug('Attempting to use latest snapshot rather than creating.')
            
        for dest_pool in args.dest_zpools:
            for source_dataset in args.dataset:
                source_snaps = snapshots[source_dataset]
                dest_dataset = os.path.join(dest_pool, strip_zpool(source_dataset))
                try :
                    dest_snaps = existing_snapshots(dest_dataset)
                    common_snaps = common_snapshots(source_snaps, dest_snaps)                    
                    if common_snaps:
                        incr_snap = os.path.join(zpool(source_dataset), common_snaps[-1])

                        logger.info('Sending incremental stream from %s to %s for %s' % \
                                    (incr_snap, source_snaps[-1], dest_dataset))
                        run_shell_cmd('zfs send -v -p -I "%s" "%s" | zfs receive "%s"' % \
                                      (incr_snap, source_snaps[-1], dest_dataset),
                                      logger, args.dry_run)
                    else:
                        logger.info('No common snapshots. Sending non-incremental package for %s' % source_snaps[-1])
                        run_shell_cmd('zfs send -v -p "%s" | zfs receive "%s"' % \
                                      (source_snaps[-1], dest_dataset),
                                      logger, args.dry_run)
                
                except subprocess.CalledProcessError, e:
                    logger.info('Destination dataset %s does not exist. Sending full replication stream' % dest_dataset)
                    run_shell_cmd('zfs send -v -R "%s" | zfs receive "%s"' % \
                                  (source_snaps[-1], dest_dataset),
                                  logger, args.dry_run)
    except BackupException, e:
        logger.fatal(str(e))
    except subprocess.CalledProcessError, e:
        logger.fatal(str(e))

    logger.info('%s exiting.' % sys.argv[0])
