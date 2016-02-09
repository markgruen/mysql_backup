#! /usr/bin/env python
"""
Usage:
    innodbbackup.py --full [--threads THREADS] [--compress] [--no-prepare] [--prepare-mem MEM] [--keep DAYS] [BACKUP_BASE_PATH]
    innodbbackup.py --inc LEVEL [--threads THREADS] [--compress] [--no-prepare] [--prepare-mem MEM] [--keep DAYS] [BACKUP_BASE_PATH]

Options:
    --full              full backup
    --inc=LEVEL         increamental
    --threads=THREADS   backup threads  [default: 1]
    --compress          compress prepared or not prepared backup if no-prepared option
    --no-prepare        skip prepare step
    --prepare-mem=MEM   extra memory for preparing backup
    --keep=DAYS         keep DAYS number od days backups [default: 3]

"""


import etc.innodbbackup_config as config
import subprocess
import os
import re
import sys
from datetime import datetime
from lib.docopt import docopt
import shutil
import clean

__version__ = "0.8-alpha"
__author__ = "Mark Gruenberg"

"""
Notes: subprocess handles exceptions better in python 2.7. This script was designed to run with the default python
version in centos 2.6
"""

class ErrorNoSpace(Exception):
    def __init__(self, value):
        print('ERROR: Not enough space remaining {1} on {0} for backups to continue'.format(*value))
        sys.exit(-1)

def build_full(opts):
    #get user
    if not opts['BACKUP_BASE_PATH']:
        backup_base = config.base_dir
    else:
        backup_base = opts['BACKUP_BASE_PATH']
    current_datetime = datetime.now()
    current_date = datetime.strftime(current_datetime, '%Y-%m-%d')
    current_time = datetime.strftime(current_datetime, '%H-%M-%S')
    current_backup_base = os.path.join(backup_base, current_date, 'FULL')
    current_backup_path = os.path.join(backup_base, current_date, 'FULL', current_time)

    current_backup_base = os.path.normpath(current_backup_base)
    assert current_backup_base not in ('/', '/bin', '/boot', '/dev', '/etc', '/home',
                           '/lib', '/lib64', '/media','/mnt', '/opt', '/proc',
                           '/root', '/data', '/data/mysql', '/data/backup',
                           '/sbin', '/tmp', '/usr', '/var'), 'Bad backup_path {0}'.format(backup_path)

    try:
        os.makedirs(current_backup_base)
    except OSError, e:
        if e.errno == 13:
            print("ERROR don't have permissions to create {0}".format(current_backup_base))
            sys.exit(1)
        elif e.errno == 17:
            print('Warning path aready exists {0}'.format(e))
        else:
            print('ERROR: Creating base backup directory, error: {0}'.format(e))
            sys.exit(1)

    backup_user = '--user={0}'.format(config.backup_user)
    threads = '--parallel={0}'.format(opts['--threads'])
    port = '--port={0}'.format(config.port)
    cmd = ['innobackupex', backup_user, '--no-timestamp', 
            threads, '--host=127.0.0.1', port, current_backup_path]
    return cmd, current_backup_path, current_backup_base, backup_base

def build_inc(opts):
    print('{0}'.format('-'*80))
    print('INC backups not yet implementated')
    print('{0}'.format('-'*80))
    """
    if --full == False
     if --inc == 0
        DATE
            INC
                timestamp
                    base
    elsif inc > 0
        test if base exists
        create
            DATE
                INC
                    timestamp
                        base
                        inc1_timestamp
                        inc2_timestamp

    """
    assert opts['--full'] == False, ('ASSERT ERROR: full backup in incremental setup.')
    assert opts['--inc'] >= 0, ('ASSERT ERROR: bad value {} for --inc'.format(opts['--inc'))

    if not opts['BACKUP_BASE_PATH']:
        backup_base = config.base_dir
    else:
        backup_base = opts['BACKUP_BASE_PATH']

    current_datetime = datetime.now()
    current_date = datetime.strftime(current_datetime, '%Y-%m-%d')
    current_time = datetime.strftime(current_datetime, '%H-%M-%S')
    current_backup_base = os.path.join(backup_base, current_date, 'INC')
    inc_base_path = os.path.join(backup_base, current_date, 'INC', current_time, 'base')
    if opts['--inc'] == 0:
    current_backup_path = os.path.join(backup_base, current_date, 'INC', current_time)

    current_backup_base = os.path.normpath(current_backup_base)
    assert current_backup_base not in ('/', '/bin', '/boot', '/dev', '/etc', '/home',
                           '/lib', '/lib64', '/media','/mnt', '/opt', '/proc',
                           '/root', '/data', '/data/mysql', '/data/backup',
                           '/sbin', '/tmp', '/usr', '/var'), 'Bad backup_path {0}'.format(backup_path)

    try:
        os.makedirs(current_backup_base)
    except OSError, e:
        if e.errno == 13:
            print("ERROR don't have permissions to create {0}".format(current_backup_base))
            sys.exit(1)
        elif e.errno == 17:
            print('Warning path aready exists {0}'.format(e))
        else:
            print('ERROR: Creating base backup directory, error: {0}'.format(e))
            sys.exit(1)

    backup_user = '--user={0}'.format(config.backup_user)
    threads = '--parallel={0}'.format(opts['--threads'])
    port = '--port={0}'.format(config.port)
    cmd = ['innobackupex', backup_user, '--no-timestamp',
            threads, '--host=127.0.0.1', port, current_backup_path]
    return cmd, current_backup_path, current_backup_base, backup_base

def build_full_prepare(opts, backup_path):

    backup_user = '--user={0}'.format(config.backup_user)
    threads = '--parallel={0}'.format(opts['--threads'])
    if opts['--prepare-mem']:
        prepare_mem = '--use-memory={0}'.format(opts['--prepare-mem'])
    else:
        prepare_mem = ''
    cmd = ['innobackupex', '--apply-log', '--redo-only', backup_user, threads, prepare_mem, backup_path]
    return [c for c in cmd if c != '']

def check_success(out):
    """
    checks the innodbbackup for the success string
    :rtype : object
    :param out: innodb log file
    :return: Boolean
    """

    successful_re = re.compile(r'(?<!")completed OK!', re.I|re.M)
    try:
        succ = successful_re.search(out)
    except TypeError, e:
        succ = successful_re.search('\n'.join(out))
    issucc = True if succ else False
    return issucc

def tar_dir(backup_path, check=True):

    backup_path = os.path.normpath(backup_path)
    assert backup_path not in ('/', '/bin', '/boot', '/dev', '/etc', '/home',
                               '/lib', '/lib64', '/media','/mnt', '/opt', '/proc',
                               '/root', '/data', '/data/mysql', '/data/backup',
                               '/sbin', '/tmp', '/usr', '/var'), 'Bad backup_path {0}'.format(backup_path)

    name = os.path.basename(backup_path)
    tar_file = backup_path + '.tgz'
    print('tar: {0}'.format(tar_file))
    cmd = ['tar', '--use-compress-program=pigz', '-cf', tar_file, '-C', backup_path, '.']
    print('TAR: {0}'.format(' '.join(cmd)))
    try:
        out = subprocess.check_call(cmd)
        print('TAR completed.')
    except subprocess.CalledProcessError, e:
        print('Error: Tar Compression Failed: {0}'.format(e))
        print('Removing failed tarball: {0}'.format(tar_file))
        os.remove(tar_file)
    else:
        if check:
            print('Validating tarball - this can take some time')
            try:
                with open(os.devnull, "w") as fnull:
                    subprocess.check_call(('tar', '--use-compress-program=pigz', '-tf', tar_file), stdout=fnull)
                print('TAR Validated Successfully')
                print('TAR Removing tar directory {0}'.format(backup_path))
                shutil.rmtree(backup_path, ignore_errors=True)
            except subprocess.CalledProcessError, e:
                print('Error: Tar Check Failed: {0}'.format(e))
        else:
            print('TAR Removing tar directory {0}'.format(backup_path))
            shutil.rmtree(backup_path, ignore_errors=True)

def check_space(backup_path):
    space = os.statvfs(backup_path)
    if space.f_bavail * space.f_bsize < 1024*1024:
        raise ErrorNoSpace((backup_path,space.f_bavail * space.f_bsize))

def run_backup(cmd):
    """
    run backup cmd
    :param cmd: list
    :return: issucc Boolean
    """
    print(' '.join(cmd))
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    print('BACKUP OUT: {0}'.format('-'*(90-12)))
    rtncode = p.poll()
    out = p.communicate()
    for line in out[0].splitlines():
        print('{0}'.format(line))
    print('{0}'.format('-'*90))
    issucc = check_success(out[0])
    return issucc

def main(opts):
    ''' builds a backup command string list for subpross from options '''
    if arguments['--full']:
        cmd, backup_path, backup_base, top_backup_base = build_full(arguments)
        clean.clean_backups(top_backup_base, int(arguments['--keep']), False)
        check_space(top_backup_base)
        succ = run_backup(cmd)
        print('Backup ended {0}'.format(('Error', 'Successfully')[succ]))
        if succ and not opts['--no-prepare']:
            cmd = build_full_prepare(opts, backup_path)
            succ = run_backup(cmd)
            print('Prepare ended {0}'.format(('Error', 'Successfully')[succ]))
        if succ and opts['--compress']:
            tar_dir(backup_path)
    elif arguments['--inc']:
        build_inc(arguments)

def create_backup_dir(backup_type, level=None):
    None

if __name__ == '__main__':
    arguments = docopt(__doc__, version=__version__)
    print arguments
    start = datetime.now()
    main(arguments)
    end = datetime.now()
    print('Backup completed at {0} in {1}'.format(end, end-start))
