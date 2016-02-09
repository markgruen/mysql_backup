#! /usr/bin/env python
"""
Usage:
  mydumperbackup.py [options] [<path>]

Arguments:
  path                                          Top level path to backups

Options:
  -D <database>, --database <database>           Database to backup
  -T <tables> ..., --tables <tables> ...         Coma separated list of tables
  -i <engine>..., --ignore-engines <engine>...   Ignore engines
  -x <re>, --regex <re>                          Regular expression for 'db.table' matching
  -e, --build-empty-files                        Build dump files even if no data available from table
  -t <threads>, --threads <threads>              Number of backup threads [default: 4] 
  -c, --compress                                 Compress output files
  -m, --no-schemas                               Do not dump table schemas with the data
  -F <size>, --chunk-filesize <size>             Split tables output into files SIZE in MB. [default: 1]
  -U <days>, --updated-since <days>              Dump tables only updated in the last <days> days
  -K <days>, --keep-days <days>                  Keep <days> backups [default: 3]
  --less-locking                                 Minimize locking time on InnoDB tables.
  --use-savepoints                               Use savepoints to reduce metadata locking issues, needs SUPER privilege
  -?, --help                                     Display this help message

Example:
  mydumperbackup.py 

"""

import etc.innodbbackup_config as config
import subprocess
import os
import re
import sys
from datetime import datetime
from lib.docopt import docopt
from schema import Schema, And, Or, Use, Optional, SchemaError
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


def build_mydumper(**args):
    '''
    build the mydumper command and path
    '''
    default_options =['--tiggers','--routines','--build-empty-files','']
    backup_base = config.base_dir if args['<path>'] is None else args['<path>']
    current_datetime = datetime.now()
    current_date = datetime.strftime(current_datetime, '%Y-%m-%d')
    current_time = datetime.strftime(current_datetime, '%H-%M-%S')
    current_backup_base = os.path.normpath(os.path.join(backup_base, current_date, 'MYDUMPER'))
    current_backup_path = os.path.normpath(os.path.join(backup_base, current_date, 'MYDUMPER', current_time))


    args.pop('<path>')
    args['--outputdir'] = current_backup_path
    args = {k:v for k,v in args.iteritems() if v is not None and v != False}
    flags = {k:v for k,v in args.iteritems() if v and isinstance(v, bool)}.keys()
    options = {k:v for k,v in args.iteritems() if not(isinstance(v, bool))}
    cmd = ['mydumper'] + [' '.join(map(str,r)) for r in options.items()] + flags + \
        ['-h 127.0.0.1', '--port '+ str(config.port), '--user '+ config.backup_user] + \
        default_options

    print(' '.join(cmd))
    return cmd, current_backup_path, current_backup_base, backup_base

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
    assert opts['--inc'] >= 0, ('ASSERT ERROR: bad value {} for --inc'.format(opts['--inc']))

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
    print('CMD: {}'.format(' '.join(cmd)))
    print('BACKUP OUT: {0}'.format('-'*(90-12)))
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    #rtncode = p.poll()
    out = p.communicate()
    rtncode = p.poll()
    for line in out[0].splitlines():
        print('{0}'.format(line))
    print('{0}'.format('-'*90))
    #issucc = check_success(out[0])
    issucc = True if rtncode == 0 else False
    return issucc


def main(**args):
    ''' builds a backup command string list for subpross from options '''

    keep_days = args.pop('--keep-days')
    cmd, backup_path, backup_base, top_backup_base = build_mydumper(**args)
    #check_space(top_backup_base, keep_days, False)
    succ = run_backup(cmd)
    print('Backup ended {0}'.format(('Error', 'Successfully')[succ]))
    #if succ and args['--compress']:
    #    tar_dir(backup_path)


def create_backup_dir(backup_type, level=None):
    None


if __name__ == '__main__':
    args = docopt(__doc__, version=__version__)
    schema = Schema({
        '<path>': Or(None, And(os.path.exists, error='{0} Not Valid Path'.format(args['<path>']))),
        '--chunk-filesize': Use(int, error='Must be integer with size in MB'),
        '--keep-days': Use(int, error='keep_days must be an integer value'),
        '--threads': Use(int, error='threads must be an integer value'),
        '--updated-since': Or(None, And(Use(int), error='updated since must be integer days')),
        #'--regex': Or(None, And(str, lambda s: re.sub(r'\\|..'
        object: object
        })
    try:
        args = schema.validate(args)
    except SchemaError, e:
        sys.exit(e.code)
    print args
    start = datetime.now()
    main(**args)
    #build_mydumper(**args)
    end = datetime.now()
    print('Backup completed at {0} in {1}'.format(end, end-start))

    """
    {'--build-empty-files': False,
     '--chunk-filesize': None,
     '--compress': False,
     '--database': None,
     '--help': False,
     '--ignore-engines': None,
     '--less-locking': False,
     '--no-schemas': False,
     '--regex': '.*health\\..*',
     '--tables': None,
     '--threads': '6',
     '--updated-since': None,
     '--use-savepoints': False}
    schema = Schema({
        'path': And(os.path.exists, error='{0} Not a valid path'.format(args['<path>'])),
        '--chunk-filesize': Use(int, error='Must be integer with size in MB'),
        '--keep_days': Use(int, error='keep_days must be an integer value'),
        })

    schema = Schema({
        'FILE': [Use(open, error='FILE should be readable')],
        'PATH': And(os.path.exists, error='PATH should exist'),
            '--count': Or(None, And(Use(int), lambda n: 0 < n < 5),
                error='--count=N should be integer 0 < N < 5')})
    """
