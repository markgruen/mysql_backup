#! /usr/bin/env python
"""
Usage:
  innodbbackup.py --full [(--no-prepare | --prepare-mem=MEM)] [--compress --no-check --threads=THREADS --keep=DAYS --enc] [<BACKUP_BASE_PATH>]
  innodbbackup.py --inc=LEVEL [--threads=THREADS --keep=DAYS] [<BACKUP_BASE_PATH>]
  innodbbackup.py --inc-prepare [--compress --no-check --threads=THREADS --keep=DAYS --enc] [--prepare-mem=MEM] [<BACKUP_BASE_PATH>]

Options:
  --compress             Compress prepared or not prepared backup if no-prepared option
  --no-check             Skip checking the compressed archive
  --threads=THREADS      Backup threads  [default: 1]
  --keep=DAYS            Keep DAYS number of days backups  [default: 1]
  --enc                  Encrypt using openssl with passphrase

Other:
  --full                 Full backup
  --inc=LEVEL            Increamental
  --inc-prepare          Prepare the incremental backup
  --no-prepare           Skip prepare step
  --prepare-mem=MEM      Extra memory for preparing backup  [default: 1G]
  --defaults-file=DFILE  Use as the defaults file for useful for multi instance databases

"""


import etc.innodbbackup_config as config
import subprocess
import os
import re
import sys
from datetime import datetime
from docopt import docopt
import shutil
import clean

__version__ = "0.8.1.1-alpha"
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
    # get user
    backup_base = config.base_dir if not opts['<BACKUP_BASE_PATH>'] else opts['<BACKUP_BASE_PATH>']
    current_datetime = datetime.now()
    current_date = datetime.strftime(current_datetime, '%Y-%m-%d')
    current_time = datetime.strftime(current_datetime, '%H-%M-%S')
    current_backup_base = os.path.join(backup_base, current_date, 'FULL')
    current_backup_path = os.path.join(backup_base, current_date, 'FULL', current_time)
    current_backup_base = os.path.normpath(current_backup_base)
    assert current_backup_base not in ('/', '/bin', '/boot', '/dev', '/etc', '/home',
                   '/lib', '/lib64', '/media', '/mnt', '/opt', '/proc',
                   '/root', '/data', '/data/mysql', '/data/backup',
                   '/sbin', '/tmp', '/usr', '/var'), 'Bad backup_path {0}'.format(os.path.normpath(backup_base))

    try:
        os.makedirs(current_backup_base)
    except OSError, e:
        if e.errno == 13:
            print("ERROR don't have permissions to create {0}".format(current_backup_base))
            sys.exit(1)
        elif e.errno == 17:
            print('Warning path already exists {0}'.format(e))
        else:
            print('ERROR: Creating base backup directory, error: {0}'.format(e))
            sys.exit(1)

    backup_user = '--user={0}'.format(config.backup_user)
    threads = '--parallel={0}'.format(opts['--threads'])
    port = '--port={0}'.format(config.port)
    try:
        if config.defaults_file != '':
            defaults_file = '--defaults-file={}'.format(config.defaults_file)
        else:
            defaults_file = ''
    except AttributeError, e:
        defaults_file = ''

    cmd = ['innobackupex', defaults_file, backup_user, '--no-timestamp',
           threads, '--host=127.0.0.1', port, current_backup_path]
    return cmd, current_backup_path, current_backup_base, backup_base


def build_inc(opts):
    print('{0}'.format('-' * 80))
    print('INC backups not yet implementated')
    print('{0}'.format('-' * 80))
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
    assert opts['--full'] == False, 'ASSERT ERROR: full backup in incremental setup.'
    assert opts['--inc'] >= 0, ('ASSERT ERROR: bad value {} for --inc'.format(opts['--inc']))
    # FIXME current backup path, backup_path
    backup_base = config.base_dir if not opts['<BACKUP_BASE_PATH>'] else opts['<BACKUP_BASE_PATH>']
    current_datetime = datetime.now()
    current_date = datetime.strftime(current_datetime, '%Y-%m-%d')
    current_time = datetime.strftime(current_datetime, '%H-%M-%S')
    current_backup_base = os.path.join(backup_base, current_date, 'INC')
    inc_base_path = os.path.join(backup_base, current_date, 'INC', current_time, 'base')
    if opts['--inc'] == 0:
        current_backup_path = os.path.join(backup_base, current_date, 'INC', current_time)

    current_backup_base = os.path.normpath(current_backup_base)
    assert current_backup_base not in ('/', '/bin', '/boot', '/dev', '/etc', '/home',
                                       '/lib', '/lib64', '/media', '/mnt', '/opt', '/proc',
                                       '/root', '/data', '/data/mysql', '/data/backup',
                                       '/sbin', '/tmp', '/usr', '/var'), 'Bad backup_path {0}'.format(backup_path)

    try:
        os.makedirs(current_backup_base)
    except OSError, e:
        if e.errno == 13:
            print("ERROR don't have permissions to create {0}".format(current_backup_base))
            sys.exit(1)
        elif e.errno == 17:
            print('Warning path already exists {0}'.format(e))
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

    successful_re = re.compile(r'(?<!")completed OK!', re.I | re.M)
    try:
        succ = successful_re.search(out)
    except TypeError, e:
        succ = successful_re.search('\n'.join(out))
    issucc = True if succ else False
    return issucc


def tar_dir(backup_path, check=True):
    backup_path = os.path.normpath(backup_path)
    assert backup_path not in ('/', '/bin', '/boot', '/dev', '/etc', '/home',
                               '/lib', '/lib64', '/media', '/mnt', '/opt', '/proc',
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
    return tar_file

def check_space(backup_path):
    space = os.statvfs(backup_path)
    if space.f_bavail * space.f_bsize < 1024 * 1024:
        raise ErrorNoSpace((backup_path, space.f_bavail * space.f_bsize))


def encrypt(archive_path, passphase_path):
    """
    Encrypt a compressed backup using openssl with a passphrase stored in a file so it's not
    passed on the command line
    :param archive_path: Path to the compressed backup archive
    :param passphase_path: Path to password used for openssl encryption
    :return:
    """
    # openssl enc -e -aes-256-cbc -pass file:/root/.secret.txt -in sso.2016020907.sql.gz -out sso.2016020907.sql.gz.enc
    assert os.path.exists(archive_path), "Error can't find backup archive '{}'".format(archive_path)
    assert os.path.exists(passphase_path), "Error can't open pass phrase file '{}'".format(passphase_path)
    in_file = '-in {}'.format(archive_path)
    out_file = '-out {}.enc'.format(archive_path)
    passphrase = '-pass file:{}'.format(passphase_path)

    cmd = ['openssl', 'enc', '-e', '-aes-256-cbc'] + passphrase.split() + \
            in_file.split() + out_file.split()
    print('encrypting: {}'.format(' '.join(cmd)))
    try:
        print('Encrypting {} to {}'.format(in_file, out_file))
        subprocess.check_call(cmd, shell=False)
        print('Cleaning up and removing {}'.format(in_file))
        os.remove(archive_path)
    except subprocess.CalledProcessError, e:
        print('Error during encryption {}'.format(e))


def run_backup(cmd):
    """
    run backup cmd
    :param cmd: list
    :return: issucc Boolean
    """
    print(' '.join(cmd))
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    print('BACKUP OUT: {0}'.format('-' * (90 - 12)))
    rtncode = p.poll()
    out = p.communicate()
    for line in out[0].splitlines():
        print('{0}'.format(line))
    print('{0}'.format('-' * 90))
    issucc = check_success(out[0])
    return issucc


def main(opts):
    """ builds a backup command string list for subpross from options """
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
            if opts['--no-check']:
                tar_file = tar_dir(backup_path, check=False)
            else:
                tar_file = tar_dir(backup_path)
            if opts['--enc']:
                encrypt(tar_file, config.pass_phrase)
    elif arguments['--inc']:
        build_inc(arguments)


def create_backup_dir(backup_type, level=None):
    None


if __name__ == '__main__':
    arguments = docopt(__doc__, version=__version__)
    print arguments
    start = datetime.now()
    #encrypt('/disk2/backup/XBACKUP/2016-02-09/FULL/08-51-32.tgz', '/root/.secret.txt')
    #sys.exit()
    main(arguments)
    end = datetime.now()
    print('Backup completed at {0} in {1}'.format(end, end - start))
