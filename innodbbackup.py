#! /usr/bin/env python2.7
"""
Usage:
  innodbbackup.py --full [(--no-prepare | --prepare-mem=MEM)] [--compress --compress-threads=CTHREADS --no-check] [options] [<BACKUP_BASE_PATH>]
  innodbbackup.py --generate-pigz
  innodbbackup.py --inc=LEVEL [options] [<BACKUP_BASE_PATH>]
  innodbbackup.py --inc-prepare [--prepare-mem=MEM] [--compress --compress-threads=CTHREADS --no-check] [options] [<BACKUP_BASE_PATH>]

Options:
  --threads=THREADS      Backup threads  [default: 1]
  --keep=DAYS            Keep DAYS number of days backups  [default: 1]
  --enc                  Encrypt using openssl with passphrase
  --slave-safe           Stops SQL Thread for consistent slave backup
  --throttle=IOPS        IOPS in MB/S
  --login-path           Use mysql login-path 

Other:
  --full                 Full backup
  --inc=LEVEL            Increamental
  --inc-prepare          Prepare the incremental backup
  --no-prepare           Skip prepare step
  --prepare-mem=MEM      Extra memory for preparing backup  [default: 1G]
  --defaults-file=DFILE  Use as the defaults file for useful for multi instance databases
  --generate-pigz        Generate pigz scripts for threaded compression
  --compress             Compress prepared or not prepared backup if no-prepared option
  --compress-threads=CTHREADS      Compress threads using pigz  [default: 1]
  --no-check             Skip checking the compressed archive

"""
#FIXME add option for nocheck space
#FIXME add option to use login-path
#FIXME update alogrith for checking space get du datadir 1.X of target_path
#TODO add help to generate the pigz_N scripts in the working directory

import os
import re
import shutil
import socket
import subprocess
import sys 
import glob
from datetime import datetime
from time import sleep

#import MySQLdb
import mysql.connector
import humanfriendly
import pyzmail
import psutil
from docopt import docopt
#from passlib.hash import cisco_type7

import clean
import etc.innodbbackup_config as config
try:
    from secureconfig import SecureConfigParser, SecureConfigException, zeromem
except ImportError as e:
    print(e)
    from lib.secureconfig import SecureConfigParser, SecureConfigException, zeromem
    sys.exit(0)

__version__ = "0.9"
__author__ = "Mark Gruenberg"

scfg = SecureConfigParser.from_file(config.key_path)
scfg.read(config.config_path)
section = 'backup'

"""
TODO FIXME Is this still true?
Notes: subprocess handles exceptions better in python 2.7. This script was designed to run with the default python
version in centos 2.6
"""


class BackupError(Exception):
    def __init__(self, message, subject_message, error_system, level, email_addresses=None):
        try:
            print(message)
            self.subject = self.build_subject(subject_message, error_system, level)
            run_sql('set global innodb_flush_log_at_trx_commit=2;set global old_alter_table=0;')
        except AttributeError as e:
            pass


    def get_hostname(self):
        hostname = os.uname()[1]
        return hostname.split('.')[0]

    def get_ipaddress(self):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(('google.com', 0))
            ip_address = s.getsockname()[0]
        except socket.error as e:
            ip_address = None
        return ip_address

    def build_subject(self, subject_message, error_system, level):
        """
        [ERROR][BACKUP][MDLI01VMD02@162.218.136.43] Not Enough Space Sat 2016-07-30 13:51:19
        """
        ip = self.get_ipaddress()
        hostname = self.get_hostname()
        curr_time = datetime.now().strftime('%a %Y-%m-%d %H:%M:%S')
        host = '{}'.format(hostname) if ip is None else '{}@{}'.format(hostname, ip)
        subject = '[{}][{}][{}] {} {}'.format(level.upper(), error_system.upper(), host, subject_message, curr_time)
        return subject

    def send_mail(self, text_content, html_content, email_addresses=None):
        #subject = self.build_subject(subject_message, level)
        subject = self.subject
        sender = config.sender
        if email_addresses == None:
            recipients = config.email_addresses
        else:
            recipients = email_addresses
        payload, mail_from, rcpt_to, msg_id = pyzmail.compose_mail(
                sender,
                recipients,
                subject,
                config.prefered_encoding,
                text=(text_content, config.text_encoding),
                html=(html_content, config.html_encoding)
                # attachments=[('attached content', 'text', 'plain', 'text.txt', 'us-ascii')]
        )

        # print payload

        smtp_host = 'localhost'
        smtp_port = 587
        smtp_mode = 'tls'
        smtp_login = 'my.gmail.addresse@gmail.com'
        smtp_password = 'my.gmail.password'

        print('Sending email ...')
        ret = pyzmail.send_mail(payload, mail_from, rcpt_to, smtp_host  # ,
                                # smtp_port=smtp_port, smtp_mode=smtp_mode,
                                # smtp_login=smtp_login, smtp_password=smtp_password
                                )


class BackupErrorNoSpace(BackupError):
    def __init__(self, value, level):
        level = 'Error'
        message = '{0}: Not enough space remaining: {1} on {2} '\
                  'for backups to continue'.format(level, humanfriendly.format_size(value[1]), value[0])
        subject_message = 'Not Enough Space'
        super(BackupErrorNoSpace, self).__init__(message, subject_message, 'BACKUP', level)
        text = ('Host: {}'.format(self.get_hostname()),
                'IP: {}'.format(self.get_ipaddress()),
                '',
                'Backup terminated on error. Not enough space',
                'Space remaining on {0}: {1}'.format(value[0], humanfriendly.format_size(value[1])))
        self.send_mail('\n'.join(text), '<p style="margin: .2em .2em;">'.join(text))
        print('Exiting backup at {} ...'.format(datetime.now().strftime('%a %Y-%m-%d %H:%M:%S')))
        sys.exit(-1)


class BackupErrorPermissions(BackupError):
    def __init__(self, message, error):
        level = 'Error'
        message1 = 'Error {0}: Error: {1.errno}, {1.strerror} Path: {1.filename}'.format(message, error)
        subject_message = '{1.strerror} {0}'.format(message, error)
        super(BackupErrorPermissions, self).__init__(message1, subject_message, 'BACKUP', level)
        text = ('Host: {}'.format(self.get_hostname()),
                'IP: {}'.format(self.get_ipaddress()),
                '',
                'Backup terminated on error. {}'.format(error.strerror),
                'Creating directory: {}'.format(error.filename))
        self.send_mail('\n'.join(text), '<p style="margin: .2em .2em;">'.join(text))
        print('Exiting backup at {} ...'.format(datetime.now().strftime('%a %Y-%m-%d %H:%M:%S')))
        sys.exit(-1)


class BackupErrorTarFailed(BackupError):
    def __init__(self, message, error):
        level = 'Error'
        message1 = 'Error {0}: Error: {1.returncode} Path: {1.cmd[5]} File: {1.cmd[3]}'.format(message, error)
        subject_message = '{0}'.format(message)
        print('Removing failed tarball: {0.cmd[3]}'.format(error))
        os.remove(error.cmd[3])
        super(BackupErrorTarFailed, self).__init__(message1, subject_message, 'BACKUP', level)
        text = ('Host: {}'.format(self.get_hostname()),
                'IP: {}'.format(self.get_ipaddress()),
                '',
                'Backup terminated Successfully',
                '',
                'Tar failed with status: {0.returncode}'.format(error),
                'Removing failed tarball: {0.cmd[3]}'.format(error),
                'on Directory: {0.cmd[5]} and File: {0.cmd[3]}'.format(error),
                '',
                'command: {}'.format(' '.join(error.cmd)))
        self.send_mail('\n'.join(text), '<p style="margin: .2em .2em;">'.join(text))
        print('Exiting backup at {} ...'.format(datetime.now().strftime('%a %Y-%m-%d %H:%M:%S')))
        sys.exit(-1)


class BackupErrorBackupFailed(BackupError):
    def __init__(self, process, backup_path, message=None, error=None):
        level = 'Error'
        message1 = '{} ended with error.'.format(process)
        subject_message = '{} ended with error'.format(process)
        super(BackupErrorBackupFailed, self).__init__(message1, subject_message, 'BACKUP', level)
        text = ('Host: {}'.format(self.get_hostname()),
                'IP: {}'.format(self.get_ipaddress()),
                'Backup Path: {}'.format(backup_path),
                '',
                '{} terminated on error.'.format(process))
        self.send_mail('\n'.join(text), '<p style="margin: .2em .2em;">'.join(text))
        print('Exiting backup at {} ...'.format(datetime.now().strftime('%a %Y-%m-%d %H:%M:%S')))
        sys.exit(-1)


class BackupErrorBackup(BackupError):
    """
    backup status, prepare status, tar and encyption
    if any are error send email

    backup success and prepare error - possible recovery
    backup and prepare OK and tar error - recoverable
    backup and prepare and tar OK and enc error - recoverable
    """
    #def __init__(self, value, level):
    #    super(BackupErrorBackup, self).__init__(message, subject_message, 'BACKUP', level)

    backup_status = None
    tar_status = None
    enc_status = None

    def __init__(self, value=None, level=None):
        subject_message = 'Not Enough Space'
        message = ''
        super(BackupErrorBackup, self).__init__(message, subject_message, 'BACKUP', level)

    def set_backup_status(self, status):
        self.backup_status = status

    def set_tar_status(self, status):
        self.tar_status = status

    def set_enc_status(self, status):
        self.enc_status = status

    def get_statuses(self):
        return (self.backup_status, self.tar_status, self.enc_status)


def build_full(opts):
    # get user
    hostname = os.uname()[1].split('.')[0]
    backup_base = config.base_dir if not opts['<BACKUP_BASE_PATH>'] else opts['<BACKUP_BASE_PATH>']
    current_datetime = datetime.now()
    current_date = datetime.strftime(current_datetime, '%Y-%m-%d')
    current_time = datetime.strftime(current_datetime, '%H-%M-%S')
    dir_name = '_'.join([current_date, current_time, 'FULL', hostname])
    current_backup_base = os.path.join(backup_base, current_date, 'FULL')
    #current_backup_path = os.path.join(backup_base, current_date, 'FULL', current_time)
    current_backup_path = os.path.join(backup_base, current_date, 'FULL', dir_name)
    current_backup_base = os.path.normpath(current_backup_base)
    assert current_backup_base not in ('/', '/bin', '/boot', '/dev', '/etc', '/home', '/lib', '/lib64', 
                                       '/media', '/mnt', '/opt', '/proc', '/root', '/data',
                                       '/data/mysql', '/data/backup', '/sbin', '/tmp', '/usr', '/var'), \
        'Bad backup_path {0}'.format(os.path.normpath(backup_base))

    try:
        os.makedirs(current_backup_base)
    except OSError as e:
        if e.errno == 13:
            raise BackupErrorPermissions('creating backup directory', e)
        elif e.errno == 17:
            print('Warning path already exists {0}'.format(e))
        else:
            raise BackupErrorPermissions('creating backup directory', e)

    threads = '--parallel={0}'.format(opts['--threads'])
    slave_safe = '--slave-safe-backup' if opts['--slave-safe'] else ''
    throttle_io = '--throttle={}'.format(opts['--throttle']) if opts['--throttle'] else ''
    port = '--port={0}'.format(scfg.get(section, 'port'))
    backup_user = '--user={0}'.format(scfg.get(section, 'backup_user'))
    secret = '--password={}'.format(scfg.get(section, 'secret')) if len(scfg.get(section, 'secret'))>0 else ''
    secret_hide = '--password={}'.format('xxxxxxx') if len(secret) > 0 else ''
    target_dir = '--target-dir={}'.format(current_backup_path)

    try:
        if config.defaults_file != '':
            defaults_file = '--defaults-file={}'.format(config.defaults_file)
        else:
            defaults_file = ''
    except AttributeError as e:
        defaults_file = ''

    #FIXME add logic to use login-path
    cmd = ['xtrabackup', defaults_file, '--backup', backup_user, '--no-timestamp',
           threads, '--host=127.0.0.1', secret, '--slave-info', slave_safe, port,
           '--lock-ddl', throttle_io, target_dir]
    cmd_hide = ['xtrabackup', defaults_file, backup_user, '--no-timestamp',
           threads, '--host=127.0.0.1', '--backup', secret_hide, '--slave-info', slave_safe, port,
           '--lock-ddl', throttle_io, target_dir]
    return ([p for p in cmd if p != ''], 
            [p for p in cmd_hide if p != ''], 
            current_backup_path, 
            current_backup_base, 
            backup_base)


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
    except OSError as e:
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
    threads = '--parallel={0}'.format(opts['--threads'])
    target_dir = '--target-dir={}'.format(backup_path)
    if opts['--prepare-mem']:
        prepare_mem = '--use-memory={0}'.format(opts['--prepare-mem'])
    else:
        prepare_mem = ''
    cmd = ['xtrabackup', '--prepare', threads, prepare_mem, target_dir]
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
        succ = successful_re.search(out.decode('ascii'))
    except TypeError as e:
        succ = successful_re.search('\n'.join(out))
    issucc = True if succ else False
    return issucc


def gen_pigz_thread_helper():
    import stat

    for p in range(2, psutil.cpu_count()*2 + 1):
        fname = '{}/pigz_{}'.format(config.working_dir, p)
        with open(fname, 'w') as f:
            f.write('pigz -p {}\n'.format(p))
        os.chmod(fname, stat.S_IRWXU | stat.S_IRWXG | stat.S_IROTH)


def check_pigz_treads(threads):
    threads = int(threads)
    assert threads > 0 and threads <= psutil.cpu_count()*2, 'Compress threads are limited to a maximum of 2 x core count'
    pigz = []
    pigz += [f for f in glob.glob(config.working_dir + "/pigz_[1-9]")]
    pigz += [f for f in glob.glob(config.working_dir + "/pigz_[1-9][0-9]")]
    pigz += [f for f in glob.glob(config.working_dir + "/pigz_[1-9][0-9][0-9]")]
    try:
        max_pigz = max(pigz, key=lambda x:int(x.split('_')[-1]))
        min_pigz = min(pigz, key=lambda x:int(x.split('_')[-1]))

        max_pigz_threads = int(max_pigz.split('_')[-1])
        if threads > max_pigz_threads:
            threads = max_pigz_threads
            print('WARNING Compression threads adjusted to {} threads'.format(max_pigs))
    except:
        if len(pigz) == 0:
            print('WARNING: No pigz helper scripts in working directory. To build run with --generate-pigz')
        else:
            print('WARNING Unexpected error occured in checking compression threads.')
        print('        threads set to 1')
        threads = 1

    return threads


def tar_dir(backup_path, threads=1, check=True):
    backup_path = os.path.normpath(backup_path)
    assert backup_path not in ('/', '/bin', '/boot', '/dev', '/etc', '/home',
                               '/lib', '/lib64', '/media', '/mnt', '/opt', '/proc',
                               '/root', '/data', '/data/mysql', '/data/backup',
                               '/sbin', '/tmp', '/usr', '/var'), 'Bad backup_path {0}'.format(backup_path)

    tar_file = backup_path + '.tgz'
    print('tar: {0}'.format(tar_file))
    if threads == 1:
        cmd = ['tar', '-czf', tar_file, '-C', backup_path, '.']
    else:
        cmd = ['tar', '--use-compress-program={}/pigz_{}'.format(config.working_dir, threads), '-cf', tar_file, '-C', backup_path, '.']
    print('TAR: {0}'.format(' '.join(cmd)))
    try:
        out = subprocess.check_call(cmd)
        print('TAR completed.')
    except subprocess.CalledProcessError as e:
        raise BackupErrorTarFailed('tar failed', e)
    else:
        if check:
            print('Validating tarball - this can take some time')
            try:
                with open(os.devnull, "w") as fnull:
                    subprocess.check_call(('tar', '--use-compress-program=pigz', '-tf', tar_file), stdout=fnull)
                print('TAR Validated Successfully')
                print('TAR Removing tar directory {0}'.format(backup_path))
                shutil.rmtree(backup_path, ignore_errors=True)
            except subprocess.CalledProcessError as e:
                print('Error: Tar Check Failed: {0}'.format(e))
        else:
            print('TAR Removing tar directory {0}'.format(backup_path))
            shutil.rmtree(backup_path, ignore_errors=True)
    return tar_file


def check_space(backup_path):
    """
    get space using by db
    space_limit = space_used(1.3) = (space_used + space_used*1.1 + space_used*1.2)
    """
    space = os.statvfs(backup_path)
    #spare = 90
    spare = 1
    if space.f_bavail * space.f_bsize < 1024 * 1024 * 1024 * spare:
        raise BackupErrorNoSpace((backup_path, space.f_bavail * space.f_bsize), 'ERROR')


def encrypt(archive_path, passphase_path):
    """
    Encrypt a compressed backup using openssl with a passphrase stored in a file so it's not
    passed on the command line
    :param archive_path: Path to the compressed backup archive
    :param passphase_path: Path to password used for openssl encryption
    :return:
    """
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
    except subprocess.CalledProcessError as e:
        print('Error during encryption {}'.format(e))


def run_sql(sql):
    if not isinstance(sql, (list, tuple)):
        sql = [s for s in sql.split(';') if s!='']
    
    config = {
        'user': scfg.get(section, 'super_user'),
        'passwd': scfg.get(section, 'super_secret'),
        'port': int(scfg.get(section, 'port')),
        }
    db = mysql.connector.connect(**config)
    cur = db.cursor()

    for s in sql:
        print('Running sql: {}'.format(s))
        cur.execute(s)
    cur.close()
    db.close()


def run_backup(cmd, cmd_hide):
    """
    run backup cmd
    :param cmd: list
    :return: issucc Boolean
    """
    # commented but this is handled by xtrabackup
    #run_sql('stop slave sql_thread;set global slave_parallel_workers=0;start slave sql_thread;set global innodb_flush_log_at_trx_commit=1;flush engine logs;set global old_alter_table=1')
    #TODO FIXME
    #run_sql('set global innodb_flush_log_at_trx_commit=1;flush engine logs;set global old_alter_table=1')
    #print('Sleeping for 60 seconds while logs flush')
    #TODO FIXME
    #sleep(60)
    print(' '.join(cmd_hide))
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    print('BACKUP OUT: {0}'.format('-' * (90 - 12)))
    rtncode = p.poll()
    out = p.communicate()
    for line in out[0].splitlines():
        print('{0}'.format(line.decode('utf-8')))
    print('{0}'.format('-' * 90))
    issucc = check_success(out[0])
    sql = [s for s in 'set global innodb_flush_log_at_trx_commit=2;'.split(';') if s!='']
    #TODO FIXME
    #run_sql('set global innodb_flush_log_at_trx_commit=2;set global old_alter_table=0;stop slave sql_thread;set global slave_parallel_workers=4;start slave sql_thread;')
    #run_sql('set global innodb_flush_log_at_trx_commit=2;set global old_alter_table=0')
    return issucc


def main(opts):
    """ builds a backup command string list for subprocess from options """

    if arguments['--generate-pigz']:
        gen_pigz_thread_helper()
        sys.exit(0)

    if arguments['--full']:
        cmd, cmd_hide, backup_path, backup_base, top_backup_base = build_full(arguments)
        clean.clean_backups(top_backup_base, int(arguments['--keep']), False)
        check_space(top_backup_base)
        succ = run_backup(cmd, cmd_hide)
        print('Backup ended {0}'.format(('Error', 'Successfully')[succ]))
        if not succ: raise BackupErrorBackupFailed('Backup', backup_path)
        if succ and not opts['--no-prepare']:
            cmd = build_full_prepare(opts, backup_path)
            succ = run_backup(cmd, cmd_hide)
            print('Prepare ended {0}'.format(('Error', 'Successfully')[succ]))
            if not succ: raise BackupErrorBackupFailed('Prepare', backup_path)
        if succ and (opts['--compress'] or int(opts['--compress-threads'])>0):
            threads = check_pigz_treads(opts['--compress-threads'])
            tar_file = tar_dir(backup_path, threads, check=not opts['--no-check'])
            if opts['--enc']:
                encrypt(tar_file, config.pass_phrase)
    elif arguments['--inc']:
        build_inc(arguments)


def create_backup_dir(backup_type, level=None):
    None


if __name__ == '__main__':
    arguments = docopt(__doc__, version=__version__)
    print(arguments)
    start = datetime.now()
    main(arguments)
    end = datetime.now()
    print('Backup completed at {0} in {1}'.format(end, end - start))
