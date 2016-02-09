#! /usr/bin/env python
"""
Usage: clean.py <path> <keep_days> [--trial-run]

Deletes directories named in the form YYYY-MM-DD

Arguments:
  path       Path the the directories that need to be cleaned
  keep_days  Delete directories older then keep_days

Options:
  -h, --help
  -t, --trial-run     will not delete

Example:
   clean.py /backup 2 

This will delete directories in the form YYYY-MM-DD that are more than 2 days old 

"""

import os
import sys
from datetime import datetime, timedelta, time
import shutil
from docopt import docopt
from schema import Schema, And, Or, Use, Optional, SchemaError

__author__ = 'markgruenberg'
__version__ = '0.1'

def clean_backups(base_directory, keep_days, test=True):
    base_directory = os.path.normpath(base_directory)
    assert base_directory not in ('/', '/bin', '/boot', '/dev', '/etc', '/home',
                           '/lib', '/lib64', '/media','/mnt', '/opt', '/proc',
                           '/root', '/data', '/data/mysql', '/data',
                           '/sbin', '/tmp', '/usr', '/var'), 'Bad backup_path {0}'.format(base_directory)

    trigger_date = datetime.combine(datetime.now().date() - timedelta(days=keep_days), time.min)
    print('Removing backup directories from {0} older then {1}'.format(base_directory, trigger_date.date()))
    for d in os.listdir(base_directory):
        try:
            d_date = datetime.strptime(d, '%Y-%m-%d')
            path = os.path.join(base_directory, d)
            if d_date < trigger_date:
                print('DELETING  {0}'.format(path))
                if not test:
                    ''' need to trap permissions error '''
                    shutil.rmtree(path, ignore_errors=True)
                    #print('REALLY DELETING {0}'.format(path))

            else:
                print('KEEPING   {0}'.format(os.path.join(path)))
        except ValueError, e:
            print("SKIPPING  {0} - doesn't match YYYY-MM-DD".format(os.path.join(base_directory, d)))

if __name__ == '__main__':
    args = docopt(__doc__, version=__version__)
    schema = Schema({
        '<path>': And(os.path.exists, error='{0} Not a valid path'.format(args['<path>'])),
        '<keep_days>': Use(int, error='keep_days must be an integer value'),
            Optional('--trial-run'): bool
        })

    """
    schema = Schema({
        'FILE': [Use(open, error='FILE should be readable')],
        'PATH': And(os.path.exists, error='PATH should exist'),
            '--count': Or(None, And(Use(int), lambda n: 0 < n < 5),
                error='--count=N should be integer 0 < N < 5')})
    """

    print(args)
    try:
        args = schema.validate(args)
    except SchemaError, e:
        sys.exit(e.code)
    clean_backups(args['<path>'], args['<keep_days>'], args['--trial-run'])

