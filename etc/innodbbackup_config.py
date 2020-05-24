import os

__author__ = 'markgruenberg'

# working directory
working_dir = os.path.dirname(os.path.realpath(__file__))
working_dir = os.path.normpath(os.path.join(working_dir, '..'))

# backup user
backup_user = 'backup_user'
super_user = 'root'

# secureconfig
key_path = '/root/.keys/db.key'
config_path = '/root/.secrets/config.ini'

# base backup directory
base_dir = '/data/backup/XBACKUP'

#defaults file
defaults_file = '/etc/my.cnf'

#file with passphrase. This should be a secured with .passphease with 400 permissions
pass_phrase = '/root/.secret.txt'

# database port
port = 3306

# path to mydumper
mydumper_path = '/usr/local/bin'

# email addresses
email_addresses = [
    ('Mark Gruenberg', 'veryverygoofy@gmail.com')
]

sender = (u'Mark Gruenberg', 'mgruenberg@markgruenberg.site')

prefered_encoding = 'iso-8859-1'
text_encoding = 'iso-8859-1'
html_encoding = 'iso-8859-1'
