# mysql_backup
wrapper for xtrabackup

* updated to use xtrabackup
* added secureconfig
* supports pigz thread compression
* compatible with python3
* python3 requires using an off branch version of secureconfig
* refactored docopt option arguments
* added option to generate pigz helper scripts

To setup to use python3 

```
sudo pip3 install setuptools --upgrade
sudo yum install gcc python3-devel
```


Adding requirements

```sudo pip3 install -r requirements.txt```

You can add python3 branch of secureconfig manually using:

```
  wget https://github.com/cimichaelm/python3-secureconfig/archive/master.zip
  sudo python3 setup.py install 
```

change:

```#! /usr/bin/env python2.7```

to:

```#! /usr/bin/env python3.6```

## Before first backup run create the pigz helper scripts 

```./innodbbackup_pigz.py --generate-pigz```

## create the secure config secrets file

```
cat /root/.secrets/config.ini
[backup]
port = 3306
host = '127.0.0.1'
backup_user = backup_user
super_user = root

secret = CK_FERNET::gAAAAABdYFLoRxwc89xVHY_JFBXXXXXXXXXXXXXXXXXXXIPfITLSXXXXXXXXXXXXXXXXXXXGfcY95vocwNujhe_kNX2-gLCCCD3fyM_glEpKYvwft8kfmXM=
super_secret = CK_FERNET::gAAAAABdYXXXXXXXXXXXXXXXXXXXfc9iAVMWj_AHR_o-A0DQXXXXXXXXXXXXXXXXXXXx2r63qzbmI6fGDzgdBvn6mrtQhxH1mA==
```
## update encrypted passwords

Use the secrets_config_editor.py to update or add encrypted passwords to your securets configurations file:
https://github.com/markgruen/connect_rds_through_proxy_host

```
./secrets_config_editor.py --help
Secret Config Editor

Used to add encrypted secrets to configuration files.

Usage:
  secret_config_editor.py set --section <SECTION> --config <PATH> --key <KEY_PATH>
  secret_config_editor.py --version
  secret_config_editor.py --help

Options:
  -h --help            Show this screen.
  --version            Show version.
  --section=<SECTION>  Section to add encrypted password.
  --config=<PATH>      Path to config file.
  --key=<KEY_PATH>     Path to key file.

Notes:
   This tool will add an encrypted password attribute to the configuration file. If the
   key file does exist the tool will create one on the first run.

   I suggest storing keys in your home directory under a hidden directory ~/.keys


Example:

   ./secrets_config_editor.py set --section=uweee_dev --config=~/config.ini --key=~/.keys/db.key

```
