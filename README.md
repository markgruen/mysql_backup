# mysql_backup
wrapper for xtrabackup

* updated to use xtrabackup
* added secureconfig
* supports pigz thread compression
* compatable with python3
* python3 will requiring using an off branch version of secureconfig
* refactored docopt option arguments
* added option to generate pigz helper scripts

to setup to use python3 

```
wget https://github.com/cimichaelm/python3-secureconfig/archive/master.zip
python3 setup.py install 

change:

```#! /usr/bin/env python2.7```

to:

```#! /usr/bin/env python3.6```
```

## Before first backup run create the pigz helper scripts 

  ./innodbbackup_pigz.py --generate-pigz

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
