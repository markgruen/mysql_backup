# mysql_backup
wrapper for xtrabackup

* updated to use xtrabackup
* added secureconfig
* supports pigz thread compression
* now compatable with python3
* python3 will requiring using an off branch version of secureconfig
* refactored docopt option arguments
* added option to generate pigz helper scripts

to setup to use python3 secure config

wget https://github.com/cimichaelm/python3-secureconfig/archive/master.zip
python3 setup.py install 

Before first backup run create the pigz helper scripts 

./innodbbackup_pigz.py --generate-pigz


