# Description
-----------
JP2k-distiller is a collection of Python scripts that deploy, manage, schedule 
and distribute conversion of various document types to Jpeg-2000 format. Current
 version (2.0) supports distributed conversion of PDF and TIFF files. 
JP2k-distiller's highly distributed nature and scalability up to hundreds of 
machines are among its main features. It can be easily configured via it's 
configuration file. The already well-known Adore-Djatoka project is used as 
conversion and compression engine.

# JP2k-distiller Architecture
---------------------------
JP2k-distiller uses a multilayered distributed software architecture. The system
 consists of master and worker/slave nodes. ZeroMQ is used as the main transport
message layer among different nodes, GlusterFS is used as the elastic common 
datastore between all worker nodes and the master server.

# Installation
------------------

### MASTER: ###
* Create a user named jdistiller with uid 1000 (useradd -u 1000). 
* Clone dmaster folder to a master node (physical/virtual server) in jdistiller 
home folder 
* Run as root:
``
yum groupinstall 'Development Tools' python-devel python-virtualenv
``
* Run as jdistiller in /home/jdistiller:
``
virtualenv PYTHON;
source PYTHON/bin/activate;
pip install -r requirements.txt
``
* Copy the .bashrc file (found inside dmaster/init folder), in jdistiller home 
directory (/home/jdistiller).
* Copy the init script from master/init to /etc/init.d/jdistiller, and run: 
``
chmod 755 /etc/init.d/jdistiller
``
as root.
* Mount the shared filesystem  by adding the following line in /etc/fstab:
(ex. for a shared folder named SILO)
``<server-name>:/SILO /SILO nfs defaults,soft,_netdev 0 0`` 
Note: for the first mount:
``mount -t nfs <server-name>:/SILO /SILO
``

* Follow the instructions found in dmaster/distiller_master.conf, to configure
your installation.
* Start Master services with /etc/init.d/jdistiller start

### SLAVE ###
* Create a user named jdistiller with uid 1000 (useradd -u 1000). 
* Clone dslave folder to a slave node (physical/virtual server) in jdistiller
home folder.
* Run as root:
``
yum groupinstall 'Development Tools' python-devel python-virtualenv
``
* Run as jdistiller in /home/jdistiller:
``
virtualenv PYTHON;
source PYTHON/bin/activate;
pip install -r requirements.txt;
``
* Copy the .bashrc file (found inside dslave/init folder), in jdistiller home 
directory (/home/jdistiller).
* Copy the init script from slave/init to /etc/init.d/jdistiller, and run:
``
chmod 755 /etc/init.d/jdistiller
``
as root.
* Mount the shared filesystem  by adding the following line in /etc/fstab:
(ex. for a shared folder named SILO)
``<server-name>:/SILO /SILO nfs defaults,soft,_netdev 0 0``
Note: for the first mount: 
``mount -t nfs <server-name>:/SILO /SILO
``

* Install java
* Install ghostscript:
``yum install ghostscript
yum install ghostscript-fonts``

* Start Slave services with /etc/init.d/jdistiller start



# Conversion Workflow Mechanism
-----------------------------
The workflow mechanism of the distiller searches the unconverted directory for 
a particular directory structure starting with the journal_id in the root-path,
followed by the article_id. Finally the article_id directory should contain the
unconverted pdf or tiff files. Then the distiller creates two directories, the 
first one is the pdf_id.png directory which contains the converted page-by-page
png files from the first step of the conversion path for the pdf files. The 
second step is the creation of a second directory named either pdf_id.jp2 for 
the pdf files or article_id.jp2 for the directory which contains the tiff files.
The jp2 directory contains the JPEG-2000 files. If the conversion path doesn't 
encounter any errors then the distiller moves the converted directories to their
final local and remote destinations. If an errors occur, e.g. the pdf file 
cannot be converted then the distiller removes this directory from the 
conversion queue, leaves the created pdf_id.png directory in the article_id 
directory and does not remove the directory to the final local/remote 
destination. Errors and information are logged in every step of the above
process and emails are sent to a group of users included in the mailing list 
of the configuration files.
