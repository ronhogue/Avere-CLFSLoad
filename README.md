# Use Avere-CLFSLoad to populate Azure Blob cache storage

Avere CLFSLoad is a Python-based tool that copies data into Azure Blob storage
containers and stores it in Microsoft's Avere Cloud FileSystem (CLFS) format.
The proprietary CLFS format is used by the Azure HPC Cache and Avere vFXT for
Azure products.

This tool is a simple option for moving existing data to cloud storage for use
with specific Microsoft high-performance computing cache products. Because these
products use a proprietary cloud filesystem format to manage data, you must
populate storage by using the cache service instead of through a native copy
command. Avere-CLFSLoad lets you transfer data without using the cache - for
example, to pre-populate storage or to add files to a working set without
increasing cache load. (For other data ingest methods, read [Moving data to the
Avere vFXT for Azure 
cluster](<https://docs.microsoft.com/azure/avere-vfxt/avere-vfxt-data-ingest>).)

> **NOTE:** This is a preview release of CLFSLoad. Please report any issues to the email address in [Reporting problems](#reporting-problems), below. 

CLFSLoad runs on one Linux node and transfers a single rooted subtree. It
assumes that the source tree does not change during the transfer. 

The destination is an Azure Storage container. When the transfer is complete,
the destination container can be used with an Azure HPC Cache instance or Avere
vFXT for Azure cluster.

## Requirements

CLFSLoad requires Python version 3.6 or newer. Python 3.7 gives better
performance than 3.6.

Install Python 3 according to the instructions for your Linux distribution.

## Installation

You can install Avere-CLFSLoad on a physical Linux workstation or on a VM. (Read
[Running on Azure virtual machines](#running-on-azure-virtual-machines) for VM
recommendations.)

This example creates a Python virtual environment and installs CLFSLoad:

```bash
python3 -m venv $HOME/CLFSLoad < /dev/null
source $HOME/CLFSLoad/bin/activate
easy_install -U setuptools < /dev/null
python -B gen_setup.py
python -B setup.py install
```

This example creates a ``/CLFSLoad`` subdirectory in the user's HOME and
installs Avere-CLFSLoad there, but you can use any path. You might consider
installing it under ``/usr/CLFSLoad`` as root.

## Usage

To start a transfer, use a command like this:

```bash
CLFSLoad.py --new local_state_path source_root target_storage_account target_container sas_token
```

* *``local_state_path``* is a directory on the local disk. CLFSLoad uses this
  path to store temporary state and log files.

  If a transfer is interrupted (for example because of a system failure or
  reboot) you can resume the transfer using the contents of *local_state_path*
  by re-executing the command without the ``--new`` option.

* *``source_root``* is the path to the subtree that will be transferred. The
  path can be on a filesystem local to the node, or it can be remote (for
  example, an NFS mount).

* *``target_storage_account``* is the name of the Azure storage account to which
  the files in *``source_root``* will be copied. The account must exist before
  running CLFSLoad.

* *``target_container``* is the name of the Blob storage container within
  *``target_storage_account``* to which the files under *``source_root``* will
  be copied. This container must exist before running CLFSLoad.

  * A new transfer expects the container to be empty.
  * A resumed transfer expects to find the contents it previously wrote.

* *``sas_token``* is the secure access signature for accessing
  *``target_container``*.  
 
  For more information, see [Using shared access signatures
  (SAS)](https://docs.microsoft.com/azure/storage/common/storage-dotnet-shared-access-signature-part-1)

  Your shell will likely require you to quote or otherwise escape the
  *``sas_token``* value.

### Resuming an interrupted transfer

If a transfer is interrupted, it can be resumed instead of restarted from the
beginning. The command to resume a transfer is the same as the command used to
start it, but without the ``--new`` argument.

```bash
CLFSLoad.py local_state_path source_root target_storage_account target_container sas_token
```

### Important command-line options:

* *```compression``` Set this to LZ4 to enable compressing blob contents
  using LZ4. Set this to DISABLED to prevent blob content compression.
  Compressing blob contents reduces the capacity consumed by the
  target container and reduces network consumption while using the
  container when it is used at the cost of additional CPU cycles.
  Enabling LZ4 compression is recommended for most workloads.

* *```preserve_hardlinks``` When this is not enabled, an entity linked
  in the namespace more than once is transferred separately for each
  time it is found, and there is no relationship between these entities
  in the generated target container. When this is enabled, CLFSLoad
  utilizes inode number (```st_ino```) to infer identity relationships
  between entities with a link count (```st_nlink```) greater than one.
  Do not use this option if your source filesystem does not provide
  unique inode numbers for each unique entity.

### Running on Azure virtual machines

The recommended VM size for running Avere-CLFSLoad is Standard_F16s_v2.

For best performance, place your local state directory in ``/mnt``.

If you are using a Standard_L series VM size, you can use one of the NVME
devices as a local filesystem and place your local state directory there. This
option gives more capacity than using ``/mnt`` but does not typically give
better performance. 

### Using the populated Blob storage with Azure HPC Cache or Avere vFXT for Azure

Follow the directions in product documentation to define the populated container
as a backend storage target for your Azure HPC Cache or Avere vFXT for Azure.

The first time you attach the populated container, it might take a few minutes
before its ``/`` export appears and is ready to be added as a junction. This is
normal internal configuration overhead and not a cause for alarm.

## Troubleshooting

This troubleshooting section gives tips for resolving or avoiding common issues.

Note that many errors can be traced to incomplete Python 3 installation. The
installation process varies greatly depending on which Linux distribution you
are using. Always use the documentation specific to your Linux type and the
correct version number.

### Fatal error: Python.h: No such file or directory

An error like this indicates that your Python installation is incomplete:

```fatal error: Python.h: No such file or directory
#include <Python.h>
          ^~~~~~~~~~
compilation terminated.
```

You must install the development package and venv support.

apt example (used with Ubuntu):

```sudo apt-get update
sudo apt-get --assume-yes install python3-dev
sudo apt-get --assume-yes install python3-venv
```

Consult your Linux distribution's documentation to determine the best way to
install Python 3.

### Installing Python 3 on Ubuntu 

Ubuntu is a commonly used distribution for Microsoft Azure VMs. These example
steps demonstrate installing Python 3 on a modern Ubuntu distribution and then installing CLFSLoad.

```bash
sudo apt-get install python3-venv
sudo apt-get install gcc
sudo apt-get install python3-dev
sudo apt-get install unzip
python3 -m venv $HOME/CLFSLoad < /dev/null
source $HOME/CLFSLoad/bin/activate
easy_install -U setuptools < /dev/null
python setup.py install
```

### Installing Python 3 on Red Hat Enterprise Linux 7 

This example installs Python on RHEL 7.6. Refer to
<https://developers.redhat.com/blog/2018/08/13/install-python3-rhel/> for more
information. 

Follow these steps while logged in as root:

```bash
yum install -y @development
yum install -y rh-python36
yum install -y rh-python36-python-tools
yum install -y libffi-devel
yum install -y openssl
yum install -y openssl-devel
```
Next, take these steps from either a root login or user account to install CLFSLoad: 

```bash
scl enable rh-python36 bash
rm -rf $HOME/CLFSLoad
python3 -m venv $HOME/CLFSLoad < /dev/null
source $HOME/CLFSLoad/bin/activate
easy_install -U setuptools < /dev/null
pip install --upgrade pip
pip install lazy-object-proxy==1.4.1
python setup.py install
``` 

## Reporting problems

Report problems to averesupport@microsoft.com.

## Reporting security issues

Bugs or other issues affecting security should be reported privately, by email,
to the Microsoft Security Response Center (MSRC) at
[secure@microsoft.com](mailto:secure@microsoft.com). You should receive a
response within 24 hours. If for some reason you do not, please follow up by
email to ensure we received your original message. Further information,
including the [MSRC PGP
key](https://technet.microsoft.com/en-us/security/dn606155), can be found in the
[Security TechCenter](https://technet.microsoft.com/en-us/security/default).

## Microsoft open source code of conduct

This project has adopted the [Microsoft Open Source Code of
Conduct](https://opensource.microsoft.com/codeofconduct/).

For more information see the [Code of Conduct
FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact
[opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional
questions or comments.
