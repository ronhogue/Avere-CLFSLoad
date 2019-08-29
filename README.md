CLFSLoad is a Python-based tool for ingesting data into Azure blob containers
and storing it in Microsoft's CLoud FileSystem (CLFS) format. That format is
used by HPCCaching and Avere vFXT.

CLFSLoad runs on a single Linux node. It transfers a single rooted subtree. It
assumes that the source tree does not change during the transfer. The
destination is an Azure Storage container. When the transfer is complete, the
destination container may be added to an HPCCaching or Avere vFXT cluster.

## Requirements
CLFSLoad requires Python version 3.6 or newer.

CLFSLoad offers better performance with Python 3.7 than it does with Python 3.6.

## Installation
To create a Python virtual environment and install CLFSLoad there:
```bash
python3 -m venv /usr/CLFSLoad < /dev/null
source /usr/CLFSLoad/bin/activate
easy_install -U setuptools < /dev/null
python setup.py install
```

This example creates /usr/CLFSLoad and installs CLFSLoad there. Any directory
may be used.

## Usage
To execute a new transfer:
```bash
CLFSLoad.py --new local_state_path source_root target_storage_account target_container sas_token
```

local_state_path is a directory on the local disk. CLFSLoad uses this to store
temporary state and logfiles. In the event that a transfer is interrupted,
such as by a system failure or reboot, it may be resumed using the contents
of local_state_path by re-executing the above command without the "--new" option.

source_root is the path to subtree to transfer. This may be on a filesystem
local to the node, or it may be remote, such as an NFS mount.

target_storage_account is the name of the Azure storage account to which
source_root shall be copied. This must exist before running CLFSLoad.

target_container is the name of the Azure container within
target_storage_account to which source_root shall be copied. This must be
created before running CLFSLoad. A new transfer expects the container to be
empty. A resumed transfer expects to find the contents it previously wrote.

sas_token is a secure access signature for accessing target_container.
For more information, see:
[Using shared access signatures (SAS)](https://docs.microsoft.com/en-us/azure/storage/common/storage-dotnet-shared-access-signature-part-1)
Your shell will most likely require you to quote or otherwise escape the
sas_token.

### Resuming an interrupted transfer
If a transfer is interrupted, it need not be restarted from the beginning.
Instead, it may be resumed. The command to resume a transfer is the same
as the command used to start it, but without the --new argument.
```bash
CLFSLoad.py local_state_path source_root target_storage_account target_container sas_token
```

### Running on Azure virtual machines

The recommended VM size is Standard_F16s_v2.
For best performance, place your local state directory in /mnt.

If you are using a Standard_L series VM size, you may use one of the NVME
devices as a local filesystem and place your local state directory there. This
gives more capacity but not typically more performance than using /mnt.

### Attaching the container to an Avere vFXT cluster or HPC Cache

The first time a container is attached to an Avere vFXT cluster or HPC Cache,
the system may require a few minutes for internal configuration before the /
export for the storage target appears and is ready to be added as a
junction. This is normal and not a cause for alarm.

## Troubleshooting

### fatal error: Python.h: No such file or directory

If you get an error like this:
```fatal error: Python.h: No such file or directory
#include <Python.h>
          ^~~~~~~~~~
compilation terminated.
```

then your Python installation is incomplete. You must install the development
package and venv support.

apt (Ubuntu et al)
```sudo apt-get update
sudo apt-get --assume-yes install python3-dev
sudo apt-get --assume-yes install python3-venv
```

You may need to consult your distribution's documentation to determine the
best way to install Python3.

## Microsoft Open Source Code of Conduct
This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).

For more information see the [Code of Conduct
FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact
[opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional
questions or comments.

## Reporting Problems

Report problems to averesupport@microsoft.com.

## Reporting Security Issues

Security issues and bugs should be reported privately, via email, to the
Microsoft Security Response Center (MSRC) at
[secure@microsoft.com](mailto:secure@microsoft.com). You should receive a
response within 24 hours. If for some reason you do not, please follow up via
email to ensure we received your original message. Further information,
including the [MSRC PGP] (https://technet.microsoft.com/en-us/security/dn606155)
key, can be found in the [Security TechCenter]
(https://technet.microsoft.com/en-us/security/default).

