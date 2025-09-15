"""Local Enterprise Gateway Kernel Provisioner.

Provides local kernel provisioning with Enterprise Gateway features,
replacing LocalProcessProxy.
"""

from __future__ import annotations

import asyncio
import os
import re
import signal
import subprocess
from typing import Any, Dict, override

from jupyter_client.provisioning.local_provisioner import LocalProvisioner
from jupyter_client import KernelConnectionInfo, localinterfaces
from traitlets import List, Unicode

from .base import EnterpriseProvisionerBase


class LocalEnterpriseProvisioner(EnterpriseProvisionerBase, LocalProvisioner):
    """
    Local kernel provisioner with Enterprise Gateway features.
    
    This provisioner replaces LocalProcessProxy and provides:
    - Local kernel launching using subprocess.Popen
    - Enterprise Gateway session persistence
    - Authorization enforcement
    - Port range management
    - Process group tracking
    """
    
    def __init__(self, **kwargs):
        """Initialize the local Enterprise Gateway provisioner.""" 
        super().__init__(**kwargs)
