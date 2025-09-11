"""Local Enterprise Gateway Kernel Provisioner.

Provides local kernel provisioning with Enterprise Gateway features,
replacing LocalProcessProxy.
"""

from __future__ import annotations

import os
import subprocess
from typing import Any, Dict, Optional, Union

from jupyter_client.provisioning.local_provisioner import LocalProvisioner
from jupyter_client import localinterfaces

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
        
        # Set IP to localhost for local kernels
        self.ip = localinterfaces.LOCALHOST
        
    async def launch_kernel(self, cmd: list[str], **kwargs) -> Dict[str, Union[int, str, bytes]]:
        """
        Launch a local kernel process.
        
        Args:
            cmd: Command to launch the kernel
            **kwargs: Additional launch parameters
            
        Returns:
            Connection information dictionary
        """
        self.log.info(f"Launching local kernel with command: {cmd}")
        
        # Call parent pre_launch for Enterprise Gateway setup
        kwargs = await self.pre_launch(**kwargs)
        
        # Use LocalProvisioner's launch mechanism
        connection_info = await super().launch_kernel(cmd, **kwargs)
        
        # Add Enterprise Gateway specific information
        if hasattr(self, 'process') and self.process:
            self.log.info(
                f"Local kernel launched with PID: {self.process.pid}, "
                f"Kernel ID: {self.kernel_id}"
            )
            
            # Track process group if available
            if hasattr(os, "getpgid"):
                try:
                    pgid = os.getpgid(self.process.pid)  # type: ignore
                    self.log.debug(f"Process group ID: {pgid}")
                except OSError:
                    pass
                    
        return connection_info
        
    async def get_provisioner_info(self) -> Dict[str, Any]:
        """
        Capture provisioner information for session persistence.
        
        Returns:
            Dictionary containing provisioner state for persistence
        """
        info = await super().get_provisioner_info()
        
        # Add local-specific information
        info.update({
            'provisioner_class': 'LocalEnterpriseProvisioner',
            'ip': self.ip,
        })
        
        # Add process information if available
        if hasattr(self, 'process') and self.process:
            info.update({
                'pid': self.process.pid,
                'process_info': {
                    'pid': self.process.pid,
                    'ip': self.ip,
                }
            })
            
            # Add process group if available
            if hasattr(os, "getpgid"):
                try:
                    info['process_info']['pgid'] = os.getpgid(self.process.pid)  # type: ignore
                except OSError:
                    pass
                    
        return info
        
    async def load_provisioner_info(self, provisioner_info: Dict[str, Any]) -> None:
        """
        Load provisioner information from session persistence.
        
        Args:
            provisioner_info: Dictionary containing persisted provisioner state
        """
        await super().load_provisioner_info(provisioner_info)
        
        # Restore local-specific state
        if 'ip' in provisioner_info:
            self.ip = provisioner_info['ip']
            
        # Note: We cannot restore the actual process object from persistence
        # This is expected for session recovery scenarios
        if 'process_info' in provisioner_info:
            process_info = provisioner_info['process_info']
            self.log.debug(f"Loaded process info: {process_info}")
            
    async def cleanup(self, restart: bool = False) -> None:
        """
        Clean up local kernel resources.
        
        Args:
            restart: Whether this cleanup is for a restart operation
        """
        self.log.debug(f"Cleaning up local kernel (restart={restart})")
        
        # Call parent cleanup
        await super().cleanup(restart)
        
        # Additional Enterprise Gateway cleanup if needed
        # (Most cleanup is handled by LocalProvisioner)