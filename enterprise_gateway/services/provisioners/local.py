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

    # Trait for specifying local IPs that should not be included when determining the response address
    prohibited_local_ips = List(
        config=True,
        help="""List of local IP patterns (regular expressions) that should not be included
        when determining the response address. For example, on systems with many network interfaces,
        some may have their IPs appear in the local interfaces list (e.g., docker's 172.17.0.* is an example)
        that should not be used. (EG_PROHIBITED_LOCAL_IPS env var)"""
    )

    def _prohibited_local_ips_default(self):
        """Default value for prohibited_local_ips from environment variable."""
        prohibited_ips_str = os.getenv("EG_PROHIBITED_LOCAL_IPS", "")
        if prohibited_ips_str:
            return [ip.strip() for ip in prohibited_ips_str.split(",") if ip.strip()]
        return []
    
    def __init__(self, **kwargs):
        """Initialize the local Enterprise Gateway provisioner.""" 
        super().__init__(**kwargs)
        
        # Set IP to localhost for local kernels
        self.ip = self._get_local_ip()
        
        # Local process tracking
        self.pgid = 0

    def _get_local_ip(self) -> str:
        """
        Honor the prohibited IPs, locating the first not in the list.
            
        Returns:
            First public IP not matching any prohibited pattern
        """
        for ip in localinterfaces.public_ips():
            is_prohibited = False
            for prohibited_ip in self.prohibited_local_ips:  # exhaust prohibited list, applying regexs
                if prohibited_ip and re.match(prohibited_ip, ip):
                    is_prohibited = True
                    break
            if not is_prohibited:
                return ip
        return localinterfaces.public_ips()[0]  # all were prohibited, so go with the first
        
    @override
    def detect_launch_failure(self) -> None:
        """
        Detect if local kernel launch has failed.
        
        Checks the local process for failure conditions.
        """
        if hasattr(self, 'process') and self.process:
            poll_result = self.process.poll()
            if poll_result and poll_result > 0:
                # Process exited with error code
                try:
                    self.process.wait()
                except subprocess.TimeoutExpired:
                    pass
                
                error_message = (
                    f"Local kernel launch failed for KernelID: {self.kernel_id} "
                    f"with exit code: {poll_result}. Check Enterprise Gateway log for more information."
                )
                self.log_and_raise(http_status_code=500, reason=error_message)
        
    @override
    async def launch_kernel(self, cmd: list[str], **kwargs) -> KernelConnectionInfo:
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
        
        # Set start time for timeout tracking
        self.start_time = self.get_current_time()
        
        # Use LocalProvisioner's launch mechanism
        connection_info = await super().launch_kernel(cmd, **kwargs)
        
        # Add Enterprise Gateway specific information
        if self.process:
            self.log.info(
                f"Local kernel launched with PID: {self.process.pid}, "
                f"Kernel ID: {self.kernel_id}"
            )
            
            # Track process group if available
            if hasattr(os, "getpgid"):
                try:
                    self.pgid = os.getpgid(self.process.pid)  # type: ignore
                    self.log.debug(f"Process group ID: {self.pgid}")
                except OSError:
                    pass
                    
            # Check for immediate launch failure
            self.detect_launch_failure()
                    
        return connection_info
        
    @override
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
                'pgid': self.pgid,
                'process_info': {
                    'pid': self.process.pid,
                    'pgid': self.pgid,
                    'ip': self.ip,
                }
            })
                    
        return info
        
    @override
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
        if 'pgid' in provisioner_info:
            self.pgid = provisioner_info['pgid']
            
        # Note: We cannot restore the actual process object from persistence
        # This is expected for session recovery scenarios
        if 'process_info' in provisioner_info:
            process_info = provisioner_info['process_info']
            self.log.debug(f"Loaded process info: {process_info}")
            # Restore pgid if available
            if 'pgid' in process_info:
                self.pgid = process_info['pgid']
            
    @override
    async def cleanup(self, restart: bool = False) -> None:
        """
        Clean up local kernel resources.
        
        Args:
            restart: Whether this cleanup is for a restart operation
        """
        self.log.debug(f"Cleaning up local kernel (restart={restart})")
        
        # Cleanup process group if we have one
        if self.pgid > 0 and hasattr(os, 'killpg'):
            try:
                self.log.debug(f"Cleaning up process group {self.pgid}")
                # Send SIGTERM to process group (Unix only)
                os.killpg(self.pgid, signal.SIGTERM)  # type: ignore
                # Give processes time to terminate gracefully
                await asyncio.sleep(0.5)
                # Send SIGKILL if still running
                try:
                    if hasattr(signal, 'SIGKILL'):
                        os.killpg(self.pgid, signal.SIGKILL)  # type: ignore
                except ProcessLookupError:
                    # Process group already terminated
                    pass
            except (OSError, ProcessLookupError) as e:
                self.log.debug(f"Process group cleanup error (normal): {e}")
        
        # Call parent cleanup
        await super().cleanup(restart)
        
        # Reset local state
        self.pgid = 0
        
    @override
    async def send_signal(self, signum: int) -> None:
        """
        Send signal to local kernel process.
        
        Args:
            signum: Signal number to send
        """
        self.log.debug(f"Sending signal {signum} to local kernel")
        
        if hasattr(self, 'process') and self.process:
            try:
                # Try to send signal to the process
                self.process.send_signal(signum)
                self.log.debug(f"Signal {signum} sent to PID {self.process.pid}")
            except (OSError, ProcessLookupError) as e:
                self.log.debug(f"Could not send signal to process: {e}")
        elif self.pgid > 0 and hasattr(os, 'killpg'):
            try:
                # Fall back to process group if available (Unix only)
                os.killpg(self.pgid, signum)  # type: ignore
                self.log.debug(f"Signal {signum} sent to process group {self.pgid}")
            except (OSError, ProcessLookupError) as e:
                self.log.debug(f"Could not send signal to process group: {e}")
        else:
            self.log.warning(f"No process or process group available to send signal {signum}")
            
    @property
    @override
    def has_process(self) -> bool:
        """
        Check if provisioner is managing a process.
        
        Returns:
            True if managing a process, False otherwise
        """
        if hasattr(self, 'process') and self.process:
            return self.process.poll() is None
        return False