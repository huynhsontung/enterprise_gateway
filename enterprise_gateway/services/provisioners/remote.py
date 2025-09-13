"""Remote Enterprise Gateway Kernel Provisioner.

Provides remote kernel provisioning with Enterprise Gateway features,
replacing RemoteProcessProxy and its subclasses.
"""

from __future__ import annotations

import asyncio
import os
import signal
import socket
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, override
from socket import SHUT_RDWR

from jupyter_client import KernelConnectionInfo

from .base import EnterpriseProvisionerBase
from ..processproxies.processproxy import ResponseManager


class RemoteEnterpriseProvisioner(EnterpriseProvisionerBase, ABC):
    """
    Remote kernel provisioner for distributed environments.
    
    This provisioner replaces RemoteProcessProxy and provides:
    - Remote kernel launching and management
    - SSH tunneling support
    - Response socket management for remote communication
    - Enterprise Gateway session persistence
    - Authorization enforcement for remote users
    - Distributed process lifecycle management
    
    This is an abstract base class that must be subclassed for specific
    remote environments (YARN, Kubernetes, Docker, etc.).
    """
    
    def __init__(self, **kwargs):
        """Initialize the remote Enterprise Gateway provisioner."""
        super().__init__(**kwargs)
        
        # Remote-specific attributes
        self.assigned_ip = None
        self.assigned_host = ""
        self.comm_ip = None
        self.comm_port = 0
        self.tunneled_connect_info = None
        self.tunnel_processes = {}
        self.response_socket = None
        
        # ResponseManager integration
        self.response_manager = ResponseManager.instance()
        self.response_manager.register_event(self.kernel_id)
        
        # Set response address and public key for kernel launchers
        self.response_address = self.response_manager.response_address
        self.public_key = self.response_manager.public_key
        
        # Process tracking attributes
        self.pid = 0
        self.pgid = 0
        self.ip = None
        
        # Initialize response management for remote communication
        self._setup_response_management()

    @override
    async def pre_launch(self, **kwargs) -> Dict[str, Any]:
        """
        Setup remote kernel launch environment.
        
        Adds ResponseManager environment variables for remote kernel communication.
        
        Args:
            **kwargs: Launch arguments
            
        Returns:
            Updated launch arguments
        """
        # Call parent pre_launch first
        kwargs = await super().pre_launch(**kwargs)
        
        # Add ResponseManager environment variables for remote kernel launcher
        if 'env' not in kwargs:
            kwargs['env'] = {}
            
        # ResponseManager communication parameters
        kwargs['env']['EG_RESPONSE_ADDRESS'] = self.response_address
        kwargs['env']['EG_PUBLIC_KEY'] = self.public_key
        kwargs['env']['EG_KERNEL_ID'] = self.kernel_id
        
        self.log.debug(f"Added ResponseManager env vars - address: {self.response_address}")
        
        return kwargs
        
    def _setup_response_management(self) -> None:
        """Setup response socket management for remote communication."""
        # ResponseManager is already initialized in __init__
        # This method is now used for any additional setup if needed
        self.log.debug(f"Response management setup complete for kernel {self.kernel_id}")
        self.log.debug(f"Response address: {self.response_address}")
        
    def _close_response_socket(self) -> None:
        """Close the response socket if it exists."""
        if self.response_socket:
            try:
                self.log.debug("Response socket still open, closing it")
                self.response_socket.shutdown(SHUT_RDWR)
                self.response_socket.close()
            except OSError:
                # Tolerate exceptions since we don't need this socket and want to continue
                pass
            self.response_socket = None
            
    def _extract_pid_info(self, connection_info: KernelConnectionInfo) -> None:
        """
        Extract PID and PGID information from connection info.
        
        Args:
            connection_info: Connection information from remote kernel
        """
        # Extract process information if available
        if 'pid' in connection_info:
            self.pid = int(connection_info['pid'])
            self.log.debug(f"Extracted PID: {self.pid}")
            
        if 'pgid' in connection_info:
            self.pgid = int(connection_info['pgid'])
            self.log.debug(f"Extracted PGID: {self.pgid}")
            
        # Update IP if we have process information and it's remote
        if (self.pid or self.pgid) and self.assigned_ip:
            self.ip = self.assigned_ip
        
    @abstractmethod
    async def confirm_remote_startup(self) -> bool:
        """
        Confirm the remote kernel has started successfully.
        
        This method must be implemented by subclasses to provide
        environment-specific startup confirmation logic.
        
        Returns:
            True if startup confirmed, False otherwise
        """
        pass
        
    @override
    async def launch_kernel(self, cmd: list[str], **kwargs) -> KernelConnectionInfo:
        """
        Launch a remote kernel process.
        
        Args:
            cmd: Command to launch the kernel
            **kwargs: Additional launch parameters
            
        Returns:
            Connection information dictionary
        """
        self.log.info(f"Launching remote kernel with command: {cmd}")
        
        # Set start time for timeout tracking
        self.start_time = self.get_current_time()
        
        # Call parent pre_launch for Enterprise Gateway setup
        kwargs = await self.pre_launch(**kwargs)
        
        # Launch the remote process (implemented by subclasses)
        connection_info = await self._launch_remote_process(cmd, **kwargs)
        
        # Wait for connection info from ResponseManager if this is a remote launch
        # The kernel launcher will send connection details back via ResponseManager
        if not connection_info:
            self.log.debug("No immediate connection info, waiting for ResponseManager...")
            success = await self.receive_connection_info()
            if not success:
                self.detect_launch_failure()
                raise RuntimeError("Failed to receive connection info from remote kernel launcher")
            # After receive_connection_info, connection_info is stored in self.connection_info
            connection_info = getattr(self, 'connection_info', {})
        
        # Extract process information from connection info
        self._extract_pid_info(connection_info)
        
        # Confirm remote startup
        startup_confirmed = await self.confirm_remote_startup()
        if not startup_confirmed:
            self.detect_launch_failure()
            raise RuntimeError("Failed to confirm remote kernel startup")
            
        # Setup connection info and tunneling if needed
        await self._setup_connection_info(connection_info)
        
        self.log.info(
            f"Remote kernel launched successfully. "
            f"Assigned to host: {self.assigned_host}, IP: {self.assigned_ip}"
        )
        
        return connection_info
        
    @abstractmethod 
    async def _launch_remote_process(self, cmd: list[str], **kwargs) -> KernelConnectionInfo:
        """
        Launch the actual remote process.
        
        This method must be implemented by subclasses to provide
        environment-specific launch logic.
        
        Args:
            cmd: Command to launch
            **kwargs: Launch parameters
            
        Returns:
            Initial connection information
        """
        pass
        
    async def _setup_connection_info(self, connection_info: KernelConnectionInfo) -> None:
        """
        Setup connection information and tunneling if configured.
        
        Args:
            connection_info: Connection information from remote launch
        """
        # Extract host/IP information
        if 'assigned_host' in connection_info:
            self.assigned_host = str(connection_info['assigned_host'])
        if 'assigned_ip' in connection_info:
            self.assigned_ip = str(connection_info['assigned_ip'])
            
        # Set IP in connection info
        if self.assigned_ip:
            connection_info['ip'] = self.assigned_ip
        
        # Setup SSH tunneling if enabled
        tunneling_enabled = os.getenv('EG_ENABLE_TUNNELING', 'False').lower() == 'true'
        if tunneling_enabled and self.assigned_ip:
            await self._setup_ssh_tunneling(connection_info)
            
        # Setup communication port if available
        if 'comm_port' in connection_info:
            self.comm_port = int(connection_info['comm_port'])

    async def _setup_ssh_tunneling(self, connection_info: KernelConnectionInfo) -> None:
        """
        Setup SSH tunneling for remote kernel connections.
        
        Args:
            connection_info: Connection information to tunnel
        """
        self.log.debug("Setting up SSH tunneling for remote kernel")
        
        # Store original connection info
        self.tunneled_connect_info = dict(connection_info)
        
        # This will implement SSH tunneling logic
        # Similar to _tunnel_to_kernel in RemoteProcessProxy
        # For now, we'll log that tunneling setup is needed
        self.log.warning("SSH tunneling setup not yet implemented in provisioner")
        
    def _update_connection(self, connection_info: KernelConnectionInfo) -> None:
        """
        Update connection information and notify kernel manager.
        
        Args:
            connection_info: Connection information to update
        """
        if connection_info:
            # Extract communication port if present
            if 'comm_port' in connection_info:
                self.comm_port = int(connection_info['comm_port'])
                self.log.debug(f"Updated comm_port: {self.comm_port}")
            
            # Extract assigned host/IP if present  
            if 'assigned_host' in connection_info:
                self.assigned_host = str(connection_info['assigned_host'])
                self.log.debug(f"Updated assigned_host: {self.assigned_host}")
                
            if 'assigned_ip' in connection_info:
                self.assigned_ip = str(connection_info['assigned_ip'])
                self.log.debug(f"Updated assigned_ip: {self.assigned_ip}")
                
            # Set comm_ip to assigned_ip if not already set
            if self.assigned_ip and not self.comm_ip:
                self.comm_ip = self.assigned_ip
            
            # Extract process information
            self._extract_pid_info(connection_info)
            
            # Store the connection info for the provisioner
            self.connection_info = connection_info
            
            # Update kernel manager with connection info
            # TODO: Integrate with kernel manager's load_connection_info
            self.log.debug(
                f"Received connection info for KernelID '{self.kernel_id}' "
                f"from host '{self.assigned_host}': {connection_info}..."
            )
            
            self.log.info(f"Connection information updated for kernel {self.kernel_id}")
        else:
            error_message = (
                f"Unexpected runtime encountered for Kernel ID '{self.kernel_id}' - "
                "connection information is null!"
            )
            self.log_and_raise(http_status_code=500, reason=error_message)
            
        # Close response socket as it's no longer needed
        self._close_response_socket()
        
    async def receive_connection_info(self) -> bool:
        """
        Monitor response address for connection info from remote kernel launcher.
        
        Returns:
            True if connection info received successfully
        """
        try:
            self.log.debug(f"Waiting for connection info for kernel {self.kernel_id}")
            connect_info = await self.response_manager.get_connection_info(self.kernel_id)
            self.log.debug(f"Received connection info: {connect_info}")
            self._update_connection(connect_info)
            return True
        except Exception as e:
            self.log.error(f"Failed to receive connection info for kernel {self.kernel_id}: {e}")
            return False
        
    def detect_launch_failure(self) -> None:
        """
        Detect if remote kernel launch has failed.
        """
        # TODO: Implement remote launch failure detection
        # This might check for:
        # - Process exit codes
        # - Connection timeouts  
        # - Response socket errors
        self.log.warning("Remote launch failure detection not yet implemented")
        
    async def _send_listener_request(self, request: Dict[str, Any], shutdown_socket: bool = False) -> None:
        """
        Send request to kernel launcher listener.
        
        Args:
            request: Request to send
            shutdown_socket: Whether to shutdown socket after sending
        """
        if self.comm_port > 0 and self.comm_ip:
            try:
                self.log.debug(f"Sending request to {self.comm_ip}:{self.comm_port}: {request}")
                
                # Create socket and send request
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                try:
                    sock.connect((self.comm_ip, self.comm_port))
                    
                    # Send the request (simplified implementation)
                    import json
                    message = json.dumps(request).encode()
                    sock.sendall(message)
                    
                    if shutdown_socket:
                        sock.shutdown(SHUT_RDWR)
                        
                finally:
                    sock.close()
                    
            except Exception as e:
                self.log.warning(f"Exception sending request to listener: {e}")
        else:
            self.log.debug(f"Invalid comm port, not sending request '{request}'")
        
    @override
    async def get_provisioner_info(self) -> Dict[str, Any]:
        """
        Capture provisioner information for session persistence.
        
        Returns:
            Dictionary containing provisioner state for persistence
        """
        info = await super().get_provisioner_info()
        
        # Add remote-specific information
        info.update({
            'provisioner_class': self.__class__.__name__,
            'assigned_ip': self.assigned_ip,
            'assigned_host': self.assigned_host,
            'comm_ip': self.comm_ip,
            'comm_port': self.comm_port,
            'tunneled_connect_info': self.tunneled_connect_info,
            'pid': self.pid,
            'pgid': self.pgid,
            'ip': self.ip,
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
        
        # Restore remote-specific state
        self.assigned_ip = provisioner_info.get('assigned_ip')
        self.assigned_host = provisioner_info.get('assigned_host', '')
        self.comm_ip = provisioner_info.get('comm_ip')
        self.comm_port = provisioner_info.get('comm_port', 0)
        self.tunneled_connect_info = provisioner_info.get('tunneled_connect_info')
        self.pid = provisioner_info.get('pid', 0)
        self.pgid = provisioner_info.get('pgid', 0)
        self.ip = provisioner_info.get('ip')
        
        # Re-establish tunneling if it was in use
        if self.tunneled_connect_info:
            self.log.debug("Re-establishing SSH tunneling from session persistence")
            # This would re-establish tunnels
            
    @override
    async def cleanup(self, restart: bool = False) -> None:
        """
        Clean up remote kernel resources.
        
        Args:
            restart: Whether this cleanup is for a restart operation
        """
        self.log.debug(f"Cleaning up remote kernel (restart={restart})")
        
        # Cleanup tunnels
        await self._cleanup_tunnels()
        
        # Close response socket
        self._close_response_socket()
        
        # Reset remote state
        self.assigned_ip = None
        self.assigned_host = ""
        self.comm_ip = None
        self.comm_port = 0
        self.tunneled_connect_info = None
        
        # Call parent cleanup
        await super().cleanup(restart)
        
    async def _cleanup_tunnels(self) -> None:
        """Clean up SSH tunnels."""
        for channel, process in self.tunnel_processes.items():
            self.log.debug(f"Terminating {channel} tunnel process")
            try:
                process.terminate()
                # Give process time to terminate gracefully
                await asyncio.sleep(0.1)
                if process.poll() is None:
                    process.kill()
            except Exception as e:
                self.log.warning(f"Error terminating tunnel process: {e}")
                
        self.tunnel_processes.clear()
        
    @override
    async def send_signal(self, signum: int) -> None:
        """
        Send signal to remote kernel process.
        
        Args:
            signum: Signal number to send
        """
        self.log.debug(f"Sending signal {signum} to remote kernel")
        
        # Try to send signal via communication socket first
        if self.comm_port > 0:
            try:
                request = {
                    'action': 'signal',
                    'signal': signum,
                    'kernel_id': self.kernel_id
                }
                await self._send_listener_request(request)
                return
            except Exception as e:
                if isinstance(e, ConnectionRefusedError):
                    self.log.debug("Connection refused, no process listening, cannot send signal.")
                    return
                else:
                    self.log.warning(f"Unexpected exception sending signal ({signum}): {e}")
        
        # Fall back to parent implementation
        await super().send_signal(signum)
        
    @override
    async def poll(self) -> Optional[int]:
        """
        Poll remote kernel process for termination status.
        
        Returns:
            None if process is still running, exit code if terminated
        """
        # This will be implemented by subclasses to handle
        # environment-specific process polling
        self.log.debug("Remote process polling - placeholder implementation")
        return None
        
    @override
    async def wait(self) -> Optional[int]:
        """
        Wait for remote kernel process to terminate.
        
        Returns:
            Exit code when process terminates
        """
        # This will be implemented by subclasses to handle
        # environment-specific process waiting
        self.log.debug("Remote process waiting - placeholder implementation")
        return 0
        
    @override
    async def terminate(self, restart: bool = False) -> None:
        """
        Terminate remote kernel process.
        
        Args:
            restart: Whether this termination is for a restart operation
        """
        self.log.debug(f"Terminating remote kernel (restart={restart})")
        
        # Send SIGTERM signal first
        try:
            await self.send_signal(signal.SIGTERM)
        except Exception as e:
            self.log.debug(f"Error sending SIGTERM: {e}")
            
        # Give process time to terminate gracefully
        await asyncio.sleep(1.0)
        
    @override
    async def kill(self, restart: bool = False) -> None:
        """
        Forcefully kill remote kernel process.
        
        Args:
            restart: Whether this kill is for a restart operation
        """
        self.log.debug(f"Killing remote kernel (restart={restart})")
        
        # Send SIGKILL signal
        try:
            # Use SIGKILL if available (Unix), otherwise SIGTERM (Windows)
            kill_signal = getattr(signal, 'SIGKILL', signal.SIGTERM)
            await self.send_signal(kill_signal)
        except Exception as e:
            self.log.debug(f"Error sending kill signal: {e}")
        
    @property
    @override
    def has_process(self) -> bool:
        """
        Check if provisioner is managing a process.
        
        Returns:
            True if managing a process, False otherwise
        """
        # This should be implemented based on remote process state
        return self.assigned_ip is not None