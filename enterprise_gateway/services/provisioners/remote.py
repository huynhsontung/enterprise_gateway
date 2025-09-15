"""Remote Enterprise Gateway Kernel Provisioner.

Provides remote kernel provisioning with Enterprise Gateway features,
replacing RemoteProcessProxy and its subclasses.
"""

from __future__ import annotations

import asyncio
import errno
import os
import signal
import socket
import json
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, override
from socket import SHUT_RDWR

from jupyter_client import KernelConnectionInfo

from .base import EnterpriseProvisionerBase
from ..processproxies.processproxy import KernelChannel, ResponseManager


socket_timeout = float(os.getenv("EG_SOCKET_TIMEOUT", "0.005"))

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
        self.assigned_ip = ""
        self.assigned_host = ""
        self.comm_ip = ""
        self.comm_port = 0
        self.tunneled_connect_info = None
        self.tunnel_processes = {}
        
        # ResponseManager integration
        self.response_manager = ResponseManager.instance()
        self.response_manager.register_event(self.kernel_id)
        if self.kernel_manager:
            self.kernel_manager.response_address = self.response_manager.response_address
            self.kernel_manager.public_key = self.response_manager.public_key
        
        # Set response address and public key for kernel launchers
        self.response_address = self.response_manager.response_address
        self.public_key = self.response_manager.public_key
        
        # Process tracking attributes
        self.pid = 0
        self.pgid = 0
        # Note: self.ip is a traitlet defined in EnterpriseGatewayConfigMixin with default value

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
        
        # Confirm remote startup
        startup_confirmed = await self.confirm_remote_startup()
        if not startup_confirmed:
            self.detect_launch_failure()
            raise RuntimeError("Failed to confirm remote kernel startup")
        
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
        self.log.debug(
            f"Host assigned to the kernel is: '{self.assigned_host}' '{self.assigned_ip}'"
        )
            
        # Set IP in connection info
        if self.assigned_ip:
            connection_info['ip'] = self.assigned_ip
        
        # Setup SSH tunneling if enabled
        tunneling_enabled = os.getenv('EG_ENABLE_TUNNELING', 'False').lower() == 'true'
        if tunneling_enabled and self.assigned_ip:
            await self._setup_ssh_tunneling(connection_info)
            
        # Setup communication port if available
        if 'comm_port' in connection_info:
            self.comm_ip = connection_info["ip"]
            self.comm_port = int(connection_info['comm_port'])
            self.log.debug(
                f"Established gateway communication to: {self.assigned_ip}:{self.comm_port} for KernelID '{self.kernel_id}'"
            )
        else:
            self.log.debug(
                f"Gateway communication port has NOT been established for KernelID '{self.kernel_id}' (optional)."
            )

        self._update_connection(connection_info)

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
        raise NotImplementedError("SSH tunneling setup not yet implemented in provisioner")

    def _update_connection(self, connection_info: KernelConnectionInfo) -> None:
        """
        Update connection information and notify kernel manager.
        
        Args:
            connection_info: Connection information to update
        """
        if not self.kernel_manager:
            self.log.warning("Kernel manager not available to update connection info")
            return
        
        # Reset the ports to 0 so load can take place (which resets the members to value from file or json)...
        self.kernel_manager.stdin_port = self.kernel_manager.iopub_port = (
            self.kernel_manager.shell_port
        ) = self.kernel_manager.hb_port = self.kernel_manager.control_port = 0

        if connection_info:
            # Extract process information
            self._extract_pid_info(connection_info=connection_info)
            self.kernel_manager.load_connection_info(info=connection_info)
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

        self.kernel_manager._connection_file_written = True  # allows for cleanup of local files (as necessary)
        
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
            await self._setup_connection_info(connect_info)
            return True
        except (asyncio.TimeoutError, TimeoutError):
            self.log.warning(f"Timeout waiting for KernelID '{self.kernel_id}' to send "
                             f"connection info from host '{self.assigned_host}' - retrying...")
        except Exception as e:
            error_message = (
                    f"Exception occurred waiting for connection file response for KernelId '{self.kernel_id}' "
                    f"on host '{self.assigned_host}': {e}"
                )
            await self.kill()
            self.log_and_raise(http_status_code=500, reason=error_message)

        return False
        
    def _send_listener_request(self, request: Dict[str, Any], shutdown_socket: bool = False) -> None:
        """
        Send request to kernel launcher listener. Caller is responsible for handling any exceptions.
        
        Args:
            request: Request to send
            shutdown_socket: Whether to shutdown socket after sending
        """
        if self.comm_port > 0 and self.comm_ip:
            self.log.debug(f"Sending request to {self.comm_ip}:{self.comm_port}: {request}")
            
            # Create socket and send request
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.settimeout(socket_timeout)
                sock.connect((self.comm_ip, self.comm_port))
                
                # Send the request (simplified implementation)
                message = json.dumps(request).encode()
                sock.sendall(message)
            finally:
                if shutdown_socket:
                    try:
                        sock.shutdown(SHUT_RDWR)
                    except Exception as e:
                        if isinstance(e, OSError) and e.errno == errno.ENOTCONN:
                            # Listener is not connected.  This is probably a follow-on to ECONNREFUSED on connect
                            self.log.debug(
                                f"OSError(ENOTCONN) raised on socket shutdown, listener "
                                f"has likely already exited. Cannot send '{request}'"
                            )
                        else:
                            self.log.warning(
                                f"Exception occurred attempting to shutdown communication "
                                f"socket to {self.comm_ip}:{self.comm_port} "
                                f"for KernelID '{self.kernel_id}' (ignored): {e!s}"
                            )
                sock.close()
        else:
            self.log.debug(f"Invalid comm port, not sending request '{request}'")

    def shutdown_listener(self):
        """
        Sends a shutdown request to the kernel launcher listener.
        """
        # If a comm port has been established, instruct the listener to shutdown so that proper
        # kernel termination can occur.  If not done, the listener keeps the launcher process
        # active, even after the kernel has terminated, leading to less than graceful terminations.

        if self.comm_port > 0:
            shutdown_request = {}
            shutdown_request["shutdown"] = 1

            try:
                self._send_listener_request(shutdown_request, shutdown_socket=True)
                self.log.debug("Shutdown request sent to listener via gateway communication port.")
            except Exception as e:
                if not isinstance(e, OSError) or e.errno != errno.ECONNREFUSED:
                    self.log.warning(
                        "An unexpected exception occurred sending listener shutdown to {}:{} for "
                        "KernelID '{}': {}".format(
                            self.comm_ip, self.comm_port, self.kernel_id, str(e)
                        )
                    )

            # Also terminate the tunnel process for the communication port - if in play.  Failure to terminate
            # this process results in the kernel (launcher) appearing to remain alive following the shutdown
            # request, which triggers the "forced kill" termination logic.

            comm_port_name = KernelChannel.COMMUNICATION.value
            comm_port_tunnel = self.tunnel_processes.get(comm_port_name, None)
            if comm_port_tunnel:
                self.log.debug(f"shutdown_listener: terminating {comm_port_name} tunnel process.")
                comm_port_tunnel.terminate()
                del self.tunnel_processes[comm_port_name]
        
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
        self.assigned_ip = str(provisioner_info.get('assigned_ip', ''))
        self.assigned_host = str(provisioner_info.get('assigned_host', ''))
        self.comm_ip = str(provisioner_info.get('comm_ip', ''))
        self.comm_port = int(provisioner_info.get('comm_port', 0))
        self.tunneled_connect_info = provisioner_info.get('tunneled_connect_info')
        self.pid = int(provisioner_info.get('pid', 0))
        self.pgid = int(provisioner_info.get('pgid', 0))
        self.ip = str(provisioner_info.get('ip', ''))

        # TODO: Re-establish tunneling if it was in use
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
        
        # Reset remote state
        self.assigned_ip = ""
        self.assigned_host = ""
        self.comm_ip = ""
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
                self._send_listener_request(request)
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
        return bool(self.assigned_ip)