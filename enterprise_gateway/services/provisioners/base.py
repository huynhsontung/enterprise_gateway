"""Base Enterprise Gateway Kernel Provisioner.

Provides common functionality for all Enterprise Gateway provisioners,
including session persistence, authorization, and port management.
"""

from __future__ import annotations

import asyncio
import os
import signal
import socket
from typing import Any, Dict, Optional, Union
from abc import ABC, abstractmethod

from jupyter_client.provisioning.provisioner_base import KernelProvisionerBase
from traitlets import Bool, Float, Int, Set, Unicode

from enterprise_gateway.mixins import EnterpriseGatewayConfigMixin


class EnterpriseProvisionerBase(KernelProvisionerBase, EnterpriseGatewayConfigMixin):
    """
    Base class for Enterprise Gateway kernel provisioners.
    
    Provides common functionality that was previously in BaseProcessProxyABC:
    - Session persistence (get/load_provisioner_info)
    - Authorization enforcement
    - Port range validation
    - Process information capture
    - Enterprise Gateway configuration integration
    """
    
    # Traitlets for Enterprise Gateway configuration
    authorized_users = Set(
        config=True,
        help="""Set of users allowed to launch kernels with this provisioner."""
    )
    
    unauthorized_users = Set(
        config=True, 
        help="""Set of users denied access to launch kernels with this provisioner."""
    )
    
    port_range = Unicode(
        config=True,
        help="""Port range to use for kernel ports (e.g., '10000:10100')."""
    )
    
    impersonation_enabled = Bool(
        default_value=False,
        config=True,
        help="""Whether to enable user impersonation for kernel processes."""
    )
    
    kernel_launch_timeout = Float(
        default_value=40.0,
        config=True,
        help="""Time in seconds to wait for kernel to start."""
    )
    
    def __init__(self, **kwargs):
        """Initialize the Enterprise Gateway provisioner."""
        super().__init__(**kwargs)
        
        # Initialize port range
        self.lower_port = 0
        self.upper_port = 0
        self._validate_port_range()
        
        # Initialize authorization
        self._setup_authorization()
        
        # Response management for remote communication
        self.response_address = None
        self.public_key = None
        
        # Timeout and error handling
        self.start_time = None
        self._setup_response_management()
        
    def _validate_port_range(self) -> None:
        """Validate and parse the port range configuration."""
        if self.port_range:
            try:
                port_ranges = self.port_range.split(':')
                self.lower_port = int(port_ranges[0])
                self.upper_port = int(port_ranges[1])
                
                if self.lower_port <= 0 or self.upper_port <= 0 or self.lower_port >= self.upper_port:
                    raise ValueError("Invalid port range")
                    
                self.log.debug(f"Port range configured: {self.lower_port}:{self.upper_port}")
            except (ValueError, IndexError):
                self.log.warning(f"Invalid port range format: {self.port_range}")
                self.lower_port = 0
                self.upper_port = 0
    
    def _setup_authorization(self) -> None:
        """Setup authorization from kernel manager and proxy config."""
        # This will be implemented to integrate with kernel manager authorization
        pass
        
    def _setup_response_management(self) -> None:
        """Setup response management for remote communication."""
        # Initialize response management
        # This will be enhanced when ResponseManager is available
        pass
        
    def _enforce_authorization(self, **kwargs) -> None:
        """Enforce authorization before kernel launch."""
        # Extract username from launch kwargs
        username = kwargs.get('env', {}).get('KERNEL_USERNAME', 'anonymous')
        
        # Check unauthorized users first
        if username in self.unauthorized_users:
            raise PermissionError(f"User '{username}' is not authorized to launch kernels")
            
        # If authorized users is configured, check membership
        if self.authorized_users and username not in self.authorized_users:
            raise PermissionError(f"User '{username}' is not in the authorized users list")
            
    def select_ports(self, count: int) -> list[int]:
        """
        Select available ports for kernel communication.
        
        Args:
            count: Number of ports needed
            
        Returns:
            List of available port numbers
        """
        import socket
        
        ports = []
        if self.lower_port > 0 and self.upper_port > 0:
            # Use configured port range
            for port in range(self.lower_port, self.upper_port + 1):
                if len(ports) >= count:
                    break
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.bind(('', port))
                    sock.close()
                    ports.append(port)
                except OSError:
                    continue
        else:
            # Use system-assigned ports
            for _ in range(count):
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.bind(('', 0))
                port = sock.getsockname()[1]
                sock.close()
                ports.append(port)
                
        if len(ports) < count:
            raise RuntimeError(f"Could not allocate {count} ports")
            
        return ports
        
    async def get_provisioner_info(self) -> Dict[str, Any]:
        """
        Capture provisioner information for session persistence.
        
        Returns:
            Dictionary containing provisioner state for persistence
        """
        info = await super().get_provisioner_info()
        info.update({
            'enterprise_gateway_version': '3.0.0',  # Will be dynamic
            'provisioner_type': self.__class__.__name__,
            'kernel_id': self.kernel_id,
            'authorized_users': list(self.authorized_users),
            'port_range': self.port_range,
        })
        return info
        
    async def load_provisioner_info(self, provisioner_info: Dict[str, Any]) -> None:
        """
        Load provisioner information from session persistence.
        
        Args:
            provisioner_info: Dictionary containing persisted provisioner state
        """
        await super().load_provisioner_info(provisioner_info)
        
        # Restore Enterprise Gateway specific state
        if 'authorized_users' in provisioner_info:
            self.authorized_users = set(provisioner_info['authorized_users'])
        if 'port_range' in provisioner_info:
            self.port_range = provisioner_info['port_range']
            self._validate_port_range()
            
    async def pre_launch(self, **kwargs) -> Dict[str, Any]:
        """
        Perform pre-launch validation and setup.
        
        Returns:
            Updated kwargs for launch_kernel
        """
        kwargs = await super().pre_launch(**kwargs)
        
        # Enforce authorization
        self._enforce_authorization(**kwargs)
        
        # Add Enterprise Gateway specific environment variables
        env = kwargs.get('env', {})
        env['KERNEL_ID'] = self.kernel_id
        
        # Add kernel language if available
        if hasattr(self.kernel_spec, 'language'):
            env.setdefault('KERNEL_LANGUAGE', self.kernel_spec.language.lower())
            
        kwargs['env'] = env
        return kwargs
        
    async def handle_timeout(self) -> None:
        """
        Check if kernel launch timeout has been exceeded.
        
        This method should be called periodically during kernel launch
        to detect timeout conditions.
        """
        if self.start_time is None:
            return
            
        current_time = self.get_current_time()
        time_interval = self.get_time_diff(self.start_time, current_time)
        
        if time_interval > self.kernel_launch_timeout:
            error_message = (
                f"Kernel launch timeout exceeded ({self.kernel_launch_timeout}s) "
                f"for KernelID '{self.kernel_id}'"
            )
            # Kill the kernel process
            await self.kill(restart=False)
            raise TimeoutError(error_message)
            
    def detect_launch_failure(self) -> None:
        """
        Detect if kernel launch has failed.
        
        This method should be called to check for launch failures
        and provide appropriate error messages.
        """
        # This will be implemented by subclasses with specific launch failure detection
        pass
        
    @staticmethod
    def get_current_time() -> float:
        """Return the current time stamp in UTC time epoch format in milliseconds."""
        import time
        return time.time() * 1000
        
    @staticmethod
    def get_time_diff(time1: float, time2: float) -> float:
        """Return the difference between two timestamps in seconds."""
        return abs(time2 - time1) / 1000.0
        
    def log_and_raise(self, http_status_code: Optional[int] = None, reason: Optional[str] = None) -> None:
        """
        Log an error message and raise an appropriate exception.
        
        Args:
            http_status_code: HTTP status code for the error
            reason: Error message
        """
        error_message = reason or "Unknown error occurred"
        self.log.error(error_message)
        
        if http_status_code == 403:
            raise PermissionError(error_message)
        elif http_status_code == 500:
            raise RuntimeError(error_message)
        else:
            raise Exception(error_message)