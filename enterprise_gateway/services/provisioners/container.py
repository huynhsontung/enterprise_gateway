"""Container Enterprise Gateway Kernel Provisioner.

This module provides container-specific kernel provisioning functionality,
serving as a base class for container-based provisioners like Kubernetes.
"""

from __future__ import annotations

import abc
import asyncio
import os
import signal
from typing import Any, Dict, Optional, override

import urllib3
from jupyter_client import localinterfaces

from .remote import RemoteEnterpriseProvisioner

urllib3.disable_warnings()

local_ip = localinterfaces.public_ips()[0]

default_kernel_uid = "1000"  # jovyan user is the default
default_kernel_gid = "100"  # users group is the default

# These could be enforced via a PodSecurityPolicy, but those affect
# all pods so the cluster admin would need to configure those for
# all applications.
prohibited_uids = os.getenv("EG_PROHIBITED_UIDS", "0").split(",")
prohibited_gids = os.getenv("EG_PROHIBITED_GIDS", "0").split(",")

mirror_working_dirs = bool(os.getenv("EG_MIRROR_WORKING_DIRS", "false").lower() == "true")

# Get the globally-configured default images.  Defaulting to None if not set.
default_kernel_image = os.getenv("EG_KERNEL_IMAGE")
default_kernel_executor_image = os.getenv("EG_KERNEL_EXECUTOR_IMAGE")


class ContainerEnterpriseProvisioner(RemoteEnterpriseProvisioner):
    """
    Container kernel provisioner for Enterprise Gateway.
    
    Provides container-specific functionality including:
    - Container image management
    - UID/GID enforcement
    - Container status monitoring
    - Working directory mirroring
    """

    def __init__(self, **kwargs):
        """Initialize the container provisioner."""
        # Extract container-specific configuration before calling super()
        self.kernel_image = kwargs.pop('kernel_image', None)
        self.kernel_executor_image = kwargs.pop('kernel_executor_image', None)
        
        super().__init__(**kwargs)
        
        # Container-specific attributes
        self.container_name = ""
        self.assigned_node_ip = None

    @override
    async def pre_launch(self, **kwargs: Any) -> Dict[str, Any]:
        """
        Perform pre-launch setup specific to containers.
        
        Sets up container images, enforces security policies,
        and prepares the container environment.
        """
        # Get base pre-launch setup from parent
        launch_kwargs = await super().pre_launch(**kwargs)
        
        # Set up container-specific environment
        self._determine_kernel_images(**launch_kwargs)
        self._enforce_prohibited_ids(**launch_kwargs)
        self._configure_working_directory(**launch_kwargs)
        
        return launch_kwargs

    def _determine_kernel_images(self, **kwargs: Dict[str, Any]) -> None:
        """
        Determine which kernel images to use.

        Initialize to any defined in the process proxy override that then let those provided
        by client via env override.
        """
        # Get kernel image from various sources
        kernel_image = self.kernel_image or default_kernel_image
        env = kwargs.get("env", {})
        self.kernel_image = env.get("KERNEL_IMAGE", kernel_image)

        if self.kernel_image is None:
            self.log_and_raise(
                http_status_code=500,
                reason="No kernel image could be determined! Set the `kernel_image` in the "
                "provisioner configuration or provide KERNEL_IMAGE environment variable.",
            )

        # If no default executor image is configured, default it to current image
        kernel_executor_image = self.kernel_executor_image or default_kernel_executor_image or self.kernel_image
        self.kernel_executor_image = env.get("KERNEL_EXECUTOR_IMAGE", kernel_executor_image)
        
        # Update environment variables
        env["KERNEL_IMAGE"] = self.kernel_image
        env["KERNEL_EXECUTOR_IMAGE"] = self.kernel_executor_image

    def _enforce_prohibited_ids(self, **kwargs: Dict[str, Any]) -> None:
        """Determine UID and GID with which to launch container and ensure they are not prohibited."""
        env = kwargs.get("env", {})
        kernel_uid = env.get("KERNEL_UID", default_kernel_uid)
        kernel_gid = env.get("KERNEL_GID", default_kernel_gid)

        if kernel_uid in prohibited_uids:
            http_status_code = 403
            error_message = (
                f"Kernel's UID value of '{kernel_uid}' has been denied via EG_PROHIBITED_UIDS!"
            )
            self.log_and_raise(http_status_code=http_status_code, reason=error_message)
        elif kernel_gid in prohibited_gids:
            http_status_code = 403
            error_message = (
                f"Kernel's GID value of '{kernel_gid}' has been denied via EG_PROHIBITED_GIDS!"
            )
            self.log_and_raise(http_status_code=http_status_code, reason=error_message)

        # Ensure the kernel's env has what it needs in case they came from defaults
        env["KERNEL_UID"] = kernel_uid
        env["KERNEL_GID"] = kernel_gid

    def _configure_working_directory(self, **kwargs: Dict[str, Any]) -> None:
        """Configure working directory mirroring."""
        env = kwargs.get("env", {})
        
        # If mirroring is not enabled, remove working directory if present
        if not mirror_working_dirs and "KERNEL_WORKING_DIR" in env:
            del env["KERNEL_WORKING_DIR"]

    @override
    async def poll(self) -> Optional[int]:
        """
        Poll the container to determine if the kernel is still running.
        
        Returns:
            None if running, exit code if terminated
        """
        container_status = self.get_container_status(None)
        
        if container_status in self.get_error_states():
            self.log.warning(
                f"Container in error state: {container_status}. "
                f"Container: {self.container_name}, KernelID: {self.kernel_id}"
            )
            return 1  # Return non-zero exit code for failed state
        elif container_status in self.get_initial_states():
            return None  # Still running or starting
        else:
            # Unknown state, assume terminated
            self.log.debug(
                f"Container in unknown state: {container_status}. "
                f"Container: {self.container_name}, KernelID: {self.kernel_id}"
            )
            return 0

    @override
    async def send_signal(self, signum: int) -> None:
        """
        Send signal to container.

        Parameters
        ----------
        signum : int
            The signal number to send.  Zero is used to determine heartbeat.
        """
        if signum == 0:
            # Heartbeat check - poll for status
            await self.poll()
        elif signum == signal.SIGKILL:
            await self.kill()
        else:
            # This is very likely an interrupt signal, so defer to the super class
            # which should use the communication port.
            await super().send_signal(signum)

    @override
    async def kill(self, restart: bool = False) -> None:
        """
        Kill a containerized kernel.

        Args:
            restart: Whether this kill is for a restart operation
        """
        if self.container_name:  # We only have something to terminate if we have a name
            await self._terminate_container_resources()

    @override
    async def confirm_remote_startup(self) -> bool:
        """
        Confirm the container has started and returned necessary connection information.
        
        Returns:
            True if startup confirmed, False otherwise
        """
        self.log.debug("Trying to confirm kernel container startup status")
        
        max_attempts = 30  # 30 attempts with 2-second intervals = 60 seconds max wait
        attempt = 0
        ready_to_connect = False  # we're ready to connect when we have a connection file to use
        
        while not ready_to_connect and attempt < max_attempts:
            attempt += 1
            await asyncio.sleep(2)  # Wait 2 seconds between attempts

            container_status = self.get_container_status(attempt)
            if container_status:
                if container_status in self.get_error_states():
                    self.log_and_raise(
                        http_status_code=500,
                        reason=f"Error starting kernel container; status: '{container_status}'.",
                    )
                else:
                    if self.assigned_host:
                        ready_to_connect = await self.receive_connection_info()
                        # We won't send process signals for container lifecycle management
                        self.pid = 0
                        self.pgid = 0
            else:
                self.detect_launch_failure()
                
        return ready_to_connect

    @override
    async def get_provisioner_info(self) -> Dict[str, Any]:
        """Capture container-specific information for session persistence."""
        info = await super().get_provisioner_info()
        info.update({
            "container_name": self.container_name,
            "assigned_node_ip": self.assigned_node_ip,
            "kernel_image": self.kernel_image,
            "kernel_executor_image": self.kernel_executor_image,
        })
        return info

    @override
    async def load_provisioner_info(self, provisioner_info: Dict[str, Any]) -> None:
        """Load container-specific information for session persistence."""
        await super().load_provisioner_info(provisioner_info)
        
        self.container_name = provisioner_info.get("container_name", "")
        self.assigned_node_ip = provisioner_info.get("assigned_node_ip")
        self.kernel_image = provisioner_info.get("kernel_image")
        self.kernel_executor_image = provisioner_info.get("kernel_executor_image")

    # Abstract methods that must be implemented by subclasses
    @abc.abstractmethod
    def get_initial_states(self) -> set:
        """Return list of states in lowercase indicating container is starting (includes running)."""
        raise NotImplementedError

    @abc.abstractmethod
    def get_error_states(self) -> set:
        """Returns the list of error states (in lowercase)."""
        raise NotImplementedError

    @abc.abstractmethod
    def get_container_status(self, iteration: Optional[int]) -> str:
        """Returns the current container state (in lowercase) or the empty string if not available."""
        raise NotImplementedError

    @abc.abstractmethod
    async def _terminate_container_resources(self) -> None:
        """Terminate any artifacts created on behalf of the container's lifetime."""
        raise NotImplementedError