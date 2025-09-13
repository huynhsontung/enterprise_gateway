"""Kubernetes Enterprise Gateway Kernel Provisioner.

This module provides Kubernetes-specific kernel provisioning functionality,
replacing KubernetesProcessProxy with the modern KernelProvisioner interface.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import signal
from typing import Any, Dict, Optional, override

import kubernetes
import urllib3
from kubernetes import client, config
from jupyter_client import KernelConnectionInfo

from .remote import RemoteEnterpriseProvisioner
from ..sessions.kernelsessionmanager import KernelSessionManager

# Disable excessive kubernetes warnings
urllib3.disable_warnings()

# Default logging level of kubernetes produces too much noise - raise to warning only.
logging.getLogger("kubernetes").setLevel(os.environ.get("EG_KUBERNETES_LOG_LEVEL", logging.WARNING))

# Environment configuration
enterprise_gateway_namespace = os.environ.get("EG_NAMESPACE", "default")
default_kernel_service_account_name = os.environ.get(
    "EG_DEFAULT_KERNEL_SERVICE_ACCOUNT_NAME", "default"
)
kernel_cluster_role = os.environ.get("EG_KERNEL_CLUSTER_ROLE", "cluster-admin")
share_gateway_namespace = bool(os.environ.get("EG_SHARED_NAMESPACE", "False").lower() == "true")
kpt_dir = os.environ.get("EG_POD_TEMPLATE_DIR", "/tmp")

# Load Kubernetes configuration
try:
    # Try in-cluster config first (when running inside Kubernetes)
    config.load_incluster_config()
except config.ConfigException:
    try:
        # Fall back to local kubeconfig (development/testing)
        config.load_kube_config()
    except config.ConfigException:
        # No Kubernetes config available - this is OK for testing/development
        pass


class KubernetesEnterpriseProvisioner(RemoteEnterpriseProvisioner):
    """
    Kubernetes kernel provisioner for Enterprise Gateway.
    
    This provisioner manages kernel lifecycle in Kubernetes clusters, providing:
    - Pod creation and management
    - Namespace isolation
    - Service account and RBAC configuration
    - Pod template customization
    - Container status monitoring
    - Resource cleanup
    
    Replaces KubernetesProcessProxy with the modern KernelProvisioner interface.
    """
    
    # Identifies the kind of object being managed by this provisioner
    object_kind = "Pod"
    
    def __init__(self, **kwargs):
        """Initialize the Kubernetes Enterprise Gateway provisioner."""
        # Extract Kubernetes-specific arguments before calling super()
        self.kernel_namespace = kwargs.pop('kernel_namespace', None)
        self.kernel_image = kwargs.pop('kernel_image', None)
        self.kernel_executor_image = kwargs.pop('kernel_executor_image', None)
        self.kernel_service_account_name = kwargs.pop('kernel_service_account_name', None)
        
        super().__init__(**kwargs)
        
        # Additional Kubernetes-specific attributes
        self.kernel_pod_name = None
        self.delete_kernel_namespace = False
        self.container_name = None
        self.assigned_node_ip = None
        
        self.log = logging.getLogger(__name__)
    
    @override
    async def pre_launch(self, **kwargs: Any) -> Dict[str, Any]:
        """
        Perform pre-launch setup specific to Kubernetes.
        
        Sets up Kubernetes environment variables, determines pod name and namespace,
        and prepares the launch environment.
        """
        # Get base pre-launch setup from parent
        launch_kwargs = await super().pre_launch(**kwargs)
        
        # Add Kubernetes-specific environment variables
        env = launch_kwargs.setdefault("env", {})
        
        # Copy Kubernetes configuration from host environment
        for key in os.environ:
            if key.startswith("KUBECONFIG") or key.startswith("KUBERNETES_SERVICE"):
                env[key] = os.environ[key]
        
        # Determine pod name and namespace before launch
        self.kernel_pod_name = self._determine_kernel_pod_name(**launch_kwargs)
        self.kernel_namespace = self._determine_kernel_namespace(**launch_kwargs)
        
        # Add Kubernetes-specific environment variables for the kernel
        env.update({
            "KERNEL_POD_NAME": self.kernel_pod_name,
            "KERNEL_NAMESPACE": self.kernel_namespace,
        })
        
        self.log.info(
            f"Kubernetes pre-launch setup complete. Pod: {self.kernel_pod_name}, "
            f"Namespace: {self.kernel_namespace}, KernelID: {self.kernel_id}"
        )
        
        return launch_kwargs
    
    @override
    async def launch_kernel(self, cmd: list[str], **kwargs: Any) -> KernelConnectionInfo:
        """
        Launch a kernel in a Kubernetes pod.
        
        This method delegates to the parent class which handles the full remote launch process
        including ResponseManager communication and connection info setup.
        """
        self.log.info(
            f"Launching Kubernetes kernel. Pod: {self.kernel_pod_name}, "
            f"Namespace: {self.kernel_namespace}, KernelID: {self.kernel_id}"
        )
        
        # Call parent implementation which handles the complete remote launch process
        connection_info = await super().launch_kernel(cmd, **kwargs)
        
        self.log.info(
            f"Kubernetes kernel launch completed. Pod: {self.kernel_pod_name}, "
            f"Namespace: {self.kernel_namespace}, KernelID: {self.kernel_id}"
        )
        
        return connection_info
    
    @override
    async def _launch_remote_process(self, cmd: list[str], **kwargs) -> KernelConnectionInfo:
        """
        Launch the actual Kubernetes pod for the kernel.
        
        This delegates to the existing kernel launcher infrastructure which handles
        pod creation, template processing, and container startup. The actual pod
        creation and management is handled by the kernel launcher scripts.
        
        Args:
            cmd: Command to launch the kernel
            **kwargs: Launch parameters including environment variables
            
        Returns:
            Empty connection info - actual connection info will be received via ResponseManager
        """
        self.log.info(
            f"Launching Kubernetes pod for kernel. Pod: {self.kernel_pod_name}, "
            f"Namespace: {self.kernel_namespace}, Command: {cmd}"
        )
        
        # The Kubernetes kernel launcher handles the actual pod creation
        # We return empty connection info here because the real connection info 
        # comes via ResponseManager after the pod starts and the kernel establishes communication
        
        # The launch process is handled by:
        # 1. Enterprise Gateway launches kernel launcher script
        # 2. Kernel launcher creates pod using our pre-launch environment setup
        # 3. Pod starts and kernel sends connection info via ResponseManager
        # 4. We receive and process the connection info in receive_connection_info()
        
        # Return empty connection info as placeholder
        return {}
    
    @override 
    async def confirm_remote_startup(self) -> bool:
        """
        Confirm the Kubernetes pod has started successfully.
        
        This method polls the pod status to ensure it has reached a running state
        and the kernel process is ready to accept connections.
        
        Returns:
            True if pod is running successfully, False otherwise
        """
        self.log.debug(f"Confirming Kubernetes pod startup: {self.kernel_pod_name}")
        
        # Poll for pod to reach running state
        max_attempts = 30  # 30 attempts with 2-second intervals = 60 seconds max wait
        attempt = 0
        
        while attempt < max_attempts:
            attempt += 1
            
            try:
                pod_status = self.get_container_status(attempt)
                
                if pod_status == "running":
                    self.log.info(
                        f"Kubernetes pod confirmed running. Pod: {self.kernel_pod_name}, "
                        f"IP: {self.assigned_ip}, Attempt: {attempt}"
                    )
                    return True
                elif pod_status in self.get_error_states():
                    self.log.error(
                        f"Kubernetes pod failed to start. Pod: {self.kernel_pod_name}, "
                        f"Status: {pod_status}, Attempt: {attempt}"
                    )
                    return False
                else:
                    # Pod is still starting (pending, etc.)
                    self.log.debug(
                        f"Kubernetes pod not ready yet. Pod: {self.kernel_pod_name}, "
                        f"Status: {pod_status}, Attempt: {attempt}/{max_attempts}"
                    )
                    
            except Exception as e:
                self.log.warning(f"Error checking pod status on attempt {attempt}: {e}")
            
            # Wait before next attempt
            await asyncio.sleep(2.0)
        
        # Timed out waiting for pod to start
        self.log.error(
            f"Timeout waiting for Kubernetes pod to start. Pod: {self.kernel_pod_name}, "
            f"Final status: {self.get_container_status(None)}"
        )
        return False
    
    @override
    async def poll(self) -> Optional[int]:
        """
        Poll the Kubernetes pod to determine if the kernel is still running.
        
        Returns:
            None if running, exit code if terminated
        """
        # Use container status to determine if kernel is running
        container_status = self.get_container_status(None)
        
        if container_status in self.get_error_states():
            self.log.warning(
                f"Kubernetes pod in error state: {container_status}. "
                f"Pod: {self.kernel_pod_name}, KernelID: {self.kernel_id}"
            )
            return 1  # Return non-zero exit code for failed state
        elif container_status in self.get_initial_states():
            return None  # Still running or starting
        else:
            # Unknown state, assume terminated
            self.log.debug(
                f"Kubernetes pod in unknown state: {container_status}. "
                f"Pod: {self.kernel_pod_name}, KernelID: {self.kernel_id}"
            )
            return 0
    
    @override
    async def wait(self) -> Optional[int]:
        """
        Wait for the Kubernetes pod to terminate.
        
        Returns:
            Exit code of the terminated kernel
        """
        self.log.debug(f"Waiting for Kubernetes pod termination. Pod: {self.kernel_pod_name}")
        
        # Poll until the pod is no longer in a running state
        while True:
            exit_code = await self.poll()
            if exit_code is not None:
                self.log.info(
                    f"Kubernetes pod terminated with exit code: {exit_code}. "
                    f"Pod: {self.kernel_pod_name}, KernelID: {self.kernel_id}"
                )
                return exit_code
            
            # Wait a bit before polling again
            await asyncio.sleep(1.0)
    
    @override
    async def send_signal(self, signum: int) -> None:
        """
        Send a signal to the kernel process in the Kubernetes pod.
        
        Args:
            signum: Signal number to send
        """
        # Use getattr to safely check for signal constants (Windows compatibility)
        sigkill = getattr(signal, 'SIGKILL', 9)
        sigterm = getattr(signal, 'SIGTERM', 15)
        
        if signum == sigkill:
            self.log.info(
                f"Forcefully terminating Kubernetes pod. Pod: {self.kernel_pod_name}, "
                f"KernelID: {self.kernel_id}"
            )
            await self.kill()
        elif signum == sigterm:
            self.log.info(
                f"Gracefully terminating Kubernetes pod. Pod: {self.kernel_pod_name}, "
                f"KernelID: {self.kernel_id}"
            )
            await self.terminate()
        else:
            self.log.warning(
                f"Signal {signum} not supported for Kubernetes pods. "
                f"Pod: {self.kernel_pod_name}, KernelID: {self.kernel_id}"
            )
    
    @override
    async def kill(self, restart: bool = False) -> None:
        """
        Forcefully kill the Kubernetes pod and clean up resources.
        
        Args:
            restart: Whether this kill is for a restart operation
        """
        self.log.info(
            f"Killing Kubernetes pod (restart={restart}). Pod: {self.kernel_pod_name}, "
            f"Namespace: {self.kernel_namespace}, KernelID: {self.kernel_id}"
        )
        
        # Delete the pod immediately
        try:
            delete_options = client.V1DeleteOptions(
                grace_period_seconds=0,
                propagation_policy="Background"
            )
            client.CoreV1Api().delete_namespaced_pod(
                name=self.kernel_pod_name,
                namespace=self.kernel_namespace,
                body=delete_options
            )
            self.log.info(f"Kubernetes pod deleted: {self.kernel_pod_name}")
        except Exception as e:
            self.log.warning(f"Error deleting Kubernetes pod {self.kernel_pod_name}: {e}")
        
        # Clean up additional resources
        await self._cleanup_resources()
    
    @override
    async def terminate(self, restart: bool = False) -> None:
        """
        Gracefully terminate the Kubernetes pod and clean up resources.
        
        Args:
            restart: Whether this termination is for a restart operation
        """
        self.log.info(
            f"Terminating Kubernetes pod (restart={restart}). Pod: {self.kernel_pod_name}, "
            f"Namespace: {self.kernel_namespace}, KernelID: {self.kernel_id}"
        )
        
        # Delete the pod with grace period
        try:
            delete_options = client.V1DeleteOptions(
                grace_period_seconds=30,
                propagation_policy="Background"
            )
            client.CoreV1Api().delete_namespaced_pod(
                name=self.kernel_pod_name,
                namespace=self.kernel_namespace,
                body=delete_options
            )
            self.log.info(f"Kubernetes pod termination initiated: {self.kernel_pod_name}")
        except Exception as e:
            self.log.warning(f"Error terminating Kubernetes pod {self.kernel_pod_name}: {e}")
        
        # Clean up additional resources
        await self._cleanup_resources()
    
    @override
    async def cleanup(self, restart: bool = False) -> None:
        """
        Clean up Kubernetes resources.
        
        Args:
            restart: Whether this cleanup is for a restart
        """
        self.log.info(
            f"Cleaning up Kubernetes resources. Pod: {self.kernel_pod_name}, "
            f"Namespace: {self.kernel_namespace}, Restart: {restart}, KernelID: {self.kernel_id}"
        )
        
        # Perform base cleanup
        await super().cleanup(restart)
        
        # Kubernetes-specific cleanup
        await self._cleanup_resources()
    
    async def _cleanup_resources(self) -> None:
        """Clean up Kubernetes-specific resources."""
        if not self.kernel_pod_name:
            return
        
        try:
            # Clean up namespace if we created it
            if self.delete_kernel_namespace and self.kernel_namespace:
                self._delete_kernel_namespace()
            
            # Clean up pod template file
            self._cleanup_pod_template_file()
            
        except Exception as e:
            self.log.warning(f"Error during Kubernetes resource cleanup: {e}")
    
    def get_initial_states(self) -> set:
        """Return list of states indicating container is starting or running."""
        return {"pending", "running"}
    
    def get_error_states(self) -> set:
        """Return list of states indicating container failed."""
        return {"failed"}
    
    def get_container_status(self, iteration: Optional[int]) -> str:
        """
        Return current container state by querying the Kubernetes API.
        
        Args:
            iteration: Current polling iteration (for logging)
            
        Returns:
            Current pod status
        """
        pod_status = ""
        
        if not self.kernel_pod_name or not self.kernel_namespace:
            return "unknown"
        
        try:
            kernel_label_selector = f"kernel_id={self.kernel_id},component=kernel"
            ret = client.CoreV1Api().list_namespaced_pod(
                namespace=self.kernel_namespace,
                label_selector=kernel_label_selector
            )
            
            if ret and ret.items:
                pod_info = ret.items[0]
                self.container_name = pod_info.metadata.name
                
                if pod_info.status:
                    pod_status = pod_info.status.phase.lower()
                    
                    if pod_status == "running" and not self.assigned_host:
                        # Pod is running, capture connection info
                        self.assigned_ip = pod_info.status.pod_ip
                        self.assigned_host = self.container_name
                        self.assigned_node_ip = pod_info.status.host_ip
                        
                        self.log.info(
                            f"Kubernetes pod is running. Pod IP: {self.assigned_ip}, "
                            f"Host IP: {self.assigned_node_ip}, Pod: {self.kernel_pod_name}"
                        )
            
            if iteration:  # Only log if iteration is provided (avoid poll() noise)
                self.log.debug(
                    f"{iteration}: Kubernetes pod status check. "
                    f"Namespace: {self.kernel_namespace}, Pod: {self.container_name}, "
                    f"Status: {pod_status}, Pod IP: {self.assigned_ip}, KernelID: {self.kernel_id}"
                )
                
        except Exception as e:
            self.log.warning(f"Error getting Kubernetes pod status: {e}")
            pod_status = "error"
        
        return pod_status
    
    def _determine_kernel_pod_name(self, **kwargs: Any) -> str:
        """
        Determine the name for the kernel pod.
        
        Returns:
            Pod name for the kernel
        """
        env = kwargs.get("env", {})
        pod_name = env.get("KERNEL_POD_NAME")
        
        if pod_name is None:
            # Default pod name based on username and kernel ID
            username = KernelSessionManager.get_kernel_username(**kwargs)
            pod_name = f"{username}-{self.kernel_id}"
        else:
            self.log.debug(f"Processing KERNEL_POD_NAME from environment: {pod_name}")
            
            # Handle template variables in pod name
            if "{{" in pod_name and "}}" in pod_name:
                self.log.debug("Processing KERNEL_POD_NAME template variables")
                
                # Build template variables
                keywords = {}
                for name, value in env.items():
                    if name.startswith("KERNEL_"):
                        keywords[name.lower()] = value
                keywords["kernel_id"] = self.kernel_id
                
                # Perform safe template substitution
                substituted = self._safe_template_substitute(pod_name, keywords)
                if substituted is None:
                    # Fall back to default if template substitution fails
                    self.log.warning("Falling back to default pod name due to template issues")
                    username = KernelSessionManager.get_kernel_username(**kwargs)
                    pod_name = f"{username}-{self.kernel_id}"
                else:
                    pod_name = substituted
        
        # Ensure pod name is DNS-compliant
        pod_name = self._make_dns_compliant(pod_name)
        
        # Update environment for kernel launcher
        env["KERNEL_POD_NAME"] = pod_name
        
        return pod_name
    
    def _determine_kernel_namespace(self, **kwargs: Any) -> str:
        """
        Determine the namespace for the kernel pod.
        
        Returns:
            Namespace name for the kernel
        """
        env = kwargs.get("env", {})
        namespace = env.get("KERNEL_NAMESPACE")
        
        if namespace is None:
            if share_gateway_namespace:
                # Use the same namespace as Enterprise Gateway
                namespace = enterprise_gateway_namespace
                self.log.info(f"Using shared namespace: {namespace}")
            else:
                # Create a dedicated namespace for this kernel
                service_account_name = self._determine_kernel_service_account_name(**kwargs)
                namespace = self._create_kernel_namespace(service_account_name)
        else:
            self.log.info(f"Using client-provided namespace: {namespace}")
        
        # Update environment for kernel launcher
        env["KERNEL_NAMESPACE"] = namespace
        
        return namespace
    
    @staticmethod
    def _determine_kernel_service_account_name(**kwargs: Any) -> str:
        """
        Determine the service account name for the kernel.
        
        Returns:
            Service account name
        """
        env = kwargs.get("env", {})
        return env.get("KERNEL_SERVICE_ACCOUNT_NAME", default_kernel_service_account_name)
    
    def _create_kernel_namespace(self, service_account_name: str) -> str:
        """
        Create a dedicated namespace for the kernel.
        
        Args:
            service_account_name: Service account to bind to the namespace
            
        Returns:
            Created namespace name
        """
        if not self.kernel_pod_name:
            raise ValueError("kernel_pod_name must be set before creating namespace")
            
        namespace = self.kernel_pod_name
        
        # Prepare namespace metadata
        labels = {
            "app": "enterprise-gateway",
            "component": "kernel",
            "kernel_id": self.kernel_id
        }
        namespace_metadata = client.V1ObjectMeta(name=namespace, labels=labels)
        body = client.V1Namespace(metadata=namespace_metadata)
        
        # Create the namespace
        try:
            client.CoreV1Api().create_namespace(body=body)
            self.delete_kernel_namespace = True
            self.log.info(f"Created kernel namespace: {namespace}")
            
            # Create role binding for the namespace
            self._create_role_binding(namespace, service_account_name)
            
        except Exception as err:
            # Import the exception class for proper type checking
            from kubernetes.client.rest import ApiException
            
            if (
                isinstance(err, ApiException) 
                and err.status == 409
            ):
                # Namespace already exists - this is OK during restart scenarios
                self.delete_kernel_namespace = False  # Don't delete pre-existing namespace
                self.log.info(f"Re-using existing namespace: {namespace}")
            else:
                if self.delete_kernel_namespace:
                    # Clean up the namespace if creation failed
                    try:
                        delete_options = client.V1DeleteOptions(
                            grace_period_seconds=0,
                            propagation_policy="Background"
                        )
                        client.CoreV1Api().delete_namespace(name=namespace, body=delete_options)
                        self.log.warning(f"Deleted failed namespace: {namespace}")
                    except Exception:
                        pass
                
                reason = f"Error creating namespace '{namespace}': {err}"
                self.log_and_raise(http_status_code=500, reason=reason)
        
        return namespace
    
    def _create_role_binding(self, namespace: str, service_account_name: str) -> None:
        """
        Create a role binding for the kernel namespace.
        
        Args:
            namespace: Namespace to create role binding in
            service_account_name: Service account to bind
        """
        role_binding_name = kernel_cluster_role
        
        # Prepare role binding metadata
        labels = {
            "app": "enterprise-gateway",
            "component": "kernel",
            "kernel_id": self.kernel_id
        }
        binding_metadata = client.V1ObjectMeta(name=role_binding_name, labels=labels)
        
        # Create role reference
        binding_role_ref = client.V1RoleRef(
            api_group="rbac.authorization.k8s.io",
            kind="ClusterRole",
            name=kernel_cluster_role
        )
        
        # Create subject reference (simplified approach)
        try:
            # Try to create a V1Subject directly
            binding_subjects = [
                {
                    "kind": "ServiceAccount",
                    "name": service_account_name,
                    "namespace": namespace
                }
            ]
            
            # Create role binding
            body = client.V1RoleBinding(
                kind="RoleBinding",
                metadata=binding_metadata,
                role_ref=binding_role_ref,
                subjects=binding_subjects,
            )
            
            client.RbacAuthorizationV1Api().create_namespaced_role_binding(
                namespace=namespace, body=body
            )
            self.log.info(
                f"Created role binding '{role_binding_name}' in namespace '{namespace}' "
                f"for service account '{service_account_name}'"
            )
        except Exception as e:
            self.log.warning(f"Error creating role binding: {e}")
    
    def _delete_kernel_namespace(self) -> None:
        """Delete the kernel namespace and associated resources."""
        if not self.kernel_namespace:
            return
        
        try:
            delete_options = client.V1DeleteOptions(
                grace_period_seconds=0,
                propagation_policy="Background"
            )
            client.CoreV1Api().delete_namespace(
                name=self.kernel_namespace,
                body=delete_options
            )
            self.log.info(f"Deleted kernel namespace: {self.kernel_namespace}")
        except Exception as e:
            self.log.warning(f"Error deleting namespace {self.kernel_namespace}: {e}")
    
    def _cleanup_pod_template_file(self) -> None:
        """Clean up pod template file if it exists."""
        if not self.kernel_id:
            return
        
        kpt_file = os.path.join(kpt_dir, f"kpt_{self.kernel_id}")
        try:
            os.remove(kpt_file)
            self.log.debug(f"Removed pod template file: {kpt_file}")
        except OSError:
            pass  # File doesn't exist or already removed
    
    def _safe_template_substitute(self, template_str: str, variables: dict) -> Optional[str]:
        """
        Safely substitute template variables in a string.
        
        Args:
            template_str: Template string with {{variable}} placeholders
            variables: Dictionary of variable values
            
        Returns:
            Substituted string, or None if substitution fails
        """
        import re
        
        missing_vars = []
        
        def replace_var(match):
            var_name = match.group(1).strip()  # Strip whitespace from variable name
            if var_name in variables:
                return str(variables[var_name])
            else:
                missing_vars.append(var_name)
                return match.group(0)  # Keep original placeholder
        
        try:
            # Replace {{variable}} patterns (allowing whitespace around variable name)
            result = re.sub(r'\{\{\s*(\w+)\s*\}\}', replace_var, template_str)
            
            # Check for remaining unsupported template syntax (Jinja2 expressions)
            if re.search(r'\{\{[^}]*[^}\w\s][^}]*\}\}', result) or '{%' in result:
                self.log.warning(
                    "Invalid template syntax detected - only simple variable substitution supported"
                )
                return None
            
            # Return None if any variables are missing
            if missing_vars:
                self.log.warning(f"Template substitution failed, missing variables: {missing_vars}")
                return None
            
            return result
            
        except Exception as e:
            self.log.warning(f"Template substitution error: {e}")
            return None
    
    def _make_dns_compliant(self, name: str) -> str:
        """
        Make a name DNS-compliant for Kubernetes.
        
        Args:
            name: Original name
            
        Returns:
            DNS-compliant name
        """
        # Convert to lowercase and replace invalid characters with hyphens
        dns_name = re.sub(r"[^0-9a-z]+", "-", name.lower())
        
        # Remove leading/trailing hyphens
        while dns_name.startswith("-"):
            dns_name = dns_name[1:]
        while dns_name.endswith("-"):
            dns_name = dns_name[:-1]
        
        # Limit to 63 characters (DNS-1123 label limit)
        if len(dns_name) > 63:
            dns_name = dns_name[:63]
            # Remove trailing hyphens again after truncation
            while dns_name.endswith("-"):
                dns_name = dns_name[:-1]
        
        return dns_name