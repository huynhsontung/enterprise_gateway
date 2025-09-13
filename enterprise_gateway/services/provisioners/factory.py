"""Provisioner factory and migration utilities for Enterprise Gateway."""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Type, Union

from jupyter_client.kernelspec import KernelSpec
from jupyter_client.provisioning.provisioner_base import KernelProvisionerBase

from .local import LocalEnterpriseProvisioner
from .remote import RemoteEnterpriseProvisioner
from .kubernetes import KubernetesEnterpriseProvisioner


logger = logging.getLogger(__name__)


# Mapping from old process proxy class names to new provisioner classes
PROCESS_PROXY_TO_PROVISIONER_MAP = {
    "enterprise_gateway.services.processproxies.processproxy.LocalProcessProxy": LocalEnterpriseProvisioner,
    "enterprise_gateway.services.processproxies.yarn.YarnProcessProxy": RemoteEnterpriseProvisioner,  # Will be YarnEnterpriseProvisioner later
    "enterprise_gateway.services.processproxies.k8s.KubernetesProcessProxy": KubernetesEnterpriseProvisioner,
    "enterprise_gateway.services.processproxies.docker.DockerProcessProxy": RemoteEnterpriseProvisioner,  # Will be DockerEnterpriseProvisioner later
    "enterprise_gateway.services.processproxies.distributed.DistributedProcessProxy": RemoteEnterpriseProvisioner,
    "enterprise_gateway.services.processproxies.conductor.ConductorClusterProcessProxy": RemoteEnterpriseProvisioner,
}

# Mapping from provisioner names to provisioner classes
PROVISIONER_NAME_TO_CLASS_MAP = {
    "local-enterprise-provisioner": LocalEnterpriseProvisioner,
    "remote-enterprise-provisioner": RemoteEnterpriseProvisioner,
    "kubernetes-enterprise-provisioner": KubernetesEnterpriseProvisioner,
    # Future specific provisioners:
    # "yarn-enterprise-provisioner": YarnEnterpriseProvisioner,
    # "docker-enterprise-provisioner": DockerEnterpriseProvisioner,
}


def get_provisioner_config(kernelspec: KernelSpec) -> Dict[str, Any]:
    """
    Extract provisioner configuration from kernelspec metadata.
    
    Supports both new kernel_provisioner format and legacy process_proxy format
    for backward compatibility during migration.
    
    Args:
        kernelspec: The kernel specification object
        
    Returns:
        Dictionary with provisioner configuration
    """
    metadata = kernelspec.metadata or {}
    
    # First check for new kernel_provisioner format
    if "kernel_provisioner" in metadata:
        provisioner_config = metadata["kernel_provisioner"]
        if isinstance(provisioner_config, dict) and "provisioner_name" in provisioner_config:
            logger.debug(f"Found kernel_provisioner metadata: {provisioner_config}")
            return provisioner_config
    
    # Fall back to legacy process_proxy format for backward compatibility
    if "process_proxy" in metadata:
        process_proxy_config = metadata["process_proxy"]
        if isinstance(process_proxy_config, dict) and "class_name" in process_proxy_config:
            logger.debug(f"Found legacy process_proxy metadata: {process_proxy_config}")
            # Convert process proxy config to provisioner config
            return convert_process_proxy_to_provisioner_config(process_proxy_config)
    
    # Default to local provisioner
    logger.debug("No provisioner or process_proxy metadata found, using default local provisioner")
    return {
        "provisioner_name": "local-enterprise-provisioner",
        "config": {}
    }


def convert_process_proxy_to_provisioner_config(process_proxy_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert legacy process_proxy configuration to new kernel_provisioner format.
    
    Args:
        process_proxy_config: Legacy process proxy configuration
        
    Returns:
        New provisioner configuration
    """
    class_name = process_proxy_config.get("class_name", "")
    config = process_proxy_config.get("config", {})
    
    # Map process proxy class to provisioner name
    if class_name in PROCESS_PROXY_TO_PROVISIONER_MAP:
        provisioner_class = PROCESS_PROXY_TO_PROVISIONER_MAP[class_name]
        
        # Determine provisioner name based on class
        if provisioner_class == LocalEnterpriseProvisioner:
            provisioner_name = "local-enterprise-provisioner"
        elif provisioner_class == KubernetesEnterpriseProvisioner:
            provisioner_name = "kubernetes-enterprise-provisioner"
        else:
            provisioner_name = "remote-enterprise-provisioner"
            
        logger.info(f"Converting process_proxy '{class_name}' to provisioner '{provisioner_name}'")
        
        return {
            "provisioner_name": provisioner_name,
            "config": config,
            # Include original class name for debugging/migration tracking
            "_migrated_from": class_name
        }
    else:
        logger.warning(f"Unknown process_proxy class '{class_name}', using remote provisioner")
        return {
            "provisioner_name": "remote-enterprise-provisioner",
            "config": config,
            "_migrated_from": class_name
        }


def get_provisioner_class(kernelspec: KernelSpec) -> Type[KernelProvisionerBase]:
    """
    Get the appropriate provisioner class for a kernelspec.
    
    Args:
        kernelspec: The kernel specification object
        
    Returns:
        Provisioner class to use
    """
    provisioner_config = get_provisioner_config(kernelspec)
    provisioner_name = provisioner_config.get("provisioner_name", "local-enterprise-provisioner")
    
    # Get the provisioner class
    if provisioner_name in PROVISIONER_NAME_TO_CLASS_MAP:
        provisioner_class = PROVISIONER_NAME_TO_CLASS_MAP[provisioner_name]
        logger.debug(f"Using provisioner class: {provisioner_class.__name__}")
        return provisioner_class
    else:
        logger.warning(f"Unknown provisioner name '{provisioner_name}', using LocalEnterpriseProvisioner")
        return LocalEnterpriseProvisioner


def create_provisioner_for_kernelspec(kernelspec: KernelSpec, **kwargs) -> KernelProvisionerBase:
    """
    Create a provisioner instance for the given kernelspec.
    
    Args:
        kernelspec: The kernel specification object
        **kwargs: Additional arguments for provisioner initialization
        
    Returns:
        Configured provisioner instance
    """
    provisioner_config = get_provisioner_config(kernelspec)
    provisioner_class = get_provisioner_class(kernelspec)
    
    # Merge provisioner config into kwargs
    config = provisioner_config.get("config", {})
    kwargs.update(config)
    
    # Create the provisioner instance
    provisioner = provisioner_class(**kwargs)
    
    logger.info(f"Created {provisioner_class.__name__} for kernel '{kernelspec.display_name}'")
    return provisioner


def migrate_kernelspec_metadata(kernelspec: KernelSpec) -> bool:
    """
    Migrate kernelspec metadata from process_proxy to kernel_provisioner format.
    
    Args:
        kernelspec: The kernel specification object to migrate
        
    Returns:
        True if migration was performed, False if no migration needed
    """
    if not kernelspec.metadata:
        kernelspec.metadata = {}
    
    # Check if already using new format
    if "kernel_provisioner" in kernelspec.metadata:
        logger.debug("Kernelspec already uses kernel_provisioner metadata")
        return False
    
    # Check if has process_proxy to migrate
    if "process_proxy" in kernelspec.metadata:
        process_proxy_config = kernelspec.metadata["process_proxy"]
        provisioner_config = convert_process_proxy_to_provisioner_config(process_proxy_config)
        
        # Add new kernel_provisioner metadata
        kernelspec.metadata["kernel_provisioner"] = provisioner_config
        
        # Optionally remove old process_proxy metadata
        # For now, keep it for backward compatibility
        # del kernelspec.metadata["process_proxy"]
        
        logger.info(f"Migrated kernelspec '{kernelspec.display_name}' from process_proxy to kernel_provisioner")
        return True
    else:
        # Add default provisioner metadata
        kernelspec.metadata["kernel_provisioner"] = {
            "provisioner_name": "local-enterprise-provisioner",
            "config": {}
        }
        logger.info(f"Added default kernel_provisioner metadata to '{kernelspec.display_name}'")
        return True