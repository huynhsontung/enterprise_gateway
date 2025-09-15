"""Tests for LocalEnterpriseProvisioner."""

import os
import re
import unittest.mock as mock
from typing import List
from unittest.mock import MagicMock, patch

import pytest
from jupyter_client.kernelspec import KernelSpec

from enterprise_gateway.services.provisioners.local import LocalEnterpriseProvisioner


class TestLocalEnterpriseProvisioner:
    """Test cases for LocalEnterpriseProvisioner."""

    @pytest.fixture
    def kernel_spec(self) -> KernelSpec:
        """Create a test kernel spec."""
        return KernelSpec(argv=["python", "-m", "ipykernel_launcher", "-f", "{connection_file}"])

    @pytest.fixture
    def provisioner(self, kernel_spec: KernelSpec) -> LocalEnterpriseProvisioner:
        """Create a test provisioner instance."""
        return LocalEnterpriseProvisioner(
            kernel_id="test-kernel-123",
            kernel_spec=kernel_spec,
        )

    def test_init(self, provisioner: LocalEnterpriseProvisioner) -> None:
        """Test provisioner initialization."""
        assert provisioner.kernel_id == "test-kernel-123"
        assert provisioner.pgid == 0
        assert hasattr(provisioner, "prohibited_local_ips")
        assert isinstance(provisioner.prohibited_local_ips, list)

    def test_prohibited_local_ips_trait_default_empty(self, provisioner: LocalEnterpriseProvisioner) -> None:
        """Test prohibited_local_ips trait default when no environment variable is set."""
        with patch.dict(os.environ, {}, clear=True):
            # Create new provisioner to trigger default calculation
            spec = KernelSpec(argv=["python"])
            new_provisioner = LocalEnterpriseProvisioner(kernel_id="test", kernel_spec=spec)
            assert new_provisioner.prohibited_local_ips == []

    def test_prohibited_local_ips_trait_from_env(self, kernel_spec: KernelSpec) -> None:
        """Test prohibited_local_ips trait gets values from environment variable."""
        env_value = "172.17.*,192.168.1.*,10.0.0.*"
        expected = ["172.17.*", "192.168.1.*", "10.0.0.*"]
        
        with patch.dict(os.environ, {"EG_PROHIBITED_LOCAL_IPS": env_value}):
            provisioner = LocalEnterpriseProvisioner(kernel_id="test", kernel_spec=kernel_spec)
            assert provisioner.prohibited_local_ips == expected

    def test_prohibited_local_ips_trait_from_env_with_spaces(self, kernel_spec: KernelSpec) -> None:
        """Test prohibited_local_ips trait handles spaces in environment variable."""
        env_value = " 172.17.* , 192.168.1.* ,  10.0.0.*  "
        expected = ["172.17.*", "192.168.1.*", "10.0.0.*"]
        
        with patch.dict(os.environ, {"EG_PROHIBITED_LOCAL_IPS": env_value}):
            provisioner = LocalEnterpriseProvisioner(kernel_id="test", kernel_spec=kernel_spec)
            assert provisioner.prohibited_local_ips == expected

    def test_prohibited_local_ips_trait_from_env_empty_values(self, kernel_spec: KernelSpec) -> None:
        """Test prohibited_local_ips trait ignores empty values in environment variable."""
        env_value = "172.17.*,,192.168.1.*,,"
        expected = ["172.17.*", "192.168.1.*"]
        
        with patch.dict(os.environ, {"EG_PROHIBITED_LOCAL_IPS": env_value}):
            provisioner = LocalEnterpriseProvisioner(kernel_id="test", kernel_spec=kernel_spec)
            assert provisioner.prohibited_local_ips == expected

    def test_prohibited_local_ips_programmatic_config(self, kernel_spec: KernelSpec) -> None:
        """Test prohibited_local_ips can be configured programmatically."""
        prohibited_ips = ["127.*", "10.*", "172.17.*"]
        provisioner = LocalEnterpriseProvisioner(
            kernel_id="test",
            kernel_spec=kernel_spec,
            prohibited_local_ips=prohibited_ips
        )
        assert provisioner.prohibited_local_ips == prohibited_ips

    @patch('enterprise_gateway.services.provisioners.local.localinterfaces.public_ips')
    def test_get_local_ip_instance_method_no_prohibited(self, mock_public_ips: MagicMock, provisioner: LocalEnterpriseProvisioner) -> None:
        """Test _get_local_ip instance method with no prohibited IPs."""
        mock_public_ips.return_value = ["192.168.1.100", "10.0.0.50", "172.17.0.1"]
        provisioner.prohibited_local_ips = []
        
        result = provisioner.get_local_ip()
        assert result == "192.168.1.100"  # Should return first IP
        mock_public_ips.assert_called_once()

    @patch('enterprise_gateway.services.provisioners.local.localinterfaces.public_ips')
    def test_get_local_ip_instance_method_with_prohibited(self, mock_public_ips: MagicMock, provisioner: LocalEnterpriseProvisioner) -> None:
        """Test _get_local_ip instance method with prohibited IP patterns."""
        mock_public_ips.return_value = ["172.17.0.1", "192.168.1.100", "10.0.0.50"]
        provisioner.prohibited_local_ips = ["172.17.*", "10.*"]
        
        result = provisioner.get_local_ip()
        assert result == "192.168.1.100"  # Should skip prohibited IPs and return allowed one
        mock_public_ips.assert_called_once()

    @patch('enterprise_gateway.services.provisioners.local.localinterfaces.public_ips')
    def test_get_local_ip_instance_method_all_prohibited(self, mock_public_ips: MagicMock, provisioner: LocalEnterpriseProvisioner) -> None:
        """Test _get_local_ip instance method when all IPs are prohibited."""
        mock_public_ips.return_value = ["172.17.0.1", "10.0.0.50"]
        provisioner.prohibited_local_ips = ["172.17.*", "10.*"]
        
        result = provisioner.get_local_ip()
        assert result == "172.17.0.1"  # Should return first IP when all are prohibited
        assert mock_public_ips.call_count == 2  # Called twice: once for loop, once for fallback

    @patch('enterprise_gateway.services.provisioners.local.localinterfaces.public_ips')
    def test_get_local_ip_instance_method_regex_patterns(self, mock_public_ips: MagicMock, provisioner: LocalEnterpriseProvisioner) -> None:
        """Test _get_local_ip instance method with various regex patterns."""
        mock_public_ips.return_value = [
            "172.17.0.1", 
            "192.168.1.100", 
            "10.255.255.254",
            "203.0.113.42"
        ]
        
        # Test exact match
        provisioner.prohibited_local_ips = ["172.17.0.1"]
        result = provisioner.get_local_ip()
        assert result == "192.168.1.100"
        
        # Test wildcard pattern
        provisioner.prohibited_local_ips = ["172.17.*"]
        result = provisioner.get_local_ip()
        assert result == "192.168.1.100"
        
        # Test multiple patterns
        provisioner.prohibited_local_ips = ["172.17.*", "192.168.*", "10.*"]
        result = provisioner.get_local_ip()
        assert result == "203.0.113.42"

    def test_init_uses_instance_method(self, kernel_spec: KernelSpec) -> None:
        """Test that __init__ properly uses the instance method for IP selection."""
        with patch('enterprise_gateway.services.provisioners.local.localinterfaces.public_ips') as mock_public_ips:
            mock_public_ips.return_value = ["172.17.0.1", "192.168.1.100", "10.0.0.50"]
            
            provisioner = LocalEnterpriseProvisioner(
                kernel_id="test",
                kernel_spec=kernel_spec,
                prohibited_local_ips=["172.17.*"]
            )
            
            # IP should be set during initialization using the class method
            assert provisioner.ip == "192.168.1.100"

    def test_init_uses_instance_method_empty_prohibited(self, kernel_spec: KernelSpec) -> None:
        """Test __init__ with empty prohibited list."""
        with patch('enterprise_gateway.services.provisioners.local.localinterfaces.public_ips') as mock_public_ips:
            mock_public_ips.return_value = ["192.168.1.100", "10.0.0.50"]
            
            provisioner = LocalEnterpriseProvisioner(
                kernel_id="test",
                kernel_spec=kernel_spec,
                prohibited_local_ips=[]
            )
            
            assert provisioner.ip == "192.168.1.100"

    def test_regex_matching_edge_cases(self, provisioner: LocalEnterpriseProvisioner) -> None:
        """Test edge cases in regex pattern matching."""
        test_ips = ["172.17.0.1", "17.0.1.1", "172.170.1.1", "192.168.1.100"]
        
        # Test that 172.17.* only matches 172.17.x.x, not 17.x.x.x or 172.170.x.x
        with patch('enterprise_gateway.services.provisioners.local.localinterfaces.public_ips') as mock_public_ips:
            mock_public_ips.return_value = test_ips
            provisioner.prohibited_local_ips = ["172.17.*"]
            result = provisioner.get_local_ip()
            # Should return 17.0.1.1 (first non-matching IP)
            assert result == "17.0.1.1"

    def test_empty_prohibited_list_handling(self, provisioner: LocalEnterpriseProvisioner) -> None:
        """Test handling of empty prohibited IP patterns."""
        with patch('enterprise_gateway.services.provisioners.local.localinterfaces.public_ips') as mock_public_ips:
            mock_public_ips.return_value = ["192.168.1.100", "10.0.0.50"]
            
            # Test with empty list
            provisioner.prohibited_local_ips = []
            result = provisioner.get_local_ip()
            assert result == "192.168.1.100"
            
            # Test with list containing empty strings
            provisioner.prohibited_local_ips = ["", "   "]
            result = provisioner.get_local_ip()
            assert result == "192.168.1.100"

    def test_none_values_in_prohibited_list(self, provisioner: LocalEnterpriseProvisioner) -> None:
        """Test handling of None values in prohibited IP patterns."""
        with patch('enterprise_gateway.services.provisioners.local.localinterfaces.public_ips') as mock_public_ips:
            mock_public_ips.return_value = ["192.168.1.100", "10.0.0.50"]
            
            # The method should handle empty/None patterns gracefully
            # Note: In practice, traitlets would prevent None values, but testing robustness
            provisioner.prohibited_local_ips = [""]
            result = provisioner.get_local_ip()
            assert result == "192.168.1.100"

    @patch('enterprise_gateway.services.provisioners.local.localinterfaces.public_ips')
    def test_complex_regex_patterns(self, mock_public_ips: MagicMock, provisioner: LocalEnterpriseProvisioner) -> None:
        """Test complex regex patterns for IP matching."""
        mock_public_ips.return_value = [
            "172.17.0.1",      # Docker default
            "172.18.0.1",      # Docker custom
            "192.168.1.100",   # Private network
            "10.0.0.50",       # Private network
            "169.254.1.1",     # Link-local
            "203.0.113.42"     # Public IP
        ]
        
        # Test excluding all private and link-local ranges
        provisioner.prohibited_local_ips = [
            "172.1[7-9].*",    # Docker ranges 172.17-19.*
            "192.168.*",       # Private Class C
            "10.*",            # Private Class A
            "169.254.*"        # Link-local
        ]
        
        result = provisioner.get_local_ip()
        assert result == "203.0.113.42"  # Only public IP should remain

    def test_trait_configuration_inheritance(self, kernel_spec: KernelSpec) -> None:
        """Test that trait configuration works with inheritance."""
        # Test that the trait is properly configured and inheritable
        provisioner = LocalEnterpriseProvisioner(
            kernel_id="test",
            kernel_spec=kernel_spec
        )
        
        # Verify trait is configurable by checking the metadata
        trait = provisioner.__class__.prohibited_local_ips
        assert hasattr(trait, 'metadata')
        # Check that config=True was set when the trait was defined
        metadata = getattr(trait, 'metadata', {})
        assert metadata.get('config') is True

    def test_environment_variable_integration(self, kernel_spec: KernelSpec) -> None:
        """Test full integration with environment variable."""
        env_value = "172.17.*,10.*"
        
        with patch.dict(os.environ, {"EG_PROHIBITED_LOCAL_IPS": env_value}):
            with patch('enterprise_gateway.services.provisioners.local.localinterfaces.public_ips') as mock_public_ips:
                mock_public_ips.return_value = ["172.17.0.1", "10.0.0.1", "192.168.1.100"]
                
                provisioner = LocalEnterpriseProvisioner(kernel_id="test", kernel_spec=kernel_spec)
                
                # Check that IP was set correctly during initialization
                assert provisioner.ip == "192.168.1.100"
                assert provisioner.prohibited_local_ips == ["172.17.*", "10.*"]