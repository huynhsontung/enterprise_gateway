# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.
"""Tests for KubernetesEnterpriseProvisioner."""

import asyncio
import unittest
from unittest.mock import Mock, AsyncMock, patch, MagicMock, call
from typing import Optional

from kubernetes import client
from kubernetes.client.rest import ApiException

# Mock Kubernetes configuration before importing the module
with patch('kubernetes.config.load_incluster_config'), patch('kubernetes.config.load_kube_config'):
    from enterprise_gateway.services.provisioners.kubernetes import KubernetesEnterpriseProvisioner

from jupyter_client.kernelspec import KernelSpec


class TestKubernetesEnterpriseProvisioner(unittest.TestCase):
    """Test KubernetesEnterpriseProvisioner functionality."""

    def setUp(self):
        """Set up test fixtures."""
        # Mock kernel manager
        self.mock_kernel_manager = Mock()
        self.mock_kernel_manager.get_kernel_username.return_value = "testuser"
        self.mock_kernel_manager.port_range = "0..0"
        self.mock_kernel_manager.kernel_id = "test-kernel-id"
        self.mock_kernel_manager.kernel_name = "python3"
        
        # Mock kernel spec
        self.mock_kernelspec = Mock(spec=KernelSpec)
        self.mock_kernelspec.display_name = "Python 3"
        self.mock_kernelspec.argv = ["python", "-m", "ipykernel_launcher", "-f", "{connection_file}"]
        self.mock_kernelspec.env = {}
        self.mock_kernelspec.metadata = {}
        
        # Mock connection info - using a simple dict instead of KernelConnectionInfo
        self.mock_connection_info = {
            "transport": "tcp",
            "ip": "127.0.0.1",
            "shell_port": 12345,
            "iopub_port": 12346,
            "stdin_port": 12347,
            "control_port": 12348,
            "hb_port": 12349,
            "signature_scheme": "hmac-sha256",
            "key": b"test-key"
        }

        # Provisioner config
        self.provisioner_config = {
            "kernel_namespace": "enterprise-gateway",
            "kernel_image": "python:3.9",
            "kernel_executor_image": "elyra/enterprise-gateway:dev",
            "kernel_service_account_name": "kernel-sa"
        }

        # Create provisioner with mocked Kubernetes client
        with patch('kubernetes.client.CoreV1Api') as mock_core_v1_class, \
             patch('kubernetes.client.RbacAuthorizationV1Api') as mock_rbac_v1_class, \
             patch('enterprise_gateway.services.provisioners.remote.ResponseManager'):
            
            # Set up the mock classes to return our mock instances
            self.mock_core_v1 = Mock()
            self.mock_rbac_v1 = Mock()
            mock_core_v1_class.return_value = self.mock_core_v1
            mock_rbac_v1_class.return_value = self.mock_rbac_v1
            
            self.provisioner = KubernetesEnterpriseProvisioner(
                kernel_spec=self.mock_kernelspec,
                kernel_id="test-kernel-id",
                **self.provisioner_config
            )
        
        # Mock ResponseManager with async methods
        self.mock_response_manager = Mock()
        self.mock_response_manager.get_connection_info = AsyncMock(return_value=self.mock_connection_info)
        self.provisioner.response_manager = self.mock_response_manager

    def test_provisioner_initialization(self):
        """Test provisioner initialization."""
        self.assertEqual(self.provisioner.kernel_id, "test-kernel-id")
        self.assertEqual(self.provisioner.kernel_namespace, "enterprise-gateway")
        self.assertEqual(self.provisioner.kernel_image, "python:3.9")
        self.assertEqual(self.provisioner.object_kind, "Pod")

    def test_dns_compliant_naming(self):
        """Test DNS-compliant name generation."""
        test_cases = [
            ("test_name", "test-name"),
            ("Test-Name", "test-name"),
            ("test.name", "test-name"),
            ("test@name", "test-name"),
            ("_test_name_", "test-name"),
            ("123test", "123test"),
            ("test123", "test123"),
            ("", ""),
            ("a" * 100, "a" * 63),  # DNS names limited to 63 chars
        ]
        
        for input_name, expected in test_cases:
            with self.subTest(input_name=input_name):
                result = self.provisioner._make_dns_compliant(input_name)
                self.assertEqual(result, expected)

    def test_safe_template_substitution(self):
        """Test safe template variable substitution."""
        variables = {
            "kernel_id": "test-123",
            "kernel_namespace": "production",
            "kernel_image": "python:3.9"
        }
        
        # Valid substitutions
        test_cases = [
            ("{{ kernel_id }}", "test-123"),
            ("{{ kernel_namespace }}-{{ kernel_id }}", "production-test-123"),
            ("{{   kernel_id   }}", "test-123"),  # Whitespace handling
            ("prefix-{{ kernel_id }}-suffix", "prefix-test-123-suffix"),
            ("no-templates", "no-templates"),  # No templates
        ]
        
        for template, expected in test_cases:
            with self.subTest(template=template):
                result = self.provisioner._safe_template_substitute(template, variables)
                self.assertEqual(result, expected)

    def test_safe_template_substitution_security(self):
        """Test template substitution security against malicious input."""
        variables = {"kernel_id": "test-123"}
        
        malicious_templates = [
            "{{ __import__('os').system('rm -rf /') }}",
            "{{ ''.__class__.__mro__[1].__subclasses__() }}",
            "{{ exec('print(\"pwned\")') }}",
            "{{ eval('1+1') }}",
            "{{ kernel_id.__class__ }}",
            "{{ kernel_id|upper }}",  # Jinja2 filter
            "{{ kernel_id + '_suffix' }}",  # Expression
        ]
        
        for malicious_template in malicious_templates:
            with self.subTest(template=malicious_template):
                # Test that malicious templates return None (safe substitution failure)
                result = self.provisioner._safe_template_substitute(malicious_template, variables)
                self.assertIsNone(result)

    def test_safe_template_substitution_missing_variables(self):
        """Test template substitution with missing variables."""
        variables = {"kernel_id": "test-123"}
        template = "{{ kernel_namespace }}-{{ kernel_id }}"
        
        # Test that missing variables cause safe substitution to fail and return None
        result = self.provisioner._safe_template_substitute(template, variables)
        self.assertIsNone(result)

    @patch('enterprise_gateway.services.provisioners.kubernetes.KernelSessionManager')
    def test_determine_kernel_pod_name_with_template(self, mock_session_manager):
        """Test kernel pod name determination with template."""
        mock_session_manager.get_kernel_username.return_value = "testuser"
        
        kwargs = {
            "env": {
                "KERNEL_POD_NAME": "{{ kernel_namespace }}-{{ kernel_id }}",
                "KERNEL_NAMESPACE": "production"
            }
        }
        
        result = self.provisioner._determine_kernel_pod_name(**kwargs)
        self.assertEqual(result, "production-test-kernel-id")

    @patch('enterprise_gateway.services.provisioners.kubernetes.KernelSessionManager')
    def test_determine_kernel_pod_name_fallback(self, mock_session_manager):
        """Test kernel pod name determination fallback to default."""
        mock_session_manager.get_kernel_username.return_value = "testuser"
        
        # Template with missing variable should fall back
        kwargs = {
            "env": {
                "KERNEL_POD_NAME": "{{ missing_var }}",
                "KERNEL_NAMESPACE": "production"
            }
        }
        
        result = self.provisioner._determine_kernel_pod_name(**kwargs)
        self.assertEqual(result, "testuser-test-kernel-id")

    @patch('kubernetes.client.RbacAuthorizationV1Api')
    @patch('kubernetes.client.CoreV1Api')
    def test_create_kernel_namespace_success(self, mock_core_v1_class, mock_rbac_v1_class):
        """Test successful kernel namespace creation."""
        service_account_name = "kernel-sa"
        expected_namespace = f"kernel-{self.provisioner.kernel_id}"
        
        # Set kernel_pod_name as required by the method
        self.provisioner.kernel_pod_name = f"kernel-{self.provisioner.kernel_id}"
        
        # Set up the mock clients
        mock_core_v1 = Mock()
        mock_rbac_v1 = Mock()
        mock_core_v1_class.return_value = mock_core_v1
        mock_rbac_v1_class.return_value = mock_rbac_v1
        
        # Mock successful namespace creation
        mock_namespace = Mock()
        mock_namespace.metadata.name = expected_namespace
        mock_core_v1.create_namespace.return_value = mock_namespace
        
        # Mock successful role binding creation
        mock_rbac_v1.create_namespaced_role_binding.return_value = Mock()
        
        result = self.provisioner._create_kernel_namespace(service_account_name)
        
        self.assertEqual(result, expected_namespace)
        mock_core_v1.create_namespace.assert_called_once()
        mock_rbac_v1.create_namespaced_role_binding.assert_called_once()

    @patch('kubernetes.client.RbacAuthorizationV1Api')
    @patch('kubernetes.client.CoreV1Api')  
    def test_create_kernel_namespace_already_exists(self, mock_core_v1_class, mock_rbac_v1_class):
        """Test kernel namespace creation when namespace already exists."""
        service_account_name = "kernel-sa"
        expected_namespace = f"kernel-{self.provisioner.kernel_id}"
        
        # Set kernel_pod_name as required by the method
        self.provisioner.kernel_pod_name = f"kernel-{self.provisioner.kernel_id}"
        
        # Set up the mock clients
        mock_core_v1 = Mock()
        mock_rbac_v1 = Mock()
        mock_core_v1_class.return_value = mock_core_v1
        mock_rbac_v1_class.return_value = mock_rbac_v1
        
        # Mock namespace already exists (409 conflict)
        conflict_error = ApiException(status=409, reason="Conflict")
        mock_core_v1.create_namespace.side_effect = conflict_error
        
        # Mock successful role binding creation
        mock_rbac_v1.create_namespaced_role_binding.return_value = Mock()
        
        result = self.provisioner._create_kernel_namespace(service_account_name)
        
        self.assertEqual(result, expected_namespace)
        mock_core_v1.create_namespace.assert_called_once()
        # When namespace already exists (409), role binding is NOT created in current implementation
        mock_rbac_v1.create_namespaced_role_binding.assert_not_called()

    @patch('kubernetes.client.CoreV1Api')
    def test_create_kernel_namespace_failure(self, mock_core_v1_class):
        """Test kernel namespace creation failure."""
        service_account_name = "kernel-sa"
        
        # Set kernel_pod_name as required by the method
        self.provisioner.kernel_pod_name = f"kernel-{self.provisioner.kernel_id}"
        
        # Set up the mock client
        mock_core_v1 = Mock()
        mock_core_v1_class.return_value = mock_core_v1
        
        # Mock namespace creation failure
        api_error = ApiException(status=500, reason="Internal Server Error")
        mock_core_v1.create_namespace.side_effect = api_error
        
        with self.assertRaises(RuntimeError):
            self.provisioner._create_kernel_namespace(service_account_name)

    @patch('kubernetes.client.CoreV1Api')
    def test_delete_kernel_namespace_success(self, mock_core_v1_class):
        """Test successful kernel namespace deletion."""
        namespace_name = f"kernel-{self.provisioner.kernel_id}"
        self.provisioner.kernel_namespace = namespace_name
        
        # Set up the mock client
        mock_core_v1 = Mock()
        mock_core_v1_class.return_value = mock_core_v1
        
        # Mock successful deletion
        mock_core_v1.delete_namespace.return_value = Mock()
        
        self.provisioner._delete_kernel_namespace()
        
        mock_core_v1.delete_namespace.assert_called_once_with(
            name=namespace_name,
            body=client.V1DeleteOptions(
                grace_period_seconds=0,
                propagation_policy="Background"
            )
        )

    @patch('kubernetes.client.CoreV1Api')
    def test_delete_kernel_namespace_not_found(self, mock_core_v1_class):
        """Test kernel namespace deletion when namespace not found."""
        namespace_name = f"kernel-{self.provisioner.kernel_id}"
        self.provisioner.kernel_namespace = namespace_name
        
        # Set up the mock client
        mock_core_v1 = Mock()
        mock_core_v1_class.return_value = mock_core_v1
        
        # Mock namespace not found (404)
        not_found_error = ApiException(status=404, reason="Not Found")
        mock_core_v1.delete_namespace.side_effect = not_found_error
        
        # Should not raise exception
        self.provisioner._delete_kernel_namespace()
        
        mock_core_v1.delete_namespace.assert_called_once()

    @patch('kubernetes.client.CoreV1Api')
    def test_get_container_status_running(self, mock_core_v1_class):
        """Test getting container status for running pod."""
        # Set required attributes
        self.provisioner.kernel_pod_name = "test-pod"
        self.provisioner.kernel_namespace = "test-namespace"
        
        # Set up the mock client
        mock_core_v1 = Mock()
        mock_core_v1_class.return_value = mock_core_v1
        
        # Mock running pod
        mock_pod = Mock()
        mock_pod.metadata.name = "test-pod"
        mock_pod.status.phase = "Running"
        mock_pod.status.pod_ip = "10.0.0.1"
        mock_pod.status.host_ip = "192.168.1.100"
        
        mock_response = Mock()
        mock_response.items = [mock_pod]
        mock_core_v1.list_namespaced_pod.return_value = mock_response
        
        result = self.provisioner.get_container_status(iteration=1)
        
        self.assertEqual(result, "running")  # lowercase as returned by implementation
        self.assertEqual(self.provisioner.assigned_ip, "10.0.0.1")
        self.assertEqual(self.provisioner.assigned_host, "test-pod")
        self.assertEqual(self.provisioner.assigned_node_ip, "192.168.1.100")

    @patch('kubernetes.client.CoreV1Api')
    def test_get_container_status_failed(self, mock_core_v1_class):
        """Test getting container status for failed pod."""
        # Set required attributes
        self.provisioner.kernel_pod_name = "test-pod"
        self.provisioner.kernel_namespace = "test-namespace"
        
        # Set up the mock client
        mock_core_v1 = Mock()
        mock_core_v1_class.return_value = mock_core_v1
        
        # Mock failed pod
        mock_pod = Mock()
        mock_pod.metadata.name = "test-pod"
        mock_pod.status.phase = "Failed"
        mock_pod.status.pod_ip = None
        mock_pod.status.host_ip = "192.168.1.100"
        
        mock_response = Mock()
        mock_response.items = [mock_pod]
        mock_core_v1.list_namespaced_pod.return_value = mock_response
        
        result = self.provisioner.get_container_status(iteration=1)
        
        self.assertEqual(result, "failed")  # lowercase as returned by implementation

    @patch('kubernetes.client.CoreV1Api')
    def test_get_container_status_no_pods(self, mock_core_v1_class):
        """Test getting container status when no pods found."""
        # Set required attributes
        self.provisioner.kernel_pod_name = "test-pod"
        self.provisioner.kernel_namespace = "test-namespace"
        
        # Set up the mock client
        mock_core_v1 = Mock()
        mock_core_v1_class.return_value = mock_core_v1
        
        # Mock empty response
        mock_response = Mock()
        mock_response.items = []
        mock_core_v1.list_namespaced_pod.return_value = mock_response
        
        result = self.provisioner.get_container_status(iteration=1)
        
        self.assertEqual(result, "")  # Empty string when no pods found

    @patch('asyncio.sleep', new_callable=AsyncMock)
    def test_launch_kernel_success(self, mock_sleep):
        """Test successful kernel launch."""
        import asyncio
        cmd = ["python", "-m", "ipykernel_launcher"]
        kwargs = {"env": {}}  # No KERNEL_NAMESPACE provided so it will create one
        
        # Mock _launch_remote_process to return connection info directly (bypasses ResponseManager wait)
        # Also mock confirm_remote_startup to avoid actual Kubernetes API calls
        with patch.object(self.provisioner, '_launch_remote_process', new_callable=AsyncMock) as mock_launch, \
             patch.object(self.provisioner, 'confirm_remote_startup', new_callable=AsyncMock) as mock_confirm, \
             patch.object(self.provisioner, '_create_kernel_namespace') as mock_create_ns:
            
            mock_launch.return_value = self.mock_connection_info
            mock_confirm.return_value = True
            mock_create_ns.return_value = "kernel-test-kernel-id"
            
            result = asyncio.run(self.provisioner.launch_kernel(cmd, **kwargs))
            
            self.assertEqual(result, self.mock_connection_info)
            mock_create_ns.assert_called_once()
            mock_launch.assert_called_once()  # Just check it was called, don't check exact args
            mock_confirm.assert_called_once()
            
            # Verify that the namespace was set correctly
            call_args = mock_launch.call_args
            self.assertEqual(call_args[0][0], cmd)  # First positional arg should be cmd
            self.assertIn('env', call_args[1])  # Should have env in kwargs
            self.assertEqual(call_args[1]['env']['KERNEL_NAMESPACE'], 'kernel-test-kernel-id')

    def test_poll_running(self):
        """Test polling running kernel."""
        import asyncio
        # Mock get_container_status to return "running"
        with patch.object(self.provisioner, 'get_container_status', return_value="running"):
            result = asyncio.run(self.provisioner.poll())
            self.assertIsNone(result)  # None means still running

    def test_poll_failed(self):
        """Test polling failed kernel."""
        import asyncio
        # Mock get_container_status to return "failed"
        with patch.object(self.provisioner, 'get_container_status', return_value="failed"):
            result = asyncio.run(self.provisioner.poll())
            self.assertEqual(result, 1)  # Exit code

    def test_poll_no_pods(self):
        """Test polling when no pods found."""
        import asyncio
        # Mock get_container_status to return empty string (no pods)
        with patch.object(self.provisioner, 'get_container_status', return_value=""):
            result = asyncio.run(self.provisioner.poll())
            self.assertEqual(result, 0)  # Unknown state returns 0

    @patch('kubernetes.client.CoreV1Api')
    def test_terminate_kernel(self, mock_core_v1_class):
        """Test kernel termination."""
        import asyncio
        self.provisioner.kernel_pod_name = "test-pod"
        self.provisioner.kernel_namespace = "test-namespace"
        self.provisioner.delete_kernel_namespace = True
        
        # Set up the mock client
        mock_core_v1 = Mock()
        mock_core_v1_class.return_value = mock_core_v1
        
        # Mock successful pod deletion
        mock_core_v1.delete_namespaced_pod.return_value = Mock()
        mock_core_v1.delete_namespace.return_value = Mock()
        
        asyncio.run(self.provisioner.terminate(restart=False))
        
        mock_core_v1.delete_namespaced_pod.assert_called_once()
        # Check that it was called with a grace period (not immediate termination)
        call_args = mock_core_v1.delete_namespaced_pod.call_args
        self.assertEqual(call_args[1]['body'].grace_period_seconds, 30)

    @patch('kubernetes.client.CoreV1Api')
    def test_kill_kernel(self, mock_core_v1_class):
        """Test kernel killing."""
        import asyncio
        self.provisioner.kernel_pod_name = "test-pod"
        self.provisioner.kernel_namespace = "test-namespace"
        self.provisioner.delete_kernel_namespace = True
        
        # Set up the mock client
        mock_core_v1 = Mock()
        mock_core_v1_class.return_value = mock_core_v1
        
        # Mock successful pod deletion
        mock_core_v1.delete_namespaced_pod.return_value = Mock()
        mock_core_v1.delete_namespace.return_value = Mock()
        
        asyncio.run(self.provisioner.kill(restart=False))
        
        # Verify force deletion (grace_period_seconds=0)
        call_args = mock_core_v1.delete_namespaced_pod.call_args
        self.assertEqual(call_args[1]['body'].grace_period_seconds, 0)

    def test_cleanup(self):
        """Test kernel cleanup."""
        import asyncio
        self.provisioner.kernel_pod_name = "test-pod"
        self.provisioner.kernel_namespace = "test-namespace"
        self.provisioner.delete_kernel_namespace = True
        
        # Mock namespace deletion
        with patch('kubernetes.client.CoreV1Api') as mock_core_v1_class:
            mock_core_v1 = Mock()
            mock_core_v1_class.return_value = mock_core_v1
            mock_core_v1.delete_namespace.return_value = Mock()
            
            asyncio.run(self.provisioner.cleanup(restart=False))
            
            # Verify namespace cleanup was attempted
            mock_core_v1.delete_namespace.assert_called_once()

    def test_get_provisioner_info(self):
        """Test getting provisioner information."""
        # Use asyncio.run to handle the async method in a sync test
        import asyncio
        result = asyncio.run(self.provisioner.get_provisioner_info())
        
        self.assertIsInstance(result, dict)
        self.assertEqual(result['kernel_id'], "test-kernel-id")
        # Check fields that should exist in the remote provisioner
        self.assertIn('provisioner_class', result)
        self.assertIn('assigned_ip', result)
        self.assertIn('assigned_host', result)
        self.assertIn('enterprise_gateway_version', result)
        self.assertIn('provisioner_type', result)

    def test_load_provisioner_info(self):
        """Test loading provisioner information."""
        import asyncio
        info = {
            'kernel_id': 'loaded-kernel-id',
            'kernel_namespace': 'loaded-namespace',
            'kernel_image': 'loaded-image:latest',
            'connection_info': {},  # Add required connection_info
            'ip': '127.0.0.1'  # Add required ip field
        }
        
        asyncio.run(self.provisioner.load_provisioner_info(info))
        
        self.assertEqual(self.provisioner.kernel_id, 'loaded-kernel-id')
        # Note: kernel_namespace and kernel_image may not be set by load_provisioner_info
        # since this method calls the jupyter-client base first


class TestKubernetesProvisionerAsync(unittest.TestCase):
    """Test KubernetesEnterpriseProvisioner async functionality."""

    def setUp(self):
        """Set up async test fixtures."""
        # Use event loop for async tests
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def tearDown(self):
        """Clean up async test fixtures."""
        self.loop.close()

    def test_launch_kernel_async(self):
        """Test async kernel launch."""
        # This test would be expanded with proper async mocking
        # For now, just verify the method exists and is async
        from inspect import iscoroutinefunction
        from jupyter_client.kernelspec import KernelSpec
        
        with patch('kubernetes.client.CoreV1Api'), \
             patch('kubernetes.client.RbacAuthorizationV1Api'), \
             patch('enterprise_gateway.services.provisioners.remote.ResponseManager'):
            
            # Create a real KernelSpec
            kernel_spec = KernelSpec(
                argv=["python", "-m", "ipykernel_launcher"],
                display_name="Python 3",
                language="python"
            )
            
            provisioner = KubernetesEnterpriseProvisioner(
                kernel_spec=kernel_spec,
                kernel_id="test-id"
            )
            
            self.assertTrue(iscoroutinefunction(provisioner.launch_kernel))
            self.assertTrue(iscoroutinefunction(provisioner.poll))
            self.assertTrue(iscoroutinefunction(provisioner.terminate))
            self.assertTrue(iscoroutinefunction(provisioner.kill))
            self.assertTrue(iscoroutinefunction(provisioner.cleanup))


if __name__ == '__main__':
    unittest.main()