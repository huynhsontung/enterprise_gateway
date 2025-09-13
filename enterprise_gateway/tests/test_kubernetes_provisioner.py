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
    from enterprise_gateway.services.provisioners.factory import (
        create_provisioner_for_kernelspec,
        convert_process_proxy_to_provisioner_config
    )

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
        with patch('kubernetes.client.CoreV1Api'), \
             patch('kubernetes.client.RbacAuthorizationV1Api'), \
             patch('enterprise_gateway.services.provisioners.remote.ResponseManager'):
            
            self.provisioner = KubernetesEnterpriseProvisioner(
                kernel_spec=self.mock_kernelspec,
                kernel_id="test-kernel-id",
                **self.provisioner_config
            )
            
        # Mock Kubernetes clients
        self.mock_core_v1 = Mock(spec=client.CoreV1Api)
        self.mock_rbac_v1 = Mock(spec=client.RbacAuthorizationV1Api)
        self.provisioner.core_v1_api = self.mock_core_v1
        self.provisioner.rbac_v1_api = self.mock_rbac_v1
        
        # Mock ResponseManager
        self.mock_response_manager = Mock()
        self.provisioner.response_manager = self.mock_response_manager

    def test_provisioner_initialization(self):
        """Test provisioner initialization."""
        self.assertEqual(self.provisioner.kernel_id, "test-kernel-id")
        self.assertEqual(self.provisioner.kernel_namespace, "enterprise-gateway")
        self.assertEqual(self.provisioner.kernel_image, "python:3.9")
        self.assertEqual(self.provisioner.object_kind, "Pod")
        self.assertIsNotNone(self.provisioner.core_v1_api)
        self.assertIsNotNone(self.provisioner.rbac_v1_api)

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
                with patch.object(self.provisioner, 'log') as mock_log:
                    result = self.provisioner._safe_template_substitute(malicious_template, variables)
                    self.assertIsNone(result)
                    mock_log.warning.assert_called_once()

    def test_safe_template_substitution_missing_variables(self):
        """Test template substitution with missing variables."""
        variables = {"kernel_id": "test-123"}
        template = "{{ kernel_namespace }}-{{ kernel_id }}"
        
        with patch.object(self.provisioner, 'log') as mock_log:
            result = self.provisioner._safe_template_substitute(template, variables)
            self.assertIsNone(result)
            mock_log.warning.assert_called_once()
            self.assertIn("missing variables", mock_log.warning.call_args[0][0])

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

    def test_create_kernel_namespace_success(self):
        """Test successful kernel namespace creation."""
        service_account_name = "kernel-sa"
        expected_namespace = f"kernel-{self.provisioner.kernel_id}"
        
        # Mock successful namespace creation
        mock_namespace = Mock()
        mock_namespace.metadata.name = expected_namespace
        self.mock_core_v1.create_namespace.return_value = mock_namespace
        
        # Mock successful role binding creation
        self.mock_rbac_v1.create_namespaced_role_binding.return_value = Mock()
        
        result = self.provisioner._create_kernel_namespace(service_account_name)
        
        self.assertEqual(result, expected_namespace)
        self.mock_core_v1.create_namespace.assert_called_once()
        self.mock_rbac_v1.create_namespaced_role_binding.assert_called_once()

    def test_create_kernel_namespace_already_exists(self):
        """Test kernel namespace creation when namespace already exists."""
        service_account_name = "kernel-sa"
        expected_namespace = f"kernel-{self.provisioner.kernel_id}"
        
        # Mock namespace already exists (409 conflict)
        conflict_error = ApiException(status=409, reason="Conflict")
        self.mock_core_v1.create_namespace.side_effect = conflict_error
        
        # Mock successful role binding creation
        self.mock_rbac_v1.create_namespaced_role_binding.return_value = Mock()
        
        result = self.provisioner._create_kernel_namespace(service_account_name)
        
        self.assertEqual(result, expected_namespace)
        self.mock_core_v1.create_namespace.assert_called_once()
        self.mock_rbac_v1.create_namespaced_role_binding.assert_called_once()

    def test_create_kernel_namespace_failure(self):
        """Test kernel namespace creation failure."""
        service_account_name = "kernel-sa"
        
        # Mock namespace creation failure
        api_error = ApiException(status=500, reason="Internal Server Error")
        self.mock_core_v1.create_namespace.side_effect = api_error
        
        with self.assertRaises(ApiException):
            self.provisioner._create_kernel_namespace(service_account_name)

    def test_delete_kernel_namespace_success(self):
        """Test successful kernel namespace deletion."""
        namespace_name = f"kernel-{self.provisioner.kernel_id}"
        
        # Mock successful deletion
        self.mock_core_v1.delete_namespace.return_value = Mock()
        
        self.provisioner._delete_kernel_namespace(namespace_name)
        
        self.mock_core_v1.delete_namespace.assert_called_once_with(
            name=namespace_name,
            body=client.V1DeleteOptions(grace_period_seconds=0)
        )

    def test_delete_kernel_namespace_not_found(self):
        """Test kernel namespace deletion when namespace not found."""
        namespace_name = f"kernel-{self.provisioner.kernel_id}"
        
        # Mock namespace not found (404)
        not_found_error = ApiException(status=404, reason="Not Found")
        self.mock_core_v1.delete_namespace.side_effect = not_found_error
        
        # Should not raise exception
        self.provisioner._delete_kernel_namespace(namespace_name)
        
        self.mock_core_v1.delete_namespace.assert_called_once()

    def test_get_container_status_running(self):
        """Test getting container status for running pod."""
        # Mock running pod
        mock_pod = Mock()
        mock_pod.status.phase = "Running"
        mock_pod.status.pod_ip = "10.0.0.1"
        mock_pod.spec.node_name = "worker-node-1"
        
        mock_response = Mock()
        mock_response.items = [mock_pod]
        self.mock_core_v1.list_namespaced_pod.return_value = mock_response
        
        result = self.provisioner.get_container_status(iteration=1)
        
        self.assertEqual(result, "Running")
        self.assertEqual(self.provisioner.container_ip, "10.0.0.1")
        self.assertEqual(self.provisioner.container_host, "worker-node-1")

    def test_get_container_status_failed(self):
        """Test getting container status for failed pod."""
        # Mock failed pod
        mock_pod = Mock()
        mock_pod.status.phase = "Failed"
        mock_pod.status.pod_ip = None
        mock_pod.spec.node_name = "worker-node-1"
        
        mock_response = Mock()
        mock_response.items = [mock_pod]
        self.mock_core_v1.list_namespaced_pod.return_value = mock_response
        
        result = self.provisioner.get_container_status(iteration=1)
        
        self.assertEqual(result, "Failed")
        self.assertIsNone(self.provisioner.container_ip)

    def test_get_container_status_no_pods(self):
        """Test getting container status when no pods found."""
        # Mock empty response
        mock_response = Mock()
        mock_response.items = []
        self.mock_core_v1.list_namespaced_pod.return_value = mock_response
        
        result = self.provisioner.get_container_status(iteration=1)
        
        self.assertEqual(result, "")

    @patch('asyncio.sleep', new_callable=AsyncMock)
    async def test_launch_kernel_success(self, mock_sleep):
        """Test successful kernel launch."""
        cmd = ["python", "-m", "ipykernel_launcher"]
        kwargs = {"env": {"KERNEL_NAMESPACE": "production"}}
        
        # Mock response manager launch
        self.mock_response_manager.launch_process = AsyncMock(return_value=self.mock_connection_info)
        
        # Mock namespace creation
        with patch.object(self.provisioner, '_create_kernel_namespace') as mock_create_ns:
            mock_create_ns.return_value = "kernel-test-kernel-id"
            
            result = await self.provisioner.launch_kernel(cmd, **kwargs)
            
            self.assertEqual(result, self.mock_connection_info)
            mock_create_ns.assert_called_once()
            self.mock_response_manager.launch_process.assert_called_once()

    async def test_poll_running(self):
        """Test polling running kernel."""
        # Mock running pod
        mock_pod = Mock()
        mock_pod.status.phase = "Running"
        mock_pod.status.container_statuses = [Mock(state=Mock(running=Mock()))]
        
        mock_response = Mock()
        mock_response.items = [mock_pod]
        self.mock_core_v1.list_namespaced_pod.return_value = mock_response
        
        result = await self.provisioner.poll()
        
        self.assertIsNone(result)  # None means still running

    async def test_poll_failed(self):
        """Test polling failed kernel."""
        # Mock failed pod
        mock_pod = Mock()
        mock_pod.status.phase = "Failed"
        mock_container_status = Mock()
        mock_container_status.state.terminated.exit_code = 1
        mock_pod.status.container_statuses = [mock_container_status]
        
        mock_response = Mock()
        mock_response.items = [mock_pod]
        self.mock_core_v1.list_namespaced_pod.return_value = mock_response
        
        result = await self.provisioner.poll()
        
        self.assertEqual(result, 1)  # Exit code

    async def test_poll_no_pods(self):
        """Test polling when no pods found."""
        # Mock empty response
        mock_response = Mock()
        mock_response.items = []
        self.mock_core_v1.list_namespaced_pod.return_value = mock_response
        
        result = await self.provisioner.poll()
        
        self.assertIsNone(result)

    async def test_terminate_kernel(self):
        """Test kernel termination."""
        self.provisioner.kernel_namespace_created = "kernel-test-kernel-id"
        
        # Mock successful pod deletion
        self.mock_core_v1.delete_namespaced_pod.return_value = Mock()
        
        # Mock namespace deletion
        with patch.object(self.provisioner, '_delete_kernel_namespace') as mock_delete_ns:
            await self.provisioner.terminate(restart=False)
            
            self.mock_core_v1.delete_namespaced_pod.assert_called_once()
            mock_delete_ns.assert_called_once_with("kernel-test-kernel-id")

    async def test_kill_kernel(self):
        """Test kernel killing."""
        self.provisioner.kernel_namespace_created = "kernel-test-kernel-id"
        
        # Mock successful pod deletion
        self.mock_core_v1.delete_namespaced_pod.return_value = Mock()
        
        # Mock namespace deletion
        with patch.object(self.provisioner, '_delete_kernel_namespace') as mock_delete_ns:
            await self.provisioner.kill(restart=False)
            
            # Verify force deletion (grace_period_seconds=0)
            call_args = self.mock_core_v1.delete_namespaced_pod.call_args
            self.assertEqual(call_args[1]['body'].grace_period_seconds, 0)
            mock_delete_ns.assert_called_once_with("kernel-test-kernel-id")

    async def test_cleanup(self):
        """Test kernel cleanup."""
        self.provisioner.kernel_namespace_created = "kernel-test-kernel-id"
        
        # Mock namespace deletion
        with patch.object(self.provisioner, '_delete_kernel_namespace') as mock_delete_ns:
            await self.provisioner.cleanup(restart=False)
            
            mock_delete_ns.assert_called_once_with("kernel-test-kernel-id")

    def test_get_provisioner_info(self):
        """Test getting provisioner information."""
        result = self.provisioner.get_provisioner_info()
        
        self.assertIsInstance(result, dict)
        self.assertEqual(result['kernel_id'], "test-kernel-id")
        self.assertEqual(result['object_kind'], "Pod")
        self.assertIn('kernel_namespace', result)
        self.assertIn('kernel_image', result)

    def test_load_provisioner_info(self):
        """Test loading provisioner information."""
        info = {
            'kernel_id': 'loaded-kernel-id',
            'kernel_namespace': 'loaded-namespace',
            'kernel_image': 'loaded-image:latest'
        }
        
        self.provisioner.load_provisioner_info(info)
        
        self.assertEqual(self.provisioner.kernel_id, 'loaded-kernel-id')
        self.assertEqual(self.provisioner.kernel_namespace, 'loaded-namespace')
        self.assertEqual(self.provisioner.kernel_image, 'loaded-image:latest')


class TestKubernetesProvisionerFactory(unittest.TestCase):
    """Test KubernetesEnterpriseProvisioner factory integration."""

    def test_factory_mapping(self):
        """Test factory correctly maps to KubernetesEnterpriseProvisioner."""
        from enterprise_gateway.services.provisioners.factory import PROVISIONER_NAME_TO_CLASS_MAP
        
        provisioner_class = PROVISIONER_NAME_TO_CLASS_MAP.get('kubernetes-enterprise-provisioner')
        self.assertEqual(provisioner_class, KubernetesEnterpriseProvisioner)

    def test_legacy_conversion(self):
        """Test legacy ProcessProxy to Provisioner conversion."""
        legacy_config = {
            'class_name': 'enterprise_gateway.services.processproxies.k8s.KubernetesProcessProxy',
            'config': {'namespace': 'production', 'image': 'python:3.9'}
        }
        
        converted = convert_process_proxy_to_provisioner_config(legacy_config)
        
        self.assertEqual(converted['provisioner_name'], 'kubernetes-enterprise-provisioner')
        self.assertEqual(converted['config']['namespace'], 'production')
        self.assertEqual(converted['config']['image'], 'python:3.9')

    def test_create_provisioner_for_kernelspec(self):
        """Test creating provisioner from kernelspec."""
        # Mock kernelspec with provisioner config
        mock_kernelspec = Mock(spec=KernelSpec)
        mock_kernelspec.metadata = {
            'process_proxy': {
                'class_name': 'enterprise_gateway.services.processproxies.k8s.KubernetesProcessProxy'
            }
        }

        # Mock Kubernetes clients during provisioner creation
        with patch('kubernetes.client.CoreV1Api'), \
             patch('kubernetes.client.RbacAuthorizationV1Api'), \
             patch('enterprise_gateway.services.provisioners.remote.ResponseManager'):
            
            provisioner = create_provisioner_for_kernelspec(
                kernelspec=mock_kernelspec,
                kernel_id="test-kernel-id"
            )
            
            self.assertIsInstance(provisioner, KubernetesEnterpriseProvisioner)
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
        
        with patch('kubernetes.client.CoreV1Api'), \
             patch('kubernetes.client.RbacAuthorizationV1Api'), \
             patch('enterprise_gateway.services.provisioners.remote.ResponseManager'):
            
            provisioner = KubernetesEnterpriseProvisioner(
                kernel_spec=Mock(),
                kernel_id="test-id"
            )
            
            self.assertTrue(iscoroutinefunction(provisioner.launch_kernel))
            self.assertTrue(iscoroutinefunction(provisioner.poll))
            self.assertTrue(iscoroutinefunction(provisioner.terminate))
            self.assertTrue(iscoroutinefunction(provisioner.kill))
            self.assertTrue(iscoroutinefunction(provisioner.cleanup))


if __name__ == '__main__':
    unittest.main()