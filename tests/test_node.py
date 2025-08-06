import unittest
from unittest.mock import Mock, patch
from node import NodeStatus, NodeDiscoverer
from kubernetes import client


class TestNodeDiscoverer(unittest.TestCase):
    def setUp(self):
        self.mock_v1 = Mock(spec=client.CoreV1Api)
        self.discoverer = NodeDiscoverer(self.mock_v1)

    def _create_mock_node(self, name):
        node = Mock(spec=client.V1Node)
        node.metadata = Mock()
        node.metadata.name = name
        return node

    def _create_mock_pod(self, name, namespace="default", node_name=None, phase="Running"):
        pod = Mock(spec=client.V1Pod)
        pod.metadata = Mock()
        pod.metadata.name = name
        pod.metadata.namespace = namespace
        pod.metadata.owner_references = []
        
        pod.spec = Mock()
        pod.spec.node_name = node_name
        
        pod.status = Mock()
        pod.status.phase = phase
        return pod

    @patch.object(NodeDiscoverer, '_list_nodes')
    @patch.object(NodeDiscoverer, '_nodes_with_active_pods')
    def test_get_nodes_with_status(self, mock_active_pods, mock_list_nodes):
        mock_nodes = [
            self._create_mock_node("node1"),
            self._create_mock_node("node2")
        ]
        mock_list_nodes.return_value = mock_nodes
        mock_active_pods.return_value = {"node2"}
        
        result = self.discoverer.get_nodes_with_status()
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].name, "node1")
        self.assertTrue(result[0].is_free)
        self.assertEqual(result[1].name, "node2")
        self.assertFalse(result[1].is_free)

    def test_nodes_with_active_pods_filters_system_namespaces(self):
        mock_pods = [
            self._create_mock_pod("user-pod", namespace="default", node_name="node1"),
            self._create_mock_pod("system-pod", namespace="kube-system", node_name="node2")
        ]
        
        mock_pod_list = Mock()
        mock_pod_list.items = mock_pods
        self.mock_v1.list_pod_for_all_namespaces.return_value = mock_pod_list
        
        result = self.discoverer._nodes_with_active_pods()
        
        self.assertEqual(result, {"node1"})

    def test_count_free_nodes(self):
        mock_nodes = [
            self._create_mock_node("node1"),
            self._create_mock_node("node2"),
            self._create_mock_node("node3")
        ]
        mock_pods = [
            self._create_mock_pod("pod1", node_name="node2")
        ]
        
        mock_node_list = Mock()
        mock_node_list.items = mock_nodes
        self.mock_v1.list_node.return_value = mock_node_list
        
        mock_pod_list = Mock()
        mock_pod_list.items = mock_pods
        self.mock_v1.list_pod_for_all_namespaces.return_value = mock_pod_list
        
        result = self.discoverer.count_free_nodes()
        
        self.assertEqual(result, 2)  # node1 and node3 are free
