import logging
from types import ModuleType
from typing import Any, Dict, List, Optional

from ray.autoscaler.command_runner import CommandRunnerInterface
from ray.autoscaler._private.slurm.empty_command_runner import EmptyCommandRunner

from ray.autoscaler._private.slurm.slurm_commands import (
    slurm_cancel_job,
    slurm_launch_head,
    slurm_launch_worker,
    slurm_get_node_name
)

from ray.autoscaler._private.cli_logger import cli_logger

import copy
import os

logger = logging.getLogger(__name__)

'''Ray-Slurm Interface

Created by Tingkai Liu (tingkai2@illinois.edu) on June 10, 2022
Using the node_provider.py template

node id: the slurm batch submission id for the node
node ip: the node name in slurm

Information needed for each node:
1. Slurm job id
2. State: running, pending, terminated (should just be deleted)
    Currently only have running state
3. Tag (set and read by updator)
4. IP (Slurm node name)

# TODO: Lock?
# FIXME: It seems like node_provider is not presistant. Should store node info to local file

'''

# Const
SLURM_STATE_RUNNING = 0
SLURM_STATE_PENDING = 1
SLURM_STATE_TERMINATED = 2

HEAD_NODE_ID_OUTSIDE_SLURM = "-1"

# Default values if not provided in the node config
HEAD_UNDER_SLURM = False
WORKER_UNDER_SLURM = True
DEFAULT_TEMP_FOLDER_NAME = "temp_script"

# Indices for node info
INFO_STATE_INDEX = 0
INFO_TAG_INDEX = 1
INFO_IP_INDEX = 2

class NodeProvider:
    """Interface for getting and returning nodes from a Cloud.

    **Important**: This is an INTERNAL API that is only exposed for the purpose
    of implementing custom node providers. It is not allowed to call into
    NodeProvider methods from any Ray package outside the autoscaler, only to
    define new implementations of NodeProvider for use with the "external" node
    provider option.

    NodeProviders are namespaced by the `cluster_name` parameter; they only
    operate on nodes within that namespace.

    Nodes may be in one of three states: {pending, running, terminated}. Nodes
    appear immediately once started by `create_node`, and transition
    immediately to terminated when `terminate_node` is called.
    """

    def __init__(self, provider_config: Dict[str, Any], cluster_name: str) -> None:
        # LTK's note: provider config only has the "provider section" in the yaml
        
        self.provider_config = provider_config
        self.cluster_name = cluster_name
        self._internal_ip_cache: Dict[str, str] = {} # node ip : node id
        # self._external_ip_cache: Dict[str, str] = {}

        # Create a folder to store modified scripts
        temp_folder_name = DEFAULT_TEMP_FOLDER_NAME
        if "temp_folder_name" in provider_config:
            temp_folder_name = provider_config["temp_folder_name"]
        if os.path.exists(temp_folder_name+"/"):
            os.system("rm -rf " + temp_folder_name)
        os.mkdir(temp_folder_name)
        
        self.temp_folder = temp_folder_name
        self.template_folder = provider_config["template_path"]

        self.head_started = False
        self.head_ip = ""
        self.gcs_port = ""
        self.ray_client_port = ""
        self.dashboard_port = ""

        # The insertion order for keys are maintained, so node[0] is still head
        self.lauched_nodes = {} # node_id : [state, tag, ip]


    def is_readonly(self) -> bool:
        """Returns whether this provider is readonly.

        Readonly node providers do not allow nodes to be created or terminated.
        """
        return False

    def non_terminated_nodes(self, tag_filters: Dict[str, str]) -> List[str]:
        """Return a list of node ids filtered by the specified tags dict.

        This list must not include terminated nodes. For performance reasons,
        providers are allowed to cache the result of a call to
        non_terminated_nodes() to serve single-node queries
        (e.g. is_running(node_id)). This means that non_terminate_nodes() must
        be called again to refresh results.

        Examples:
            >>> from ray.autoscaler.node_provider import NodeProvider
            >>> from ray.autoscaler.tags import TAG_RAY_NODE_KIND
            >>> provider = NodeProvider(...) # doctest: +SKIP
            >>> provider.non_terminated_nodes( # doctest: +SKIP
            ...     {TAG_RAY_NODE_KIND: "worker"})
            ["node-1", "node-2"]

        """
        ret = []
        for node, value in self.lauched_nodes.items():
            ok = True
            for k, v in tag_filters.items():
                if value[INFO_TAG_INDEX].get(k) != v:
                    ok = False
                    break
            if ok:
                ret.append(node)
        return ret

    def is_running(self, node_id: str) -> bool:
        """Return whether the specified node is running."""
        if node_id in self.lauched_nodes:
            return self.lauched_nodes[node_id][INFO_STATE_INDEX] == SLURM_STATE_RUNNING
        else:
            cli_logger.warning("Get is_running for non-existing node\n")
            return False

    def is_terminated(self, node_id: str) -> bool:
        """Return whether the specified node is terminated."""
        return node_id not in self.lauched_nodes

    def node_tags(self, node_id: str) -> Dict[str, str]:
        """Returns the tags of the given node (string dict)."""
        if node_id in self.lauched_nodes:
            return self.lauched_nodes[node_id][INFO_TAG_INDEX]
        else:
            cli_logger.warning("Get tags for non-existing node\n")
            return {}

    def external_ip(self, node_id: str) -> str:
        """Returns the external ip of the given node."""
        raise NotImplementedError("Must use internal IPs with slurm")

    def internal_ip(self, node_id: str) -> str:
        """Returns the internal ip (Ray ip) of the given node."""
        
        if node_id in self.lauched_nodes:
            return self.lauched_nodes[node_id][INFO_IP_INDEX]
        else:
            cli_logger.warning("Get internal ip for non-existing node\n")
            return {}

    def get_node_id(self, ip_address: str, use_internal_ip: bool = True) -> str:
        """Returns the node_id given an IP address.

        Assumes ip-address is unique per node.

        Args:
            ip_address: Address of node.
            use_internal_ip: Whether the ip address is
                public or private.

        Raises:
            ValueError if not found.
        """

        if not use_internal_ip:
            raise ValueError("Must use internal IPs with slurm")
        
        # Should success
        return self._internal_ip_cache[ip_address]

    def create_node( # TODO: add tags & counts
        self, node_config: Dict[str, Any], tags: Dict[str, str], count: int
    ) -> Optional[Dict[str, Any]]:
        """Creates a number of nodes within the namespace.

        Optionally returns a mapping from created node ids to node metadata.
        """

        # LTK's note: node_config only contains the "node_cofig" filed of a specific node type
        current_conf = copy.deepcopy(node_config)

        if "head_node" not in current_conf:
            raise ValueError("Must specify whether the node is head in the node config")
        
        is_head_node = current_conf["head_node"] == 1
        if is_head_node and count != 1:
            raise ValueError("Cannot create more than one head")
        under_slurm = False

        if "under_slurm" in current_conf:
            under_slurm = current_conf["under_slurm"] == 1
        else:
            if is_head_node:
                under_slurm = HEAD_UNDER_SLURM
            else:
                under_slurm = WORKER_UNDER_SLURM
        
        if under_slurm:
            parsed_init_command = ""
            for init in current_conf["init_commands"]:
                parsed_init_command += init + "\n"
            
            if is_head_node:
                node_id = slurm_launch_head(
                    self.template_folder,
                    self.temp_folder, 
                    current_conf["head_node_name"], 
                    current_conf["gcs_port"],
                    current_conf["ray_client_port"],
                    current_conf["dashboad_port"],
                    parsed_init_command
                )
                node_name = slurm_get_node_name(node_id)
                self.lauched_nodes[node_id] = [SLURM_STATE_RUNNING, tags, node_name]
                self._internal_ip_cache[node_name] = node_id
            else:
                for _ in range(count):
                    node_id = slurm_launch_worker(
                        self.template_folder,
                        self.temp_folder,
                        self.head_ip+":"+self.gcs_port,
                        parsed_init_command
                    )
                    node_name = slurm_get_node_name(node_id)
                    self.lauched_nodes[node_id] = [SLURM_STATE_RUNNING, tags, node_name]
                    self._internal_ip_cache[node_name] = node_id

        else:
            if is_head_node: # TODO: use a script
                command = ""
                for init in current_conf["init_commands"]:
                    command += init + " && "
                command += "ray start --head"
                command += " --port=" + current_conf["gcs_port"]
                command += " --ray-client-server-port=" + current_conf["ray_client_port"]
                command += " --dashboard-port=" + current_conf["dashboad_port"]
                command += " --num-cpus=0 --num-gpus=0"
                os.system(command)
                self.lauched_nodes[HEAD_NODE_ID_OUTSIDE_SLURM] = [SLURM_STATE_RUNNING, tags, current_conf["head_ip"]]
                self._internal_ip_cache[current_conf["head_ip"]] = HEAD_NODE_ID_OUTSIDE_SLURM
            else:
                raise ValueError("Worker node must be launched under slurm. Change config file to fix")
        
        if is_head_node:
            self.head_ip = current_conf["head_ip"]
            self.gcs_port = current_conf["gcs_port"]
            self.ray_client_port = current_conf["ray_client_port"]
            self.dashboard_port = current_conf["dashboad_port"]
        

    def create_node_with_resources(
        self,
        node_config: Dict[str, Any],
        tags: Dict[str, str],
        count: int,
        resources: Dict[str, float],
    ) -> Optional[Dict[str, Any]]:
        """Create nodes with a given resource config. 

        This is the method actually called by the autoscaler. Prefer to
        implement this when possible directly, otherwise it delegates to the
        create_node() implementation.
        """

        # LTK's note: the resource config for a specific node type is fixed
        # which will be handled by create_node() using the node_config

        return self.create_node(node_config, tags, count)

    def set_node_tags(self, node_id: str, tags: Dict[str, str]) -> None:
        """Sets the tag values (string dict) for the specified node."""
        if node_id in self.lauched_nodes:
            for tag, value in tags.items():
                self.lauched_nodes[node_id][INFO_TAG_INDEX][tag] = value
        else:
            cli_logger.warning("Set tags is called non-exsiting node\n")

    def terminate_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Terminates the specified node.

        Optionally return a mapping from deleted node ids to node
        metadata.
        """
        if node_id not in self.lauched_nodes:
            cli_logger.warning("Trying to terminate non-exsiting node\n")
            return
        
        if node_id == HEAD_NODE_ID_OUTSIDE_SLURM:
            os.system("ray stop") # TODO: env init
        else:
            slurm_cancel_job(node_id)
        
        if self.lauched_nodes[node_id][INFO_IP_INDEX] in self._internal_ip_cache:
            self._internal_ip_cache.pop(self.lauched_nodes[node_id][INFO_IP_INDEX])
        self.lauched_nodes.pop(node_id)

        # TODO: check head? 


    def terminate_nodes(self, node_ids: List[str]) -> Optional[Dict[str, Any]]:
        """Terminates a set of nodes.

        May be overridden with a batch method, which optionally may return a
        mapping from deleted node ids to node metadata.
        """
        for node_id in node_ids:
            logger.info("NodeProvider: {}: Terminating node".format(node_id))
            self.terminate_node(node_id)
        return None

    @property
    def max_terminate_nodes(self) -> Optional[int]:
        """The maximum number of nodes which can be terminated in one single
        API request. By default, this is "None", which means that the node
        provider's underlying API allows infinite requests to be terminated
        with one request.

        For example, AWS only allows 1000 nodes to be terminated
        at once; to terminate more, we must issue multiple separate API
        requests. If the limit is infinity, then simply set this to None.

        This may be overridden. The value may be useful when overriding the
        "terminate_nodes" method.
        """
        return None

    @staticmethod
    def bootstrap_config(cluster_config: Dict[str, Any]) -> Dict[str, Any]:
        """Bootstraps the cluster config by adding env defaults if needed."""
        return cluster_config

    def get_command_runner(
        self,
        log_prefix: str,
        node_id: str,
        auth_config: Dict[str, Any],
        cluster_name: str,
        process_runner: ModuleType,
        use_internal_ip: bool,
        docker_config: Optional[Dict[str, Any]] = None,
    ) -> CommandRunnerInterface:
        """Returns the CommandRunner class used to perform SSH commands.

        Args:
        log_prefix(str): stores "NodeUpdater: {}: ".format(<node_id>). Used
            to print progress in the CommandRunner.
        node_id(str): the node ID.
        auth_config(dict): the authentication configs from the autoscaler
            yaml file.
        cluster_name(str): the name of the cluster.
        process_runner(module): the module to use to run the commands
            in the CommandRunner. E.g., subprocess.
        use_internal_ip(bool): whether the node_id belongs to an internal ip
            or external ip.
        docker_config(dict): If set, the docker information of the docker
            container that commands should be run on.
        """
        common_args = {
            "log_prefix": log_prefix,
            "node_id": node_id,
            "provider": self,
            "auth_config": auth_config,
            "cluster_name": cluster_name,
            "process_runner": process_runner,
            "use_internal_ip": use_internal_ip,
        }

        return EmptyCommandRunner(**common_args)

        # if docker_config and docker_config["container_name"] != "":
        #     return DockerCommandRunner(docker_config, **common_args)
        # else:
        #     return SSHCommandRunner(**common_args)

    def prepare_for_head_node(self, cluster_config: Dict[str, Any]) -> Dict[str, Any]:
        """Returns a new cluster config with custom configs for head node."""
        return cluster_config

    @staticmethod
    def fillout_available_node_types_resources(
        cluster_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Fills out missing "resources" field for available_node_types."""
        return cluster_config # TODO: Future: overide to prevent user from messing up 
# TODO: ray down???
