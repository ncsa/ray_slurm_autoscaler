import json
import logging
from types import ModuleType
from typing import Any, Dict, List, Optional

from ray.autoscaler.command_runner import CommandRunnerInterface
from ray.autoscaler._private.slurm.empty_command_runner import EmptyCommandRunner

from ray.autoscaler._private.slurm.slurm_commands import (
    slurm_cancel_job,
    slurm_launch_head,
    slurm_launch_worker,
    slurm_get_job_ip,
    slurm_get_job_status,
    SLURM_JOB_RUNNING,
    SLURM_JOB_PENDING,
    SLURM_JOB_NOT_EXIST
)

from threading import RLock
from filelock import FileLock

from ray.autoscaler._private.cli_logger import cli_logger

import copy
import os
import time

logger = logging.getLogger(__name__)

'''Ray-Slurm Interface

Created by Tingkai Liu (tingkai2@illinois.edu) on June 10, 2022
Using the node_provider.py template

node id: the slurm batch submission id for the node
node name: the node name under slurm
node ip: the local node ip

Information needed for each node:
1. Slurm job id (major key)
2. State: running, pending, terminated (should just be deleted)
3. Tag (set and read by updater)

'''

# Const
NODE_STATE_RUNNING = "running"
NODE_STATE_PENDING = "pending"
NODE_STATE_TERMINATED = "terminated"

# Map slurm job status to node states
SLURM_JOB_TRANS_MAP = {
    SLURM_JOB_RUNNING : NODE_STATE_RUNNING,
    SLURM_JOB_PENDING : NODE_STATE_PENDING,
    SLURM_JOB_NOT_EXIST : NODE_STATE_TERMINATED
}


HEAD_NODE_ID_OUTSIDE_SLURM = "-1"

# Default values if not provided in the node config
HEAD_UNDER_SLURM = False
WORKER_UNDER_SLURM = True
DEFAULT_TEMP_FOLDER_NAME = "temp_script"

# WAIT_NODE_INTERVAL = 10 # in second
# WAIT_NODE_INITIAL_TIME = 1 # in second. Use for the gap between launch and check 

filelock_logger = logging.getLogger("filelock")
filelock_logger.setLevel(logging.WARNING)

'''Load/Store additional information on file for slurm cluster
(modified from local.ClusterState)

The node states on file may not be up to date---need to query slurm for updating
The updates are performed by non-terminate-node() call

'''
class SlurmClusterState:
    def __init__(self, lock_path, save_path, provider_config):
        self.lock = RLock()
        os.makedirs(os.path.dirname(lock_path), exist_ok=True)
        self.file_lock = FileLock(lock_path)
        self.save_path = save_path

        with self.lock:
            with self.file_lock:
                if os.path.exists(self.save_path): # Reload the cluser states
                    workers = json.loads(open(self.save_path).read())
                else:
                    workers = {}
                    with open(self.save_path, "w") as f:
                        logger.debug(
                            "ClusterState: Writing cluster state: {}".format(workers)
                        )
                        f.write(json.dumps(workers))

                logger.info(
                    "ClusterState: Loaded cluster state: {}".format(list(workers))
                )

    def get(self):
        with self.lock:
            with self.file_lock:
                workers = json.loads(open(self.save_path).read())
                return workers

    def put(self, worker_id, info):
        assert "tags" in info
        assert "state" in info
        with self.lock:
            with self.file_lock:
                workers = self.get()
                workers[worker_id] = info
                with open(self.save_path, "w") as f:
                    logger.info(
                        "ClusterState: "
                        "Writing cluster state: {}".format(list(workers))
                    )
                    f.write(json.dumps(workers))
    
    def delete(self, worker_id: str):
        with self.lock:
            with self.file_lock:
                workers = self.get()
                if worker_id in workers:
                    workers.pop(worker_id)
                with open(self.save_path, "w") as f:
                    logger.info(
                        "ClusterState: "
                        "Writing cluster state: {}".format(list(workers))
                    )
                    f.write(json.dumps(workers))


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
        self._external_ip_cache: Dict[str, str] = {} # should not be used

        # Create a folder to store modified scripts
        temp_folder_name = DEFAULT_TEMP_FOLDER_NAME
        if "temp_folder_name" in provider_config:
            temp_folder_name = provider_config["temp_folder_name"]
        
        os.makedirs(temp_folder_name, exist_ok=True)

        self.temp_folder = temp_folder_name
        self.template_folder = provider_config["template_path"]

        self.head_started = False
        self.head_ip = provider_config["head_ip"]
        self.gcs_port = provider_config["gcs_port"]
        self.ray_client_port = provider_config["ray_client_port"]
        self.dashboard_port = provider_config["dashboad_port"]

        lock_path = os.path.join(self.temp_folder,
                        "cluster-{}.lock".format(cluster_name))
        state_path = os.path.join(self.temp_folder,
                        "cluster-{}.state".format(cluster_name))
        self.state = SlurmClusterState(
            lock_path,
            state_path,
            provider_config,
        )


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

        The states will be updated when called

        """
        
        workers = self.state.get()
        matching_ids = []
        for worker_id, info in workers.items():
            
            if worker_id != HEAD_NODE_ID_OUTSIDE_SLURM:
                # Update node status
                slurm_job_status = slurm_get_job_status(worker_id)
                if SLURM_JOB_TRANS_MAP[slurm_job_status] != info["state"]:
                    info["state"] = SLURM_JOB_TRANS_MAP[slurm_job_status]
                    if info["state"] == NODE_STATE_TERMINATED:
                        self.state.delete(worker_id)
                    else:
                        self.state.put(worker_id, info)
            
            if info["state"] == NODE_STATE_TERMINATED: 
                continue

            ok = True
            for k, v in tag_filters.items():
                if info["tags"].get(k) != v:
                    ok = False
                    break
            if ok:
                matching_ids.append(worker_id)

        return matching_ids

    def is_running(self, node_id: str) -> bool:
        """Return whether the specified node is running."""

        # workers = self.state.get()
        
        # if node_id in workers:
        #     return workers[node_id]["state"] == NODE_STATE_RUNNING
        # else:
        #     cli_logger.warning("Get is_running for non-existing node\n")
        #     return False
        if node_id == HEAD_NODE_ID_OUTSIDE_SLURM:
            return True # TODO:
        else:
            return slurm_get_job_status(node_id) == SLURM_JOB_RUNNING


    def is_terminated(self, node_id: str) -> bool:
        """Return whether the specified node is terminated."""
        if node_id == HEAD_NODE_ID_OUTSIDE_SLURM:
            return False # TODO:
        else:
            return slurm_get_job_status(node_id) == SLURM_JOB_NOT_EXIST

    def node_tags(self, node_id: str) -> Dict[str, str]:
        """Returns the tags of the given node (string dict)."""

        workers = self.state.get()
        if node_id in workers:
            return workers[node_id]["tags"]
        else:
            cli_logger.warning("Get tags for non-existing node\n")
            return {}

    def external_ip(self, node_id: str) -> str:
        """Returns the external ip of the given node."""
        raise NotImplementedError("Must use internal IPs with slurm")

    def internal_ip(self, node_id: str) -> str:
        """Returns the internal ip (Ray ip) of the given node."""
        if node_id == HEAD_NODE_ID_OUTSIDE_SLURM:
            return self.head_ip
        else:
            return slurm_get_job_ip(node_id)

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

        def find_node_id():
            if use_internal_ip:
                return self._internal_ip_cache.get(ip_address)
            else:
                return self._external_ip_cache.get(ip_address)

        if not find_node_id(): 
            all_nodes = self.non_terminated_nodes({})
            ip_func = self.internal_ip if use_internal_ip else self.external_ip
            ip_cache = (
                self._internal_ip_cache if use_internal_ip else self._external_ip_cache
            )
            for node_id in all_nodes:
                ip_cache[ip_func(node_id)] = node_id

        if not find_node_id():
            if use_internal_ip:
                known_msg = f"Worker internal IPs: {list(self._internal_ip_cache)}"
            else:
                known_msg = f"Worker external IP: {list(self._external_ip_cache)}"
            raise ValueError(f"ip {ip_address} not found. " + known_msg)

        return find_node_id()
        

    def create_node( 
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
                    self.gcs_port,
                    self.ray_client_port,
                    self.dashboard_port,
                    parsed_init_command
                )

                # Store pending info: will be updated by non_terminate_node
                node_info = {}
                node_info["state"] = NODE_STATE_PENDING
                node_info["tags"] = tags
                self.state.put(node_id, node_info)

                # time.sleep(WAIT_NODE_INITIAL_TIME)
                # self._wait_for_node_and_update(node_id, tags)
            else:
                for _ in range(count):
                    node_id = slurm_launch_worker(
                        self.template_folder,
                        self.temp_folder,
                        self.head_ip+":"+self.gcs_port,
                        parsed_init_command
                    )

                    # Store pending info: will be updated by non_terminate_node
                    node_info = {}
                    node_info["state"] = NODE_STATE_PENDING
                    node_info["tags"] = tags
                    self.state.put(node_id, node_info)
                    
                    # self._wait_for_node_and_update(node_id, tags)

        else:
            if is_head_node: # TODO: use a script
                command = ""
                for init in current_conf["init_commands"]:
                    command += init + " && "
                command += "ray start --head"
                command += " --port=" + self.gcs_port
                command += " --ray-client-server-port=" + self.ray_client_port
                command += " --dashboard-port=" + self.dashboard_port
                command += " --num-cpus=0 --num-gpus=0"
                command += " --autoscaling-config=~/ray_bootstrap_config.yaml"
                os.system(command)

                node_info = {}
                node_info["state"] = NODE_STATE_RUNNING
                node_info["tags"] = tags
                self.state.put(HEAD_NODE_ID_OUTSIDE_SLURM, node_info)

                self._internal_ip_cache[self.head_ip] = HEAD_NODE_ID_OUTSIDE_SLURM 

            else:
                raise ValueError("Worker node must be launched under slurm. Change config file to fix")
        
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
        with self.state.file_lock:
            workers = self.state.get()
            if node_id in workers:
                info = workers[node_id]
                info["tags"].update(tags)
                self.state.put(node_id, info)
                return
            
        cli_logger.warning("Set tags is called non-exsiting node\n")

    def terminate_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Terminates the specified node.

        Optionally return a mapping from deleted node ids to node
        metadata.
        """

        workers = self.state.get()

        if node_id not in workers:
            cli_logger.warning("Trying to terminate non-exsiting node\n")
            return
        
        if node_id == HEAD_NODE_ID_OUTSIDE_SLURM:
            os.system("ray stop") # TODO: env init
        else:
            slurm_cancel_job(node_id)
        
        self.state.delete(node_id)

        # if self.launched_nodes[node_id][INFO_IP_INDEX] in self._internal_ip_cache:
        #     self._internal_ip_cache.pop(self.launched_nodes[node_id][INFO_IP_INDEX])


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

        return EmptyCommandRunner(**common_args) # TODO: distinguish under slurm or not 

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


