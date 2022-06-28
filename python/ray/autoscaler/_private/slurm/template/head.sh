#!/bin/bash -l

head_node_ip="[_PY_HEAD_NODE_IP_]" # To be replaced by python script
port="[_PY_PORT_]"
ray_client_port="[_PY_RAY_CLIENT_PORT_]"
dashboad_port="[_PY_DASHBOARD_PORT_]"
cpus="1"
gpus="0"

[_PY_INIT_COMMAND_] # To be replaced by python laucher

ray start --head --node-ip-address="$head_node_ip" --port=$port --dashboard-port=$dashboad_port \
    --num-cpus "$cpus" --num-gpus "$gpus" --ray-client-server-port "$ray_client_port" \
    --autoscaling-config=~/ray_bootstrap_config.yaml
