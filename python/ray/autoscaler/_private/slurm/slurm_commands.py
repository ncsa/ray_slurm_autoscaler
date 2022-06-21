
'''Slurm Command interfaces

Created by Tingkai Liu (tingkai2@illinois.edu) on June 17, 2022

'''

import subprocess
import os
from ray.autoscaler._private.cli_logger import cli_logger
from ray.autoscaler._private.slurm import SLURM_IP_LOOKUP

def slurm_cancel_job(job_id: str):
    if job_id.isdecimal():
        os.system("scancel " + job_id)
    else:
        cli_logger.warning("Slurm interface: invalid job id")

def slurm_launch_worker(
    template_folder: str, 
    temp_folder_name: str, 
    head_ip_with_port: str, 
    init_command: str
) -> str:
    '''Launch a worker node under slurm 
        Return the slurm job id as the node id

        # TODO: Add support for different node type
    '''

    f = open(template_folder+"worker.slurm", "r")
    template = f.read()
    f.close()
    
    template = template.replace("[_PY_IP_HEAD_]", head_ip_with_port)
    template = template.replace("[_PY_INIT_COMMAND_]", init_command)

    f = open(temp_folder_name+"/worker.slurm", "w")
    f.write(template)
    f.close()

    slurm_command = ["sbatch", temp_folder_name+"/worker.slurm"]
    output = subprocess.check_output(slurm_command).decode()

    # Test slurm batch output
    worker_job_id = ""
    comp = output.split()
    if comp[-1].isdecimal():
        worker_job_id = comp[-1]
    else:
        cli_logger.error("Slurm error when starting worker")
        raise ValueError("Slurm error when starting worker")
    
    # TODO: Check whether slurm task is pending

    return worker_job_id
    

def slurm_launch_head(
    template_folder: str, 
    temp_folder_name:str,
    head_node: str,
    gcs_port: str,
    ray_client_port: str,
    dashboard_port: str, 
    init_command: str
) -> str:
    # Replace the script with proper var
    f = open(template_folder+"head.slurm", "r")
    template = f.read()
    f.close()
    
    template = template.replace("[_PY_HEAD_NODE_]", head_node)
    template = template.replace("[_PY_PORT_]", gcs_port)
    template = template.replace("[_PY_INIT_COMMAND_] ", init_command)
    template = template.replace("[_PY_RAY_CLIENT_PORT_]", ray_client_port)
    template = template.replace("[_PY_DASHBOARD_PORT_]", dashboard_port)

    f = open(temp_folder_name+"/head.slurm", "w")
    f.write(template)
    f.close()

    slurm_command = ["sbatch", temp_folder_name+"/head.slurm"]
    output = subprocess.check_output(slurm_command).decode()

    # Test slurm batch output
    head_job_id = ""
    comp = output.split()
    if comp[-1].isdecimal():
        head_job_id = comp[-1]
    else:
        cli_logger.error("Slurm error when starting head")
        raise ValueError("Slurm error when starting head")

    # TODO: Check whether slurm task is pending
    
    return head_job_id

def slurm_get_node_ip(job_id: str) -> str:
    slurm_command = ["squeue", "-j "+job_id]
    output = subprocess.check_output(slurm_command).decode().splitlines()

    if len(output) != 2:
        return "-1"

    node_name = output[1].split()[-1]
    if node_name in SLURM_IP_LOOKUP:
        return SLURM_IP_LOOKUP[node_name]
    else:
        return "-1"

