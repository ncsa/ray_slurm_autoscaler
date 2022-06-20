#!/bin/bash -l

#SBATCH --cpus-per-task=10
#SBATCH --gpus-per-task=0

#SBATCH --nodes=1
#SBATCH --exclusive
#SBATCH --tasks-per-node=1
#SBATCH --time=01:30:00

set -x

ip_head="[_PY_IP_HEAD_]" # To be replaced by python laucher

[_PY_INIT_COMMAND_] # To be replaced by python laucher

echo "Starting WORKER"
srun --nodes=1 --ntasks=1 \
    ray start --address "$ip_head" \
    --num-cpus "${SLURM_CPUS_PER_TASK}" --num-gpus "${SLURM_GPUS_PER_TASK}" --block &

# __doc_worker_ray_end__

sleep infinity # wait forever to presist the ray runtime

