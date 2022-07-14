#!/bin/bash

#SBATCH --partition=haswell
#SBATCH --job-name=Operational_SEAS5_BCSD      # Job name
#SBATCH --mail-type=END,FAIL                   # Mail events (NONE, BEGIN, END, FAIL, ALL)
#SBATCH --mail-user=christof.lorenz@kit.edu    # Where to send mail	
#SBATCH --nodes=1                              # Run all processes on a single node	
#SBATCH --ntasks-per-node=40		       # Number of tasks on the IVY Node
#SBATCH --time=15:00:00                        # Time limit hrs:min:sec

#SBATCH --output=logs/seas5_bcsd_%j.log             # Standard output and error log
date;hostname;pwd

module load app/cdo/1.9.9
module load app/nco/4.7.8
module load app/matlab/2021b

export PATH="/pd/home/lorenz-c/miniconda3/bin":$PATH

cd /home/lorenz-c/Projects/bias-correction-of-seas5

# Run the issue-date-update-script
python3 src/update_params.py

# Run the transformation from gaussian to regular grid
python3 src/gauss_to_regular.py

# Run the re-ordering of global data
python3 src/global_processing.py

# Run the truncation to the regional domains
python3 src/regional_processing.py

# Run the BCSD script
matlab -nodisplay -r "cd src; run_bcsd; exit"

# Do the re-chunking magic
python3 src/lnchnks_to_mapchnks.py

# Run the BCSD script
matlab -nodisplay -r "cd src; compute_ts; exit"

# Compute monthly values
python3 src/daytomonth.py

# Do the evaluation
matlab -nodisplay -r "cd src; run_eval; exit"

