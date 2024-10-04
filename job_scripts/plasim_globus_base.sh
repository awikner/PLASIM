#!/bin/bash
#SBATCH -p skx
#SBATCH -t 24:00:00
#SBATCH -N 2
#SBATCH -n 64
#SBATCH -A TG-ATM170020 
#SBATCH -o /work2/09979/awikner/stampede3/PLASIM/log_files/plasim-%SIMNAME%-%j.log
#SBATCH --mail-user=awikner@uchicago.edu
#SBATCH --mail-type=ALL

ml python
conda activate globus
#source /jet/home/awikner/globus-venv/bin/activate
globus whoami

cd %RUNDIR%
EXP=MOST    # Name your experiment here
#rm -f plasim_restart
rm -f Abort_Message
YEAR=%STEPSSTART%
YEARS=%STEPSEND%
while [ $YEAR -lt $YEARS ]
do
   YEAR=`expr $YEAR + 1`
   DATANAME=`printf '%s.%03d' $EXP $YEAR`
   DIAGNAME=`printf '%s_DIAG.%03d' $EXP $YEAR`
   RESTNAME=`printf '%s_REST.%03d' $EXP $YEAR`
   mpiexec -np 64 -env I_MPI_COMPATIBILITY=4 most_plasim_t42_l10_p64.x
   [ -e Abort_Message ] && exit 1
   [ -e plasim_output ] && mv plasim_output $DATANAME
   [ -e plasim_diag ] && mv plasim_diag $DIAGNAME
   [ -e plasim_status ] && cp plasim_status plasim_restart
   [ -e plasim_status ] && mv plasim_status $RESTNAME
   echo "PLASIM run step $YEAR finished"
   DATADIR=`printf '%DATADIR%-%03d' $YEAR`
   mkdir $DATADIR
   mv $DATANAME $DATADIR
   mv ice_output $DATADIR
   mv ocean_output $DATADIR
   mv $DIAGNAME $DATADIR
   echo "PLASIM run step $YEAR files moved"
   LABEL=`printf 'moved-%SIMNAME%-%03d' $YEAR`
   INPUTFILE=`printf '%INPUTFILE%%03d.json' $YEAR`
   OUTPUTFILE=`printf 'info-%SIMNAME%-%03d.json' $YEAR`
   #globus flows start -v --label $LABEL --input $INPUTFILE 59d76fe6-f28d-48df-b913-d7dcb6465ab4 > $OUTPUTFILE
   #echo "globus flows start -v --label $LABEL --input $INPUTFILE 59d76fe6-f28d-48df-b913-d7dcb6465ab4 > $OUTPUTFILE"
done
echo "PLASIM run finished"


#RUN_ID1=$(jq -r '.run_id' < info-%SIMNAME%.json)
#globus flows run resume  --skip-inactive-reason-check $RUN_ID1

#globus-automate flow run -v -l "move-%SIMNAME%" --flow-input %INPUTFILE% 6e950d3e-9ef0-4752-94c5-7b2db571f1dc > info-%SIMNAME%.json
#FLOW_ID=$(jq -r '.flow_id' < info-%SIMNAME%.json)
#RUN_ID=$(jq -r '.run_id' < info-%SIMNAME%.json)
#echo "Flow ID: ${FLOW_ID}"
#echo "Run ID: ${RUN_ID}"
#globus-automate flow run-resume --query-for-inactive-reason --flow-id $FLOW_ID $RUN_ID
