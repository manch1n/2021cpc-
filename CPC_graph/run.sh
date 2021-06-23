# !/bin/bash

if [ "$#" -ne 3 ]; then
  echo "Usage: $0 <executable> <data name> <process per node> " >&2
  exit 1
fi

function select_queue() {
	node=$((${1}/4))
	load=$(mqload -w | grep -P "q_sw_${2}[a-z0-9_]* " -o)
	if [ "$load" = "" ]; then
		load=$(mqload -w | grep -P "q_sw_[a-z0-9_]* " -o)
	fi
	arr=(${load//,/})
	for var in ${arr[@]}
	do
		load=$(mqload ${var} -w | grep -P "(?<=${var}).*" -o | grep -P "\d+" -o)
		tmp=(${load//,/})
		if [ $node -le ${tmp[3]} ]; then
			echo ${var}
			return 0
		fi
	done
	return 1
}

export DAPL_DBG_TYPE=0

if [ ! -d "data" ]; then
    echo "copying data"
    mkdir data
    cp -f /home/export/online1/cpc/graph_data/* data/
fi
if [ ! -d "logs" ]; then
    mkdir logs
fi
DATAPATH=data
REP=64

EXECUTABLE=$1
DATAFILE=$2
PROC_NUM=$3
queue=$(select_queue ${PROC_NUM} cpc)
if [ $? -eq 1 ]; then
	echo -e "\033[1;31mnode is not enough !\033[0m"
	mqload -w 
else
	suffix=$(date +%Y%m%d-%H%M%S)
	bsub -I -b -q $queue -host_stack 1024 -share_size 4096 -n ${PROC_NUM} -cgsp 64 ./${EXECUTABLE} ${DATAPATH}/${DATAFILE}.csr ${REP} 2>&1 | tee logs/log.${suffix}
fi