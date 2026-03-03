pbs_version = 18.1.4

提交任务：
- qsub -N JOBNAME vasp.sh

Kill任务：
- qdel $JOBID (后缀.mgr可不加，强制停止任务可以加-Wforce)

查询自己任务: 
- qstat -au $USERID

或者
- q -u $USERID

查询某个任务具体状态和参数：
- qstat -xf JOBID

查询历史任务: 
- qstat -x

查询所有节点占用情况：
- pestat