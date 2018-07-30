#!/bin/bash

cd /home/hadoop/deploy/spider/getdoubanURL

html=`curl -L movie.douban.com`
error_msg="检测到有异常请求"
if [[ ${html} =~ ${error_msg} ]]
	then
		echo `date`"[getdoubanURLIP被限制]" >> nohup.out
		echo `date`"[getdoubanURL停止爬虫]" >> nohup.out
		pkill -ef "douban/run"
		echo
		sleep 3
		echo `date`"[getdoubanURL停止完成]" >> nohup.out
	else
		echo `date`"[getdoubanURL][IP解除限制, 检查是否已经启动过爬虫...]" >> nohup.out
		pid=`pgrep -f "douban/run"`
		sleep 3
		if [ ${pid} > 0 ]
		then
			echo `date`"[getdoubanURL爬虫已经启动pid[${pid}], 直接退出...]" >> nohup.out
		else
			echo `date`"[getdoubanURL爬虫没有启动, starting...]" >> nohup.out
			sh start.sh
			sleep 3
			echo `date`"[getdoubanURL启动成功]" >> nohup.out
		fi

fi