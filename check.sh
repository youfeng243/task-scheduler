#!/usr/bin/env bash

project=task-scheduler
main_class=com.haizhi.TaskScheduler

check() {
    pid=`ps -ef | grep java | grep "$1" | grep "$2" | awk '{print $2}'`
    if [ -z "${pid}" ]; then
        exit
    fi

    echo ${pid}
}

if [ $# == 2 ]; then
    check $1 $2
else
    check ${project} ${main_class}
fi