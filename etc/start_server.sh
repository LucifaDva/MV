#! /bin/bash
#start beanstalk server
gnome-terminal -t 'beanstalkd server' -x bash -c "beanstalkd; exec bash"
#start job allocation
gnome-terminal -t 'job allocation' -x bash -c "python ../worker/JobAllocation.py; exec bash"
#start download worker
gnome-terminal -t 'download worker' -x bash -c "python ../worker/download_worker.py; exec bash"
#start scan worker
gnome-terminal -t 'scan worker' -x bash -c "python ../worker/scan_worker.py; exec bash"
