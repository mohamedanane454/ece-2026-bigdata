#!/bin/bash
# usage: bash ~/work/labkit/run.sh <name>  -> execute labkit/<name>.sh, log in labkit/logs/<name>.log
name=$1
cd /home/onyxia/work
bash /home/onyxia/work/labkit/setup.sh >/dev/null 2>&1
source /home/onyxia/work/labkit/env.sh
export PS4='$ '
{ echo "===== $name - $(date -u) ====="; bash -x /home/onyxia/work/labkit/$name.sh; echo "EXIT_CODE=$?"; } 2>&1 | tee /home/onyxia/work/labkit/logs/$name.log
echo "### FIN $name"
