#!/bin/bash
###############################################################
# Gratia APEL/LCG Configuration script
#
# Author: John Weigand (8/31/2012)
###############################################################
#-------------------------
function logit {
  echo "$1"
}
#------------------------
function logerr {
  logit "ERROR: $1";exit
}
#------------------------
function runit {
  local cmd="$1"
  logit "...RUNNING: $cmd"
  $cmd;rtn=$?
  [ "$rtn" != "0" ] && logerr "Failed running: $cmd"
}
#------------------------
function check_certificates {
  logit;logit "Checking certificates for crls"
  logit "... checking fetch-crl-cron status"
  /sbin/service fetch-crl-cron status
  if [ "`/sbin/service fetch-crl-cron status  &>/dev/null;echo $?`" != "0" ];then
    logit "... starting fetch crl init.d services"
    runit "/sbin/service fetch-crl-cron start
    runit "/sbin/service fetch-crl-boot start
  fi
  runit "/sbin/chkconfig --add fetch-crl-cron"
  runit "/sbin/chkconfig --add fetch-crl-boot"
}
#------------------------
function configure_web_services {
  logit;logit "Configuring web services"
  local httpd_port=8319
  local cfg=/etc/httpd/conf/httpd.conf
  logit "... web services will be on http://`hostname -f`:${httpd_port}/%{name}"
  logit "... editting: $cfg
  cp -p ${cfg] ${cfg}.orig
  sed -e "s/^Listen 80/Listen ${httpd_port}\n## Listen 80/" ${cfg} >${cfg}.rpmnew
  cp -p ${cfg}.rpmnew ${cfg}
}
#### MAIN ######################################
PGM=$(basename $0)
logit
logit "Running some post-install configuration updates"

check_certificates
configure_web_services


logit "Gratia-apel configuration compolete"
exit 0
