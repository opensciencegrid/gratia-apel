#!/bin/bash
################################################################
# John Weigand (10/8/13)
#
# Updates the srouce code area with the latest data that needs
# to be retained.
################################################################
function logit {
  echo "$1" >>$tmpfile
}
#---------------
function logerr {
  logit "ERROR: $1"
  src_updates_required=2
  send_email
  exit 1
}
#---------------
function validate {
  if [ -z "$to_mail" ];then 
     echo "ERROR: arg2 missing";usage;exit 1
  fi
  if [ ! -f "$cfg" ];then 
     echo "ERROR: You sure you are on the right node.
The $cfg does not exist.";usage;exit 1
  fi

  [ ! -d "$srcdir" ] && logerr "You sure you are on the right node.
The source directory does not exist: $srcdir"

  webapps="$(grep  '^WebappsDir' $cfg |awk '{print $2}')"
  [ ! -d "$webapps" ] && logerr "WebappsDir does not exist: $webapps"

  updates="$(grep  '^UpdatesDir' $cfg |awk '{print $2}')"
  [ ! -d "$updates" ] && logerr "UpdatesDir does not exist: $updates"

  sites="$(grep  '^SiteFilterFile' $cfg |awk '{print $2}')"
  [ ! -f "$sites" ] && logerr "SiteFilterFile does not exist: $sites"

  sitehistory="$(grep  '^SiteFilterHistory' $cfg |awk '{print $2}')"
  [ ! -d "$sitehistory" ] && logerr "SiteFilterHistory does not exist: $sitehistory"

  vos="$(grep  '^VOFilterFile' $cfg |awk '{print $2}')"
  [ ! -f "$vos" ] && logerr "VOFilterFile does not exist: $vos"

  executables=/usr/local/gratia-apel/bin
  [ ! -d "$executables" ] && logerr "executable directory does not exist: $executables"

  [ "$(type git &>/dev/null;echo $?)" != "0" ] && logerr "git does not appear to be installed on this node"
 
}
#---------------
function copy_file {
   cp $1 $2
   src_updates_required=1
}
#---------------
function check_webapps {
  [   -z "$webapps" ] && logerr "(check_webapps function: webapps variable not set"
  [ ! -d "$webapps" ] && logerr "WebappsDir does not exist: $webapps"
  local dir=$srcdir/webapps
  logit "Checking $dir"
  local files="$(ls $webapps)" 
  for file in $files
  do
    [ "$file" = "index.html" ] && continue
####    if [ ! -f "$dir/$file" ];then
####      logit "... add $file"
####      copy_file $webapps/$file $dir/.
####      git add $dir/$file
####      continue
####    fi
    rtn="$(diff $webapps/$file $dir/$file &>/dev/null ;echo $?)"
    if [ "$rtn" != "0" ];then
      logit "... update $file"
      copy_file $webapps/$file $dir/.
    fi
  done
}
#---------------
function check_apel_updates {
  [   -z "$updates" ] && logerr "(check_apel_updates function: updates variable not set"
  [ ! -d "$updates" ] && logerr "UpdatesDir does not exist: $updates"
  local dir=$srcdir/apel-updates
  logit "Checking $dir"
  local files="$(ls $updates)" 
  for file in $files
  do
    [ "$file" = "index.html" ] && continue
    if [ ! -f "$dir/$file" ];then
      logit "... add $file"
      copy_file $updates/$file $dir/.
      git add $dir/$file
      continue
    fi
    rtn="$(diff $updates/$file $dir/$file &>/dev/null ;echo $?)"
    if [ "$rtn" != "0" ];then
      logit "... update $file"
      copy_file $updates/$file $dir/.
    fi
  done
}
#---------------
function check_reportable_sites {
  [   -z "$sites" ] && logerr "(check_reportable_sites function: sites variable not set"
  [ ! -f "$sites" ] && logerr "SiteFilterFile does not exist: $sites"
  file=$srcdir/etc/$(basename $sites)
  logit "Checking $file"
  [ ! -f "$file" ] && logerr "source file does not exist: $file"
  rtn="$(diff $sites $file &>/dev/null ;echo $?)"
  if [ "$rtn" != "0" ];then
    logit "... update $(basename $file)"
    copy_file $sites $file
  fi
}
#---------------
function check_reportable_sites_history {
  [   -z "$sitehistory" ] && logerr "(check_reportable_sites function: sitehistory variable not set"
  [ ! -d "$sitehistory" ] && logerr "SiteFilterHistory does not exist: $sitehistory"
  local dir=$srcdir/etc/lcg-reportableSites.history
  logit "Checking $dir"
  [ ! -d "$dir" ] && logerr "source directory does not exist: $dir"
  local files="$(ls $sitehistory)" 
  for file in $files
  do
    if [ ! -f "$dir/$file" ];then
      logit "... add $file"
      copy_file $sitehistory/$file $dir/.
      git add $dir/$file
      continue
    fi
    rtn="$(diff $sitehistory/$file $dir/$file &>/dev/null ;echo $?)"
    if [ "$rtn" != "0" ];then
      logit "... update $file"
      copy_file $sitehistory/$file $dir/.
    fi
  done
}
#---------------
function check_reportable_vos {
  [   -z "$vos" ] && logerr "(check_reportable_vos function: vos variable not set"
  [ ! -f "$vos" ] && logerr "VOFilterFile does not exist: $vos"
  local file=$srcdir/etc/$(basename $vos)
  logit "Checking $file"
  [ ! -f "$file" ] && logerr "source file does not exist: $file"

  rtn="$(diff $vos $file &>/dev/null ;echo $?)"
  if [ "$rtn" != "0" ];then
    logit "... update $(basename $file)"
    copy_file $vos $file
  fi
}
#---------------
function check_source {
  [   -z "$executables" ] && logerr "(check_source function: executables variable not set"
  [ ! -d "$executables" ] && logerr "Executables dir does not exist: $executables"
  local dir=$srcdir/bin
  logit "Checking $dir for executable file changes"
  [ ! -d "$dir" ] && logerr "Source repo dir does not exist: $dir"
  local files="$(ls $executables/*.py $executables/*.sh)" 
  for file in $files
  do
    file=$(basename $file)
    [ "$file" = "lcg.sh" ] && continue
    [ "$file" = "find-late-updates.sh" ] && continue
    if [ ! -f "$dir/$file" ];then
      logit "... add $file"
      continue
    fi
    rtn="$(diff $executables/$file $dir/$file &>/dev/null ;echo $?)"
    if [ "$rtn" != "0" ];then
      logit "... update $file"
    fi
  done
}
#---------------
function check_git_status {
  [   -z "$srcdir" ] && logerr "(check_git_status function: source variable not set"
  [ ! -d "$srcdir" ] && logerr "Source repo dir does not exist: $source"
  logit "Checking source repo: $srcdir"
  cd $srcdir
  git status 2>&1 |egrep -v "rpms|tarballs" 1>>$tmpfile 2>&1
  cd - 1>>$tmpfile 2>&1
}
#---------------
function send_email {
  case $src_updates_required in 
    0 ) subject="No Gratia-APEL git updates required on $(hostname -f)" ;;
    1 ) subject="Gratia-APEL git updates required on $(hostname -f)"    ;;
    * ) subject="ERROR in Gratia-APEL git updates process: $PGM on $(hostname -f)" ;;
  esac
  mail -s "$subject" $to_mail <<EOF;rtn=$?
This is from the gratia-apel interface.
This is a cron process on $(hostname -f) called $PGM.
It's purpose is to check for updates to the source repository for the
gratia-apel interface.

$(cat $tmpfile)
EOF
}
#----------------
function usage {
  echo "Usage: $PGM config_file email_address"
}
#### MAIN #######################################################
PGM=$(basename $0)
cfg="$1"
to_mail="$2"
srcdir=$HOME/gratia-apel
tmpfile=/tmp/$PGM.log
>$tmpfile
src_updates_required=0

validate
check_webapps
check_apel_updates
check_reportable_sites
check_reportable_sites_history
check_reportable_vos
check_source
check_git_status
send_email
exit 0


