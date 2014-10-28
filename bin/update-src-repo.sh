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
  svn_updates_required=2
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

  [ "$(type svn &>/dev/null;echo $?)" != "0" ] && logerr "svn does not appear to be installed on this node"
 
}
#---------------
function copy_file {
   cp $1 $2
   svn_updates_required=1
}
#---------------
function check_webapps {
  [   -z "$webapps" ] && logerr "(check_webapps function: webapps variable not set"
  [ ! -d "$webapps" ] && logerr "WebappsDir does not exist: $webapps"
  svndir=$srcdir/webapps
  logit "Checking $svndir"
  files="$(ls $webapps)" 
  for file in $files
  do
    [ "$file" = "index.html" ] && continue
    if [ ! -f "$svndir/$file" ];then
      logit "... add $file"
      copy_file $webapps/$file $svndir/.
      svn add $svndir/$file
      continue
    fi
    rtn="$(diff $webapps/$file $svndir/$file &>/dev/null ;echo $?)"
    if [ "$rtn" != "0" ];then
      logit "... update $file"
      copy_file $webapps/$file $svndir/.
    fi
  done
}
#---------------
function check_apel_updates {
  [   -z "$updates" ] && logerr "(check_apel_updates function: updates variable not set"
  [ ! -d "$updates" ] && logerr "UpdatesDir does not exist: $updates"
  svndir=$srcdir/apel-updates
  logit "Checking $svndir"
  files="$(ls $updates)" 
  for file in $files
  do
    [ "$file" = "index.html" ] && continue
    if [ ! -f "$svndir/$file" ];then
      logit "... add $file"
      copy_file $updates/$file $svndir/.
      svn add $svndir/$file
      continue
    fi
    rtn="$(diff $updates/$file $svndir/$file &>/dev/null ;echo $?)"
    if [ "$rtn" != "0" ];then
      logit "... update $file"
      copy_file $updates/$file $svndir/.
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
  svndir=$srcdir/etc/lcg-reportableSites.history
  logit "Checking $svndir"
  [ ! -d "$svndir" ] && logerr "source directory does not exist: $svndir"
  files="$(ls $sitehistory)" 
  for file in $files
  do
    if [ ! -f "$svndir/$file" ];then
      logit "... add $file"
      copy_file $sitehistory/$file $svndir/.
      svn add $svndir/$file
      continue
    fi
    rtn="$(diff $sitehistory/$file $svndir/$file &>/dev/null ;echo $?)"
    if [ "$rtn" != "0" ];then
      logit "... update $file"
      copy_file $sitehistory/$file $svndir/.
    fi
  done
}
#---------------
function check_reportable_vos {
  [   -z "$vos" ] && logerr "(check_reportable_vos function: vos variable not set"
  [ ! -f "$vos" ] && logerr "VOFilterFile does not exist: $vos"
  file=$srcdir/etc/$(basename $vos)
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
  svndir=$srcdir/bin
  logit "Checking $svndir for executable file changes"
  [ ! -d "$svndir" ] && logerr "Source repo dir does not exist: $svndir"
  files="$(ls $executables/*.py $executables/*.sh)" 
  for file in $files
  do
    file=$(basename $file)
    [ "$file" = "lcg.sh" ] && continue
    [ "$file" = "find-late-updates.sh" ] && continue
    if [ ! -f "$svndir/$file" ];then
      logit "... add $file"
      continue
    fi
    rtn="$(diff $executables/$file $svndir/$file &>/dev/null ;echo $?)"
    if [ "$rtn" != "0" ];then
      logit "... update $file"
    fi
  done
}
#---------------
function check_svn {
  [   -z "$srcdir" ] && logerr "(check_svn function: source variable not set"
  [ ! -d "$srcdir" ] && logerr "Source repo dir does not exist: $source"
  logit "Checking source repo: $srcdir"
  cd $srcdir
#  svn status --show-updates 2>&1 |egrep -v "rpms|tarballs" 1>>$tmpfile 2>&1
  svn status 2>&1 |egrep -v "rpms|tarballs" 1>>$tmpfile 2>&1
  cd - 1>>$tmpfile 2>&1
}
#---------------
function send_email {
  case $svn_updates_required in 
    0 ) subject="No Gratia-APEL svn updates required on $(hostname -f)" ;;
    1 ) subject="Gratia-APEL svn updates required on $(hostname -f)"    ;;
    * ) subject="ERROR in Gratia-APEL svn updates process: $PGM on $(hostname -f)" ;;
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
srcdir=$HOME/cdcvs/gratia/interfaces/ssm.apel-lcg
srcdir=$HOME/cdcvs/gratia/interfaces/gratia-apel
tmpfile=/tmp/$PGM.log
>$tmpfile
svn_updates_required=0

validate
check_webapps
check_apel_updates
check_reportable_sites
check_reportable_sites_history
check_reportable_vos
check_source
check_svn
send_email
exit 0


