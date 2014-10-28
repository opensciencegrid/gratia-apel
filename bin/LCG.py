#!/usr/bin/python

########################################################################
# 
# Author Philippe Canal, John Weigand
#
# LCG 
#
# Script to transfer the data from Gratia to APEL (WLCG)
########################################################################
#
#@(#)gratia/summary:$HeadURL: https://gratia.svn.sourceforge.net/svnroot/gratia/trunk/interfaces/apel-lcg/LCG.py $:$Id: LCG.py 3000 2009-02-16 16:15:08Z pcanal 
#
#
########################################################################
# Changes:
#
########################################################################
import Downtimes
import InactiveResources
import InteropAccounting
import Rebus
import SSMInterface

import traceback
import exceptions
import time
import calendar
import datetime
import getopt
import math
import re
import string
import email
import smtplib, rfc822  # for email notifications via smtp
import os, sys, time, string
import subprocess

downtimes          = Downtimes.Downtimes()
inactives          = InactiveResources.InactiveResources()
gVoOutput          = ""
gUserOutput        = ""
gWarnings          = []
gSitesWithData     = []
gSitesMissingData  = {}
gSitesWithNoData   = [] 
gKnownVOs = {}

gParams = {"WebLocation"       :None,
           "LogDir"            :None,
           "TmpDataDir"        :None,
           "UpdatesDir"        :None,
           "UpdateFileName"    :None,
           "DeleteFileName"    :None,
           "SSMFile"           :None,
           "SSMConfig"         :None,
           "SiteFilterFile"    :None,
           "SiteFilterHistory" :None,
           "VOFilterFile"      :None,
           "DBConfFile"        :None,
           "MissingDataDays"   :None,
           "FromEmail"         :None,
           "ToEmail"           :None,
           "CcEmail"           :None,
         }
gDbParams = {"GratiaHost" :None,
            "GratiaPort"  :None,
            "GratiaDB"    :None,
            "GratiaUser"  :None,
            "GratiaPswd"  :None,
            }



gProgramName        = None
gMyOSG_available    = True  # flag indicating if there is a problem w/MyOSG
# ------------------------------------------------------------------
# -- Default is query only. Must specify --update to effect updates 
# -- This is to protect against accidental running of the script   
gInUpdateMode       = False  
#------------------------
#Command line arg to suppress email notice
gEmailNotificationSuppressed = False  
gFilterConfigFile   = None
gDateFilter         = None
gRunTime = time.strftime("%Y/%m/%d %H:%M %Z",time.localtime())

#--------------------------
#gPswdfile  = None  

# ----------------------------------------------------------
# special global variables to display queries in the email 
gVoQuery      = ""
gUserQuery    = ""

# -----------------------------------------------------------------------
# Used to get the set of resource for the resource groups being reported
gInteropAccounting = InteropAccounting.InteropAccounting()

# -----------------------------------------------------------------------
# Used to validate MyOSG Interoperability against WLCG Rebus topology
# to verify is a site/resource group is registered
gRebus = Rebus.Rebus()

#-----------------------------------------------
def Usage():
  """ Display usage """
  print  """\
LCG.py   --conf=config_file --date=month [--update] [--no-email]

     --conf - specifies the main configuration file to be used
              which would normally be the lcg.conf file

     --date - specifies the monthly time period to be updated:
              Valid values:
                current  - the current month
                previous - the previous month
                YYYY/MM  - any year and month

              The 'current' and 'previous' values are to facillitate running
              this as a cron script.  Generally, we will run a cron
              entry for the 'previous' month for n days into the current
              month in order to insure all reporting has been completed.

     The following 2 options are to facilitate testing and to avoid
     accidental running and sending of the SSM message to APEL.

     --update - this option says to go ahead and update the APEL/WLCG database.
                If this option is NOT specified, then everything is executed
                EXCEPT the actual sending of the SSM message to APEL.
                The message file will be created.
                This is a required option when running in production mode.
                Its purpose is to avoid accidentally sending data to APEL
                when testing.

     --no-email - this option says to turn off the sending of email
                notifications on failures and successful completions.
                This is very useful when testing changes.
"""

#----------------------------------------------
def getoutput (cmd):
    """Used for simple shell command execution.
       Returns: stdout
    """
    process = subprocess.Popen(cmd,stdout=subprocess.PIPE,shell=True)
    return process.communicate()[0]

#----------------------------------------------
def getstatusoutput(cmd):
    """Used for simple shell command execution.
       Returns: (return code, stdout)[0]
    """
    process = subprocess.Popen(cmd,stdout=subprocess.PIPE,shell=True)
    returncode = process.wait()
    stdout,stderr = process.communicate()
    return returncode,stdout,stderr 

#-----------------------------------------------
def GetArgs(argv):
    global gProgramName,gDateFilter,gInUpdateMode,gEmailNotificationSuppressed,gProbename,gVoOutput,gUserOutput,gFilterConfigFile
    if argv is None:
        argv = sys.argv
    gProgramName = argv[0]
    arglist=["help","no-email","date=","update","config="]

    try:
      opts, args = getopt.getopt(argv[1:], "", arglist)
    except getopt.error, e:
      msg = e.__str__()
      raise Exception("""Invalid command line argument 
%s 
For help use --help
""" % msg)

    for o, a in opts:
      if o in ("--help"):
        Usage()
        sys.exit(1)
      if o in ("--config"):
        gFilterConfigFile = a
        continue
      if o in ("--no-email"):
        gEmailNotificationSuppressed = True
        continue
      if o in ("--update"):
        gInUpdateMode = True
        continue
      if o in ("--date"):
        gDateFilter = a
        if gDateFilter  == "current":
          gDateFilter = GetCurrentPeriod()
        if gDateFilter  == "previous":
          gDateFilter = GetPreviousPeriod()
        continue
      raise Exception("""Invalid command line argument""")
    
    #---- required arguments ------
    if gFilterConfigFile == None:
      raise Exception("--config is a required argument")
    if gDateFilter == None:
      raise Exception("--date is a required argument")
   
#-----------------------------------------------
def SendEmailNotificationFailure(error):
  """ Sends a failure  email notification to the EmailNotice attribute""" 
  subject  = "GRATIA-APEL interface for %s - FAILED (%s)" % (gDateFilter,gRunTime)
  contents = """The interface from Gratia to the APEL (WLCG) database FAILED
with the following error(s):
%(error)s
""" % { "error" : error ,} 
  contents = contents + InterfaceFiles()
  SendEmailNotification(subject,contents)

#-----------------------------------------------
def SendEmailNotificationSuccess():
  """ Sends a successful email notification to the EmailNotice attribute""" 
  global gVoOutput
  global gSitesMissingData
  subject  = "GRATIA-APEL interface for %s - SUCCESS (%s)" % (gDateFilter,gRunTime)
  contents = """The interface from Gratia to the APEL (WLCG) database was successful."""
  if len(gWarnings) == 0:
    contents = contents + "\nNo warning conditions detected."
  else:
    contents = contents + "\n\nWarning conditions have been detected and a separate email will be sent."
  if len(gSitesMissingData) == 0:
    contents = contents + "\nAll sites are reporting.\n"
  else:
    contents = contents + "\nSites missing data for more than %s days:" % gParams["MissingDataDays"]
    sites = gSitesMissingData.keys()
    for site in sites:
      contents = contents + "\n" + site + ": " + str(gSitesMissingData[site])
  SendEmailNotification(subject,contents)

#-----------------------------------------------
def SendEmailNotificationWarnings():
  """ Sends a warning email notification to the EmailNotice attribute""" 
  if len(gWarnings) == 0:
    Logit("No warning conditions detected.")
    return
  Logit("Warning conditions have been detected.")
  subject  = "GRATIA-APEL interface for %s - WARNINGS/ADVISORY (%s)" % (gDateFilter,gRunTime)
  contents = """\
The interface from Gratia to the APEL (WLCG) database was successful.

However, the following possible problems were detected during the execution 
of the interface script and should be investigated.
"""
  for warning in gWarnings:
    contents = "%s\nWARNING/ADVISORY: %s\n" % (contents,warning)
  SendEmailNotification(subject,contents)

#-----------------------------------------------
def SendEmailNotification(subject, contents):
  """ Sends an email notification to the EmailNotice attribute value
      of the lcg-filters.conf file.  This can be overridden on the command
      line to suppress the notification.  This should only be done during
      testing, otherwise it is best to provide some notification on failure.
  """
  global gEmailNotificationSuppressed
  if gParams["CcEmail"] is None and gParams["ToEmail"] is None:
    gEmailNotificationSuppressed = True
    print >>sys.stderr, "ERROR: No CcEmail and ToEmail in configuration file"
    print >>sys.stderr, "Email notification suppressed due to errors with email attributes"
  if gParams["FromEmail"] is None: 
    gEmailNotificationSuppressed = True
    print >>sys.stderr, "ERROR: No FromEmail in configuration file"
    print >>sys.stderr, "Email notification suppressed due to errors with email attributes"
  if gEmailNotificationSuppressed is True:
    Logit("Email notification suppressed due to command line argument or errors")
    return
  message_body = """\
Gratia to APEL/WLCG accountin data transfer.  
This is run as a %(user)s cron process on %(hostname)s at %(runtime)s.

%(contents)s
""" % { "contents" : contents,
        "hostname" : getoutput("hostname -f"),
        "runtime"  : gRunTime,
        "user"     : getoutput("whoami"),}

  Logit("Email notification being sent to %s" % gParams["ToEmail"])
  Logit("Email notification being cc'd to %s" % gParams["CcEmail"])
  Logit("\n" +  contents) 

  try:
    fromaddr = gParams["FromEmail"]
    toaddrs  = string.split(gParams["ToEmail"],",")
    ccaddrs = ["",]
    if gParams["CcEmail"] != "NONE" and  gParams["CcEmail"] is not None:
      ccaddrs  =  string.split(gParams["CcEmail"],",")
    server   = smtplib.SMTP('localhost')
    server.set_debuglevel(0)
    message = """\
From: %(fromaddr)s
To: %(toaddr)s 
Cc: %(ccaddr)s
Subject: %(subject)s
X-Mailer: Python smtplib
%(message)s
""" % { "fromaddr"    : fromaddr,
        "toaddr"      : ",".join(toaddrs),
        "ccaddr"      : ",".join(ccaddrs),
        "subject"     : subject,
        "message"     : message_body, }
    toaddrs = toaddrs + ccaddrs
    server.sendmail(fromaddr,toaddrs,message)
    server.quit()
  except smtplib.SMTPSenderRefused:
    raise Exception("SMTPSenderRefused, message: %s" % message)
  except smtplib.SMTPRecipientsRefused:
    raise Exception("SMTPRecipientsRefused, message: %s" % message)
  except smtplib.SMTPDataError:
    raise Exception("SMTPDataError, message: %s" % message)
  except Exception, e:
    raise Exception("Unsent Message: %s" % e)

#-----------------------------------------------
def InterfaceFiles():
  return """
Key Interface files:
Script............... %(program)s
Node................. %(hostname)s
User................. %(username)s
Log file............. %(logfile)s

SSM executable....... %(ssmfile)s 
SSM config file...... %(ssmconfig)s 
SSM summary file..... %(ssmupdates)s 
SSM summary records.. %(ssmrecs)s  
SSM deletes file..... %(ssmdeletes)s 
SSM deletes records.. %(ssmdels)s 

Reportable sites file.. %(sitefilter)s
Reportable VOs file.... %(vofilter)s
""" % { "program"     : gProgramName,
                "hostname"    : getoutput("hostname -f"),
                "username"    : getoutput("whoami"),
                "logfile"     : GetFileName(gParams["LogDir"],None,"log"),
                "sitefilter"  : gParams["SiteFilterFile"],
                "vofilter"    : gParams["VOFilterFile"],
                "ssmfile"     : gParams["SSMFile"],
                "ssmconfig"   : gParams["SSMConfig"],
                "ssmupdates"  : GetFileName(gParams["UpdatesDir"],gParams["UpdateFileName"],"txt"),
                "ssmrecs"     : getoutput("grep -c '%%' %s" % GetFileName(gParams["UpdatesDir"],gParams["UpdateFileName"],"txt")),
                "ssmdeletes"  : GetFileName(gParams["UpdatesDir"],gParams["DeleteFileName"],"txt"),
                "ssmdels"     : getoutput("grep -c '%%' %s" % GetFileName(gParams["UpdatesDir"],gParams["DeleteFileName"],"txt")), }

#-----------------------------------------------
def GetVOFilters(filename):
  """ Reader for a file of reportable VOs . 
      The file contains a single entry for each filter value.  
      The method returns a formated string for use in a SQL
      'where column_name in ( filters )' structure.
  """
  try:
    filters = ""
    fd = open(filename)
    while 1:
      filter = fd.readline()
      if filter == "":   # EOF
        break
      filter = filter.strip().strip("\n")
      if filter.startswith("#"):
        continue
      if len(filter) == 0:
        continue
      filters = '"' + filter + '",' + filters
    filters = filters.rstrip(",")  # need to remove the last comma
    fd.close()
    return filters
  except IOError, (errno,strerror):
    raise Exception("IO error(%s): %s (%s)" % (errno,strerror,filename))

#-----------------------------------------------
def GetSiteFilters(filename):
  """ Reader for a file of reportable sites. 
      The file contains 2 tokens: the site name and a normalization factor.  
      The method returns a hash table with the key being site and the value
      the normalization factor to use.
  """
  try:
    #--- process the reportable sites file ---
    sites = {}
    fd = open(filename)
    while 1:
      filter = fd.readline()
      if filter == "":   # EOF
        break
      filter = filter.strip().strip("\n")
      if filter.startswith("#"):
        continue
      if len(filter) == 0:
        continue
      site = filter.split()
      if sites.has_key(site[0]):
        raise Exception("System error: duplicate - site (%s) already set" % site[0])
      factor = 0
      if len(site) == 1:
        raise Exception("System error: No normalization factory was provide for site: %s" % site[0])
      elif len(site) > 1:
        #-- verify the factor is an integer --
        try:
          tmp = int(site[1])
          factor = float(site[1])/1000
        except:
          raise Exception("Error in %s file: 2nd token must be an integer (%s" % (filename,filter))
        #-- set the factor --
        sites[site[0]] = factor
      else:
        continue
    #-- end of while loop --
    fd.close()
    #-- verify there is at least 1 site --
    if len(sites) == 0:
      raise Exception("Error in %s file: there are no sites to process" % filename)
    return sites
  except IOError, (errno,strerror):
    raise Exception("IO error(%s): %s (%s)" % (errno,strerror,filename))

#----------------------------------------------
def GetDBConfigParams(filename):
  """ Retrieves and validates the database configuration file parameters"""

  params = GetConfigParams(filename)
  for key in gDbParams.keys():
    if params.has_key(key):
      gDbParams[key] = params[key]
      continue
    raise Exception("Required parameter (%s) missing in config file %s" % (key,filename))

#----------------------------------------------
def GetFilterConfigParams(filename):
  """ Retrieves and validates the filter configuration file parameters"""
  missingEntries = ""
  params = GetConfigParams(filename)
  for key in gParams.keys():
    if params.has_key(key):
      gParams[key] = params[key]
      continue
    missingEntries = missingEntries + "Required parameter (%s) missing in config file %s\n" % (key,filename)
  if len(missingEntries) > 0:
    raise Exception(missingEntries)

#----------------------------------------------
def DetermineReportableSitesFileToUse(reportingPeriod):
  """ This determines the configuration file of reportable sites and
      normalization factors to use.  This data is time sensitive in nature as
      the list of sites and their normalization factors change over time.
      We want to insure that we are using a list that was in effect for
      the month being reported.

      This method will always copy the current SiteFilterFile to the 
      SiteFilterHistory directory during the current month.

      So, we will always use a file in the SiteFilterHistory directory
      with the name SiteFilterFile.YYYYMM in our processing.

      If we cannot find one for the month being processed, we have to fail
      as the data will be repopulated using potentially incorrect data.

      Arguments: reporting period (YYYY/MM)
      Returns: the reportable sites file to use
  """
  filterFile  = gParams["SiteFilterFile"]
  historyDir  = gParams["SiteFilterHistory"]
  #--- make the history directory if it does not exist ---
  if not os.path.isdir(historyDir):
    Logit("... creating %s directory for the reportable sites configuration files" % (historyDir))
    os.mkdir(historyDir)

  #--- determine date suffix for history file (YYYYMM) ----
  if reportingPeriod == None:
    raise Exception("System error: the DetermineReportableSitesFileToUse method requires a reporting period argument (YYYY/MM) which is missing")
  fileSuffix = reportingPeriod[0:4] + reportingPeriod[5:7]
  historyFile = historyDir + "/" + os.path.basename(filterFile) + "." + fileSuffix

  #--- update the history only if it is for the current month --
  currentPeriod = GetCurrentPeriod()
  if currentPeriod == reportingPeriod:
    if not os.path.isfile(historyFile):
      Logwarn("The %s files should be checked to see if any updates should be made to SVN/CVS in order to retain their history." % (historyDir))

    Logit("... updating the reportable sites configuration file: %s" % (historyFile))
    subprocess.call("cp -p %s %s" % (filterFile,historyFile),shell=True)

  #--- verify a history file exists for the time period. ---
  #--- if it does not, we don't want to update           ---
  if not os.path.isfile(historyFile):
    raise Exception("A reportable sites file (%s) does not exist for this time period.  We do not want to perform an update for this time period as it may not accurately reflect what was used at that time." % (historyFile))

  return historyFile

#----------------------------------------------
def GetConfigParams(filename):
  """ Generic reader of a file containing configuration parameters.
      The format of the file is 'parameter_name parameter value'.
      e.g.- GratiaHost gratia-db01.fnal.gov
      The method returns a hash table (dictionary in python terms).
  """
  try:
    params = {}
    fd = open(filename)
    while 1:
      line = fd.readline()
      if line == "":   # EOF
        break
      line = line.strip().strip("\n")
      if line.startswith("#"):
        continue
      if len(line) == 0:
        continue
      values = line.split()
      if len(values) <> 2:
        print >> sys.stderr, """Invalid config file entry (%(line)s) in file (%(filename)s)
... all entries must contain a value""" % \
                  { "line" : line, "filename" : filename,}
        sys.exit(1)
      params[values[0]]=values[1]
    fd.close()
    return params
  except IOError, (errno,strerror):
    print "... ERROR IOError\n"
    raise Exception("IO error(%s): %s (%s)" % (errno,strerror,filename))

#---------------------------------------------
def GetCurrentPeriod():
  """ Gets the current time in format for the date filter YYYY/MM 
      This will always be the current month.
  """
  return time.strftime("%Y/%m",time.localtime())
#-----------------------------------------------
def GetPreviousPeriod():
  """ Gets the previous time in format for the date filter YYYY?MM 
      This is done to handle the lag in getting all accounting data
      for the previous month in Gratia.  It will back off to the
      previous month from when this is run.
  """
  t = time.localtime(time.time())
  if t[1] == 1:
    prevMos = [t[0]-1,12,t[2],t[3],t[4],t[5],t[6],t[7],t[8],]
  else:
    prevMos = [t[0],t[1]-1,t[2],t[3],t[4],t[5],t[6],t[7],t[8],]
  return time.strftime("%Y/%m",prevMos)

#-----------------------------------------------
def GetCurrentTime():
  return time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())
#-----------------------------------------------
def Logit(message):
    LogToFile(GetCurrentTime() + " " + message)
#-----------------------------------------------
def Logerr(message):
    Logit("ERROR: " + message)
#-----------------------------------------------
def Logstderr(message):
    print >>sys.stderr,"FAILED: " + message
    Logit("ERROR: " + message)
#-----------------------------------------------
def Logwarn(message):
    Logit("WARNING: " + message)
    gWarnings.append(message)

#-----------------------------------------------
def LogToFile(message):
    "Write a message to the Gratia log file"
    file = None
    filename = ""
    try:
        filename = GetFileName(gParams["LogDir"],None,"log")
        file = open(filename, 'a')  
        file.write( message + "\n")
        if file != None:
          file.close()
    except IOError, (errno,strerror):
      raise Exception,"IO error(%s): %s (%s)" % (errno,strerror,filename)

#-----------------------------------------------
def GetFileName(dir,type,suffix):
    """ Sets the file name to YYYY-MM[.type].suffix based on the time
        period being processed, the directory, type and the 
        attribute of the filters configuration prepended to it.
    """
    if dir == None:
      print >> sys.stderr,"Failed in GetFileName method: directory (LogDir attribute) not specified"
      sys.exit(1)
    if type == None:
      qualifier = ""
    else:
      qualifier = "." + type
    if gDateFilter == None:
      filename = time.strftime("%Y-%m") + qualifier + "." + suffix
    else:
      filename = gDateFilter[0:4] + "-" + gDateFilter[5:7] + qualifier + "." + suffix 
    if not os.path.exists(dir):
      os.mkdir(dir)
    filename = dir + "/" + filename
    return filename

#-----------------------------------------------
def CheckGratiaDBAvailability():
  """ Checks the availability of the Gratia database. """
  Logit("Checking availability of Gratia database")
  connectString = CreateConnectString()
  pswdfile = CreatePswdFile()
  command = "mysql --defaults-extra-file=%s -e status %s " % (pswdfile,connectString)
  status, stdout, strerr = getstatusoutput(command)
  subprocess.call("rm -f " + pswdfile,shell=True) 
  if status == 0:
    msg =  "Status: \n" + stdout
    if stdout.find("ERROR") >= 0:
      msg = "Error in running mysql:\n  %s\nreturn code: %s\nstdout: %s\nstderr: %s\n" % (command,status,stdout,stderr)
      raise Exception(msg)
  else:
    msg = "Error in running mysql:\n  %s\nreturn code: %s\nstdout: %s\nstderr: %s\n" % (command,status,stdout,stderr)
    raise Exception(msg)
  Logit("Status: available")
      
#-----------------------------------------------
def SetDatesWhereClause():
  """ Sets the beginning and ending dates in a sql 'where' clause format
      to insure consistency on all queries. 
      This is always 1 month.
  """
  begin,end = SetDateFilter(1)
  strBegin =  DateToString(begin)
  strEnd   =  DateToString(end)
  whereClause = """ "%s" <= Main.EndTime and Main.EndTime < "%s" """ % (strBegin,strEnd)
  return whereClause

#-----------------------------------------------
def SetDateFilter(interval):
    """ Given the month and year (YYYY/MM, returns the starting and 
        ending periods for the query.  
        The beginning date will be offset from the date (in months) 
        by the interval provided. 
        The beginning date will always be the 1st of the month.
        The ending date will always be the 1st of the next month to
        insure a complete set of monthly data is selected.
    """
    # --- set the begin date compensating for year change --
    t = time.strptime(gDateFilter, "%Y/%m")[0:3]
    interval = int(interval)
    if t[1] < interval:
      new_t = (t[0]-1,13-(interval-t[1]),t[2])
    else:
      new_t = (t[0],t[1]-interval+1,t[2])
    begin = datetime.date(*new_t)
    # --- set the end date compensating for year change --
    if t[1] == 12:
      new_t = (t[0]+1,1,t[2])
    else:
      new_t = (t[0],t[1]+1,t[2])
    end = datetime.date(*new_t)
    return begin,end

#-----------------------------------------------
def DateToString(input,gmt=True):
    """ Converts an input date in YYYY/MM format local or gmt time """
    if gmt:
        return input.strftime("%Y-%m-%d 00:00:00")
    else:
        return input.strftime("%Y-%m-%d")

#-----------------------------------------------
def GetUserQuery(resource_grp,normalizationFactor,vos):
    """ Creates the SQL query DML statement for the Gratia database.
        grouping by Site/User/VO.
    """
    Logit("------ User Query: %s  ------" % resource_grp)
    return GetQuery(resource_grp,normalizationFactor,vos)

#-----------------------------------------------
def GetQuery(resource_grp,normalizationFactor,vos):
    """ Creates the SQL query DML statement for the Gratia database.
        On 5/18/09, this was changed to optionally add in CommonName
        to the query.  I chose to make it a python variable in this
        query so as not to replicate the rest of the query and take
        a chance on having it in 2 places to modify.  This is a bit
        of a gimmick but one I think is best.
        The DBflag argument, if True will allow CommonName to be included
        in the query and summary.
        On 11/04/09, this was changed to be the \"best\" of
        DistinguishedName and CommonName.

        IMPORTANT coding gimmick:
        For the ssm/activeMQ updates, the column labels MUST
        match those for the message format. They are being
        used to more easily (prgrammatically) provide the key
        for each value.
    """
    Logit("Resource Group: %(rg)s  Resources: %(resources)s NF: %(nf)s" % \
          { "rg" : resource_grp,
            "nf" : normalizationFactor,
            "resources" : GetSiteClause(resource_grp),
          })
    dates = gDateFilter.split("/")  # YYYY/MM format

    query="""\
SELECT "%(site)s"                  as Site,  
   VOName                          as "Group",
   min(UNIX_TIMESTAMP(EndTime))    as EarliestEndTime,
   max(UNIX_TIMESTAMP(EndTime))    as LatestEndTime, 
   "%(month)s"                     as Month,
   "%(year)s"                      as Year,
   IF(DistinguishedName NOT IN (\"\", \"Unknown\"),IF(INSTR(DistinguishedName,\":/\")>0,LEFT(DistinguishedName,INSTR(DistinguishedName,\":/\")-1), DistinguishedName),CommonName) as GlobalUserName, 
   Round(Sum(WallDuration)/3600)                        as WallDuration,
   Round(Sum(CpuUserDuration+CpuSystemDuration)/3600)   as CpuDuration,
   Round((Sum(WallDuration)/3600) * %(nf)s )            as NormalisedWallDuration,
   Round((Sum(CpuUserDuration+CpuSystemDuration)/3600) * %(nf)s) as NormalisedCpuDuration,
   Sum(NJobs) as NumberOfJobs 
from
     Site,
     Probe,
     VOProbeSummary Main
where
      Site.SiteName in (%(site_clause)s)
  and Site.siteid = Probe.siteid
  and Probe.ProbeName  = Main.ProbeName
  and Main.VOName in ( %(vos)s )
  and %(period)s
  and Main.ResourceType = "Batch"
group by Site,
         VOName,
         Month,
         Year, 
         GlobalUserName
""" % { "site"             : resource_grp,
        "site_clause"      : GetSiteClause(resource_grp),
        "nf"               : str(normalizationFactor),
        "month"            : dates[1],
        "year"             : dates[0],
        "vos"              : vos,
        "period"           : SetDatesWhereClause(),
}

    return query

#-----------------------------------------------
def GetSiteClause(resource_grp):
  global gInteropAccounting
  siteClause = ""
  resources = gInteropAccounting.interfacedResources(resource_grp)
  if len(resources) == 0:
    resources = [resource_grp]
  for resource in resources:
    siteClause = siteClause + '"%s",' % resource 
  siteClause = siteClause[0:len(siteClause)-1]
  return siteClause


#-----------------------------------------------
def GetQueryForDaysReported(resource_grp,resource):
    """ Creates the SQL query DML statement for the Gratia database.
        This is used to determine if there are any gaps in the
        reporting for a site. It just identifies the days that
        data is reported for the site and period (only works if its a month).
    """
    userDataClause=""
    userGroupClause=""
    periodWhereClause = SetDatesWhereClause()
    dateFmt  =  "%Y-%m-%d"
    Logit("---- Gratia days reported query - Resource Group: %(rg)s  Resource: %(resource)s ----" % \
       { "rg" : resource_grp, "resource" : resource } )
    query="""\
SELECT distinct(date_format(EndTime,"%(date_format)s"))
from 
     Site,
     Probe,
     VOProbeSummary Main 
where 
      Site.SiteName = "%(resource)s" 
  and Site.siteid = Probe.siteid 
  and Probe.ProbeName  = Main.ProbeName 
  and %(period)s 
  and Main.ResourceType = "Batch"
""" % { "date_format"  : dateFmt,
        "resource"     : resource,
        "period"       : periodWhereClause
      }
    return query

#-----------------------------------------------
def RunGratiaQuery(select,LogResults=True,headers=False):
  """ Runs the query of the Gratia database """
  host = gDbParams["GratiaHost"]
  port = gDbParams["GratiaPort"] 
  db   = gDbParams["GratiaDB"]
  Logit("Running query on %s:%s of the %s db" % (host,port,db))
  connectString = CreateConnectString(headers)
  pswdfile = CreatePswdFile()
  (status,output,stderr) = getstatusoutput("echo '" + select + "' | mysql --defaults-extra-file=" + pswdfile + " " + connectString)
  subprocess.call("rm -f " + pswdfile,shell=True)
  results = EvaluateMySqlResults((status,output))
  if len(results) == 0:
    cnt = 0
  elif headers:
    cnt = len(results.split("\n")) - 1
  else:
    cnt = len(results.split("\n")) 
  Logit("Results: %s records" % cnt)
  if LogResults:
    Logit("Results:\n%s" % results)
  return results

#-----------------------------------------------
def FindTierPath(table):
  """ The path in the org_Tier1/2 table keeps changing so we need to find it
      using the top level name which does not appear to change that
      frequently.
  """
  Logit("... finding path in table %s" % table)
  type = "data"
  if table == "org_Tier1":
    query = 'select Path from org_Tier1 where Name in ("US-FNAL-CMS","US-T1-BNL")'
  elif  table == "org_Tier2":
    query = 'select Path from org_Tier2 where Name in ("USA","Brazil")'
  else:
    Logerr("System error: method(FindTierPath) does not support this table (%s)" % (table))
  results = RunLCGQuery(query,type)
  if len(results) == 0:
    Logit("Results: None")
  else:
    LogToFile("Results:\n%s" % results)
  whereClause = "where "
  lines = results.split("\n")
  for i in range (0,len(lines)):
    if i > 0:
      whereClause = whereClause + " or "
    whereClause = whereClause + " Path like \"%s" % lines[i] + "%\""
  return whereClause + " order by Path"

#-----------------------------------------------
def WriteFile(data,filename):
  file = open(filename, 'w')
  file.write(data+"\n")
  file.close()

#-----------------------------------------------
def RunLCGUpdate(type):
  """ Performs the update of the APEL database """
  configfile = gParams["SSMConfig"]
  ssm_file   = gParams["SSMFile"]

  Logit("---------------------")
  Logit("--- Updating APEL ---")
  Logit("---------------------")
  dir = gParams["UpdatesDir"]
  if type == "delete":
    file = GetFileName(dir,gParams["DeleteFileName"],"txt")
    if not os.path.isfile(file):
      Logit("... this is likely the 1st time run for this period therefore no file to send")
      return
  if type == "update":
    file = GetFileName(dir,gParams["UpdateFileName"],"txt")
  Logit("%(type)s file... %(file)s Records: %(count)s" % \
         { "type"   : type,
           "file"   : file,
           "count"  : getoutput("grep -c '%%' %s" % file), 
         } )
  try:
    ssm = SSMInterface.SSMInterface(configfile,ssm_file)
    ssm.send_file(file)
  except SSMInterface.SSMException,e:
    raise Exception(e)
 
  if ssm.outgoing_sent(): 
    Logit("... successfulling sent")
  else:
    raise Exception("""SSM Interface failed. These files still exist:
%s""" % ssm.show_outgoing())
  Logit("------------------------------")
  Logit("--- Updating APEL complete ---")
  Logit("------------------------------")

#------------------------------------------------
def CreatePswdFile():
    file = None
    filename = gParams["TmpDataDir"] + "/.pswd"
    filedata = """[client]\npassword=%s\n"""  % gDbParams["GratiaPswd"]
    try:
        file = open(filename, 'w')
        file.write(filedata)
        if file != None:
          file.close()
        subprocess.call("chmod 0400 " + filename,shell=True)
    except IOError, (errno,strerror):
      raise Exception,"IO error(%s): %s (%s)" % (errno,strerror,filename)
    return filename

#------------------------------------------------
def CreateConnectString(headers=False):
  args = ""
  if not headers:
    args = " --disable-column-names" 
  return " %(args)s -h %(host)s --port=%(port)s -u %(user)s %(db)s " % \
      {  "host" : gDbParams["GratiaHost"], 
         "port" : gDbParams["GratiaPort"], 
         "db"   : gDbParams["GratiaDB"], 
         "user" : gDbParams["GratiaUser"],  
         "args" : args,
      }

#------------------------------------------------
def EvaluateMySqlResults((status,output)):
  """ Evaluates the output of a MySql execution based on the 
      getstatusoutput command.
      The latest upgrade of MySql to v5.1 appears to be adding a
      newline to the end of the output where it never did in the past.
      Or it is the use of the subprocess module.
      Either way We need to remove it.
  """
  if status == 0:
    if output.find("ERROR") >= 0 :
      raise Exception("MySql error:  %s" % (output))
  else:
    raise Exception("Status (non-zero rc): rc=%d - %s " % (status,output))
  if output == "NULL": 
    return ""
  if len(output) == 0: 
    return ""
  #-- remove newline from end of output --
  if output[len(output)-1] == "\n":
    output = output[0:len(output)-1]
  return output

#-----------------------------------------------
def CreateVOSummary(results,reportableSites):
  """ Creates a summary by site,vo for troubleshooting purposes. """
  Logit("-----------------------------------------------------")
  Logit("-- Creating a resource group, vo summary html page --") 
  Logit("-----------------------------------------------------")
  currentTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())
  metrics = [ "NumberOfJobs",
             "CpuDuration", 
             "WallDuration", 
             "NormalisedCpuDuration", 
             "NormalisedWallDuration", 
            ]
  headers = { "NumberOfJobs"          : "Jobs",
             "CpuDuration"            : "CPU<br>(hours)", 
             "WallDuration"           : "Wall<br>(hours)", 
             "NormalisedCpuDuration"  : "Normalized CPU<br>(hours)", 
             "NormalisedWallDuration" : "Normalized Wall<br>(hours)", 
            }
  totals = {}
  totals = totalsList(metrics)
  resourceGrp = None
  vo          = None
  dir = gParams["WebLocation"]
  htmlfilename = GetFileName(dir,"summary","html")
  datfilename  = GetFileName(dir,"summary","dat")
  Logit("... summary html file: %s" % htmlfilename) 
  Logit("... summary dat  file: %s" % datfilename) 
  htmlfile    = open(htmlfilename,"w")
  summaryfile = open(datfilename,"w")
  htmlfile.write("""<HTML><BODY>\n""")
  htmlfile.write("Last update: " + time.strftime('%Y-%m-%d %H:%M',time.localtime()) + "<BR/>")
  htmlfile.write("Host: " + getoutput("hostname -f") + "<BR/>")
  htmlfile.write("User: " + getoutput("whoami"))
  htmlfile.write("""<TABLE border="1">""")
  htmlfile.write("""<TR>""")
  htmlfile.write("""<TH align="center">Resource Group</TH>""")
  htmlfile.write("""<TH align="center">NF<br>HS06</TH>""")
  htmlfile.write("""<TH align="center">VO</TH>""")
  for metric in metrics:
    htmlfile.write("""<TH align="center">%s</TH>""" % headers[metric])
  htmlfile.write("""<TH align="center">Earliest<br>Date</TH>""")
  htmlfile.write("""<TH align="center">Latest<br>Date</TH>""")
  htmlfile.write("""<TH align="center">Measurement Date</TH>""")
  htmlfile.write("""<TH align="center">EGI<br>Accounting Name</TH>""")
  htmlfile.write("""<TH align="left">Resources / Gratia Sites</TH>""")
  htmlfile.write("""<TH align="center">Month</TH>""")
  htmlfile.write("""<TH align="center">Year</TH>""")
  htmlfile.write("</TR>\n")

  lines = results.split("\n")
  for i in range (0,len(lines)):
    values = lines[i].split('\t')
    if len(values) < 12:
      continue
    if i == 0:  # creating label list for results
      label = []
      for val in values:
        label.append(val)
      continue
    if label[0] == values[0]:  # filtering out column headings
      continue
    if values[0] != resourceGrp or values[1] != vo:
      if resourceGrp == None:  # first time
        resourceGrp = values[0]
        vo          = values[1]
        earliest    = values[2]
        latest      = values[3]
        month       = values[4]
        year        = values[5]
        nf          = reportableSites[resourceGrp]
      else:  # new resource group / vo .. write the previous one
        writeHtmlLine(htmlfile, resourceGrp, vo, nf, totals, metrics, earliest, latest, currentTime, month, year)
        writeSummaryFile(summaryfile, resourceGrp, vo, nf, totals, metrics, earliest, latest, currentTime, month, year)
        #-- new one ---
        resourceGrp = values[0]
        vo          = values[1]
        earliest    = values[2]
        latest      = values[3]
        month       = values[4]
        year        = values[5]
        nf          = reportableSites[resourceGrp]
        totals = totalsList(metrics) # reset totals to zero

    #-- want to find the earliest and latest for resource group and vo
    if values[2] < earliest:
      earliest = values[2]
    if values[3] > latest:
      latest   = values[3]
    #-- accumulate totals ---
    idx = 0
    for val in values:  # accumulate totals
      if label[idx] in metrics:
        totals[label[idx]] = totals[label[idx]] + int(val)
      idx = idx + 1

  #-- write out the last one
  writeHtmlLine(htmlfile, resourceGrp, vo, nf, totals, metrics, earliest, latest, currentTime, month, year)
  writeSummaryFile(summaryfile, resourceGrp, vo, nf, totals, metrics, earliest, latest, currentTime, month, year)
  htmlfile.write("</TABLE></BODY></HTML>\n")

  htmlfile.close()
  summaryfile.close()

#--------------------------------
def totalsList(metrics):
  totalsDict = {}
  for metric in metrics:
    totalsDict[metric] = 0 
  return totalsDict
#--------------------------------
def writeHtmlLine(file, rg, vo, nf, totals, metrics, earliest, latest, currentTime,month,year):
  global gRebus
  file.write("""<TR><TD>%s</TD><TD align="center">%s</TD><TD align="center">%s</TD>""" % (rg,nf,vo))
  for metric in metrics:
    file.write("""<TD align="right">""" + str(totals[metric])         + "</TD>")
  file.write("""<TD>"""  + time.strftime("%Y-%m-%d", time.gmtime(float(earliest))) + "</TD>")
  file.write("""<TD>"""  + time.strftime("%Y-%m-%d", time.gmtime(float(latest)))   + "</TD>")
  file.write("""<TD align="center">"""  + currentTime                 + "</TD>")
  if gRebus.isRegistered(rg):
    file.write("""<TD>"""                 + gRebus.accountingName(rg)   + "</TD>")
  else:
    file.write("""<TD><font color="red"><b>Not Registered</b></font></TD>""")
  file.write("""<TD>"""                 + GetSiteClause(rg)           + "</TD>")
  file.write("""<TD align="center">"""  + month                       + "</TD>")
  file.write("""<TD align="center">"""  + year                        + "</TD>")
  file.write("""</TR>\n""")

#--------------------------------
def writeSummaryFile(file, rg, vo, nf, totals, metrics, earliest, latest, currentTime,month,year):
  global gRebus
  line = "%s\t%s\t%s" % (rg,nf,vo)
  for metric in metrics:
    line += "\t" + str(totals[metric])
  line += "\t"   + time.strftime("%Y-%m-%d", time.gmtime(float(earliest)))
  line += "\t"   + time.strftime("%Y-%m-%d", time.gmtime(float(latest)))
  line += "\t"   + currentTime
  line += "\t"   + gRebus.accountingName(rg)
  line += "\t"   + GetSiteClause(rg)
  line += "\t"   + month
  line += "\t"   + year
  file.write(line + "\n")
  Logit("SUMMARY: " + line)

#-----------------------------------------------
def CreateLCGssmUpdates(results):
  """ Creates the SSM summary job records for the EGI portal."""
  Logit("-----------------------------------------------------")
  Logit("--- Creating SSM update records for the EGI portal --") 
  Logit("-----------------------------------------------------")
  if len(results) == 0:
    raise Exception("No updates to apply")
  ssmHeaderRec = "APEL-summary-job-message: v0.2\n"
  ssmRecordEnd = "%%\n"
  dir = gParams["UpdatesDir"]
  filename  =  GetFileName(dir,gParams["UpdateFileName"]  ,"txt")
  deletions =  GetFileName(dir,gParams["DeleteFileName"],"txt")

  Logit("... update file: %s" % filename) 
  Logit("... delete file: %s" % deletions) 
  updates = open(filename,  'w')
  deletes = open(deletions, 'w')
  updates.write(ssmHeaderRec)
  deletes.write(ssmHeaderRec)

  lines = results.split("\n")
  for i in range (0,len(lines)):  
    values = lines[i].split('\t')
    if len(values) < 12:
      continue
    if i == 0:  # creating label list for results
      label = []
      for val in values:
        label.append(val)
      continue
    if label[0] == values[0]:  # filtering out column headings
      continue
    idx = 0
    for val in values:
      updates.write("%(label)s: %(value)s\n"  %  { "label" : label[idx], "value" : val, })
      #-- create a file that will zero out all entries (to be used like a delete) ---
      if label[idx] in ["WallDuration","CpuDuration","NormalisedWallDuration","NormalisedCpuDuration","NumberOfJobs"]:
        deletes.write("%(label)s: %(value)s\n"  %  { "label" : label[idx], "value" : 0, })
      else:
        deletes.write("%(label)s: %(value)s\n"  %  { "label" : label[idx], "value" : val, })
      idx = idx + 1
    updates.write(ssmRecordEnd)
    deletes.write(ssmRecordEnd)
  updates.close()
  deletes.close()

#-----------------------------------------------
def RetrieveUserData(reportableVOs,reportableSites):
  """ Retrieves Gratia data for reportable sites """
  Logit("--------------------------------------------")
  Logit("---- Gratia user data retrieval started ----")
  Logit("--------------------------------------------")
  global gUserQuery
  global gInteropAccounting
  output = ""
  firstTime = 1
  resource_grps = sorted(reportableSites.keys())
  for resource_grp in resource_grps:
    normalizationFactor = reportableSites[resource_grp]
    query = GetUserQuery(resource_grp,normalizationFactor,reportableVOs)
    if firstTime:
      gUserQuery = query
      Logit("Query:")
      LogToFile(query)
      firstTime = 0
    results = RunGratiaQuery(query,LogResults=False,headers=True)
    if len(results) == 0:
      results = ProcessEmptyResultsSet(resource_grp,reportableVOs)
    output += results + "\n"
  Logit("---------------------------------------------")
  Logit("---- Gratia user data retrieval complete ----")
  Logit("---------------------------------------------")
  return output

#-----------------------------------------------
def ProcessEmptyResultsSet(resource_grp,reportableVOs):
  """ Creates an update for each reportable VO with no Gratia
      data from query. 
      The purpose of this is to indicate in the APEL table that
      the site was processed.
  """
  gSitesWithNoData.append(resource_grp)
  output      = ""
  year        = gDateFilter.split("/")[0]
  month       = gDateFilter.split("/")[1]
  currentTime = calendar.timegm((int(year), int(month), 1, 0, 0, 0, 0, 0, 0))

  for vo in reportableVOs.split(","):
    output += "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % \
        ( resource_grp, vo.strip('"'),
          currentTime, currentTime,
          month, year,
          "None",
          "0", "0", "0", "0", "0")
  return output

#-----------------------------------------------
def ProcessUserData(ReportableVOs,ReportableSites):
  """ Retrieves and creates the DML for the new (5/16/09) Site/User/VO summary
      data for the APEL interface.
  """
  gUserOutput = RetrieveUserData(ReportableVOs,ReportableSites)
  CreateLCGssmUpdates(gUserOutput)
  CreateVOSummary(gUserOutput,ReportableSites)

#-----------------------------------------------
def CheckMyOsgInteropFlag(reportableSites):
  """ Checks to see for mismatches between the reportable sites config
      file and MyOsg.  Include in email potential problems.
      If the Rebus site is not availble and a previous csv does not exist, 
      we want to terminate since we cannot create the needed files for the 
      WLCG reporting site.
  """
  Logit("-------------------------------------------------------")
  Logit("---- Checking against MyOsg InteropAccounting flag ----")
  Logit("---- and against the WLCG REBUS Topology           ----")
  Logit("-------------------------------------------------------")
  global gInteropAccounting
  global gRebus
  if not gRebus.wasAccessible():
    if gRebus.isAvailable():
      Logwarn("""The WLCG REBUS topology was not accessible today. We are using a previous days data for validations.""")
    else:
      raise Exception("""The WLCG REBUS topology was not accessible today and there is not previous days cvs file to use. 
We cannot provide the correct data for OSG WCLG reporting. No updates today.""")

  myosgRGs = gInteropAccounting.interfacedResourceGroups()

  #-- for resource groups we are reporting, see if registered in MyOsg and Rebus
  for rg in reportableSites:
    msg = "Resource group (%s) is being reported" % rg
    #--- check MyOsg --
    if gInteropAccounting.isRegistered(rg):
      msg += " and is registered in MyOSG/OIM"
      if gInteropAccounting.isInterfaced(rg):
        msg += " and has resources (%s) with the InteropAccounting flag set in MyOsg" %  gInteropAccounting.interfacedResources(rg)
        #-- check Rebus ---
        if gRebus.isAvailable():
          if gRebus.isRegistered(rg):
            if gRebus.accountingName(rg) != gInteropAccounting.WLCGAcountingName(rg):
              Logwarn("Resource group %(rg)s MyOsg AccountingName (%(myosg)s) does NOT match the REBUS Accounting Name (%(rebus)s)" % \
               { "rg"     : rg,
                 "rebus"  : gRebus.accountingName(rg), 
                 "myosg"  : gInteropAccounting.WLCGAcountingName(rg)})
          else:
            Logwarn("%s and is NOT registered in REBUS" % msg)
      else:
        msg += " BUT has NO resources with the InteropAccounting flag set in MyOsg"
        if gRebus.isAvailable():
          if gRebus.isRegistered(rg):
            Logwarn("%s and IS registered in REBUS" % msg)
          else:
            Logwarn("%s and is NOT registered in REBUS" % msg)
        else:
            Logwarn(msg)
    else:
      msg += " and is NOT registered in MyOSG/OIM" 
      #-- check Rebus ---
      if gRebus.isAvailable():
        if gRebus.isRegistered(rg):
          Logwarn("%s and is registered in Rebus" % msg)
        else:
          Logwarn("%s and is NOT registered in Rebus" % msg)
      else:
        Logwarn(msg)

  #-- for MyOsg resource groups with Interop flag set, see if we are reporting
  #-- and if they have been registered in Rebus
  for rg in myosgRGs:
    if rg not in reportableSites:
      msg = "Resource group (%(rg)s) is NOT being reported BUT HAS resources (%(resources)s) with the InteropAccounting flag set in MyOsg" % \
            { "rg"       : rg, 
             "resources" : gInteropAccounting.interfacedResources(rg), }
      if gRebus.isAvailable():
        if gRebus.isRegistered(rg):
          Logwarn("%s but IS registered in REBUS as %s" % (msg,gRebus.tier(rg)))
        else:
          Logwarn("%s and is NOT registered in REBUS" % msg)
      else:
        Logwarn(msg)

  Logit("-----------------------------------------------------------------")
  Logit("---- Checking against MyOsg InteropAccounting flag completed ----")
  Logit("-----------------------------------------------------------------")

#-----------------------------------------------
def CheckForUnreportedDays(reportableVOs,reportableSites):
  """ Checks to see if any sites have specific days where no data is
      reported.  If a site is off-line for maintenance, upgrades, etc, this
      could be valid.  There is no easy way to check for this however.
      So the best we can do is check for this condition and then manually
      validate by contacting the site admins.
      On 12/10/09, another condition arose.  If a site goes inactive at 
      anytime, then all downtimes for that site are never available in
      MyOsg.  So, best we can do under those circumstances is 'pretend'
      any missing days were after the site went inactive and not raise
      any alarm.
  """
  global gSitesMissingData 
  global gMyOSG_available
  global gInteropAccounting
  daysMissing = int(gParams["MissingDataDays"])
  Logit("-------------------------------------------")
  Logit("---- Check for unreported days started ----")
  Logit("-------------------------------------------")
  Logit("Starting checking for resources that are missing data for more than %d days" % (daysMissing))
  output = ""
  firstTime = 1

  #-- Using general query to see all dates reported for the period --
  periodWhereClause = SetDatesWhereClause()
  endTimeFmt = "%Y-%m-%d"
  resourceGroups = reportableSites.keys()
  query="""select distinct(date_format(EndTime,"%s")) from VOProbeSummary Main where %s """ % (endTimeFmt,periodWhereClause)
  dateResults = RunGratiaQuery(query,LogResults=False,headers=False)
  Logit("Available dates reported in Gratia: " + str(dateResults.split("\n"))) 

  #-- we need to filter out future dates as there are a few probes --
  #-- that are sending future dates to Gratia periodically.        --
  reportableDates = []
  today = str(datetime.date.today())
  for thisdate in dateResults.split("\n"):
    if thisdate < today:
      reportableDates.append(thisdate)
  Logit("Available dates used: " + str(reportableDates))

  #-- now checking for each resource within a resource groups ---
  missingDataList = []
  for resourceGroup in sorted(resourceGroups):
    resources = gInteropAccounting.interfacedResources(resourceGroup)
    for resource in sorted(resources):
      who = "Resource: %(r)s in Resource Group: %(rg)s " % \
               { "rg" : resourceGroup, "r" : resource } 
      allDates = reportableDates[:]
      query = GetQueryForDaysReported(resourceGroup,resource)
      if firstTime:
          Logit("Sample Query:")
          LogToFile(query)
          firstTime = 0
      results = RunGratiaQuery(query,LogResults=False,headers=False)

      #--- determine if any days are missing
      reportedDates = []
      if len(results) > 0:
        reportedDates = results.split("\n")
      Logit("Reported dates: " + str(reportedDates))
      for i in range (0,len(reportedDates)):  
        if reportedDates[i] < today: #needed due to sites reporting future dates
          allDates.remove(reportedDates[i])
      if  len(allDates) > 0: 
        missingDataList.append("")
        missingDataList.append("%s (missing days): %s" %  (who, str(allDates)))

        #--- see if dowmtime for those days was scheduled ---
        if gMyOSG_available:
          try:
            shutdownDays = CheckForShutdownDays(resource,allDates)
            missingDataList.append("%s (shutdown days): %s" % (who, str(shutdownDays)))
            for i in range (0,len(shutdownDays)):  
              allDates.remove(shutdownDays[i])
            #--- see if the resource is inactive ----
            # this keeps it from being reported as missing data
            # as this is another means of marking downtimes per the goc
            if  len(allDates) > daysMissing: 
              if inactives.resource_is_inactive(resource):
                missingDataList.append("%s is marked as inactive in MyOsg" % who)
                allDates = []  
          except Exception, e:
            allDates.append("WARNING: Unable to determine planned shutdowns - MyOSG error (" + str(sys.exc_info()[1]) +")" )
            gMyOSG_available = False   
        else: 
          allDates.append("WARNING: Unable to determine planned shutdowns - MyOSG error (" + str(sys.exc_info()[1]) +")" )

        #--- see if we have any missing days now ----
        if  len(allDates) > daysMissing: 
          missingDataList.append("%s is missing data for %d days" % (who,len(allDates)))
          gSitesMissingData[resource] = allDates
          Logwarn("%s missing data for more than %d days: " % \
               ( who, daysMissing) + str(gSitesMissingData[resource]))
      #--- end of resource loop ----
    # --- end of resourceGroup loop ---
  #--- create html file of missing data ---
  CreateMissingDaysHtml(missingDataList)

  #--- see if any need to be reported ---
  if len(gSitesMissingData) == 0:
    Logit("No sites/resources had missing data for more than %d days" % (daysMissing))

  Logit("Ended checking for sites/resources that are missing data for more than %d days" % (daysMissing))
  Logit("---------------------------------------------")
  Logit("---- Check for unreported days completed ----")
  Logit("---------------------------------------------")

#-----------------------------------------
def CreateMissingDaysHtml(missingData):
  """ Creates an html file of those sites that are missing data for
      any days during the period.
  """
  file = None
  filename = ""
  try:
    dir = gParams["WebLocation"]
    filename = GetFileName(dir,"missingdays","html") 
    Logit("#--------------------------------#")
    Logit("Creating html file of sites with missing data: %s" % filename)
    file = open(filename, 'w')  
    file.write("<html>\n")
    period = "%s-%s" % (gDateFilter[0:4],gDateFilter[5:7])
    file.write("<title>Gratia - APEL/LCG Interface (Missing data for %s</title>\n" % period)
    file.write("<head><h2><center><b>Gratia - APEL/LCG Interface<br/> (Missing data for %s)</b></center></h2></head>\n" % (period)) 
    file.write("<body><hr width=\"75%\" /><pre>\n")
    if len(missingData) > 0:
      for line in range (0,len(missingData)):
        Logit(missingData[line])
        file.write( missingData[line] + "\n")
    else:
      Logit("All sites reporting for all days in time period")
      file.write("\nAll sites reporting for all days in time period\n")
    file.write("</pre><hr width=\"75%\" />\n")
    file.write("Last update %s\n" % (time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())))
    file.write("</body></html>\n")
    if file != None:
      file.close()
  except IOError, (errno,strerror):
    raise Exception,"IO error(%s): %s (%s)" % (errno,strerror,filename)
  # ---- send to collector -----
  ## SendXmlHtmlFiles(filename,gParams["WebLocation"])

#-----------------------------------------
def CheckForShutdownDays(site,missingDays):
  """ Determines if a site had a scheduled shutdown for the days that
      accounting data was unreported. 
  """
  shutdownDates = []
  for i in range (0,len(missingDays)):  
    day = missingDays[i]
    if downtimes.site_is_shutdown(site,day,"CE"):
      shutdownDates.append(day)
  return shutdownDates


#--- MAIN --------------------------------------------
def main(argv=None):
  global gWarnings
  global gSitesWithNoData
  global gVoQuery
  global gUserQuery
  global gVoOutput
  global gUserOutput

  #--- get command line arguments and parameter file arguments  -------------
  try:
    old_umask = os.umask(002)  # set so new files are 644 permissions
    GetArgs(argv)
  except Exception, e:
    print >>sys.stderr, "ERROR: " + e.__str__()
    return 1

  try:      
    GetFilterConfigParams(gFilterConfigFile)
    GetDBConfigParams(gParams["DBConfFile"])
    Logit("====================================================")
    Logit("Starting transfer from Gratia to APEL")
    Logit("Filter date............ %s" % (gDateFilter))
    gParams["SiteFilterFile"] = DetermineReportableSitesFileToUse(gDateFilter)
    Logit("Reportable sites file.. %s" % (gParams["SiteFilterFile"]))
    Logit("Reportable VOs file.... %s" % (gParams["VOFilterFile"]))
    Logit("Gratia database host... %s:%s" % (gDbParams["GratiaHost"],gDbParams["GratiaPort"]))
    Logit("Gratia database........ %s" % (gDbParams["GratiaDB"]))
    Logit("Web location........... %s" % (gParams["WebLocation"]))
    Logit("Log location........... %s" % (gParams["LogDir"]))
    Logit("Missing days threshold. %s" % (gParams["MissingDataDays"]))
    Logit("SSM module............. %s" % (gParams["SSMFile"]))
    Logit("SSM config file........ %s" % (gParams["SSMConfig"]))
    Logit("SSM updates dir file... %s" % (gParams["UpdatesDir"]))
    Logit("SSM update file........ %s" % (GetFileName(gParams["UpdatesDir"],gParams["UpdateFileName"]  ,"txt")))
    Logit("SSM delete file........ %s" % (GetFileName(gParams["UpdatesDir"],gParams["DeleteFileName"],"txt")))

    #--- check db availability -------------
    CheckGratiaDBAvailability()

    #--- get all filters -------------
    ReportableSites    = GetSiteFilters(gParams["SiteFilterFile"])
    ReportableVOs      = GetVOFilters(gParams["VOFilterFile"])

    #--- Perform Rebus topology checks ---
    CheckMyOsgInteropFlag(ReportableSites)
   
    #--- process deletions ---
    if gInUpdateMode:
      RunLCGUpdate("delete")
    
    ProcessUserData(ReportableVOs,ReportableSites)
    CheckForUnreportedDays(ReportableVOs,ReportableSites)

    #--- apply the updates to the APEL accounting database ----
    if gInUpdateMode:
      RunLCGUpdate("update")
      SendEmailNotificationSuccess()
      SendEmailNotificationWarnings()
      Logit("Transfer Completed SUCCESSFULLY from Gratia to APEL")
    else:
      Logit("The --update arg was not specified. No updates attempted.")
    Logit("====================================================")

  except Exception, e:
    Logerr(e.__str__())
    SendEmailNotificationFailure(e.__str__())
    Logit("Transfer FAILED from Gratia to APEL.")
#    traceback.print_exc() #JGW uncomment when needed to test
    Logit("====================================================")
    return 1

  return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv))

