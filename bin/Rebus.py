#!/usr/bin/python

import os, sys, time, string
import subprocess
import getopt
import traceback
import csv
import urllib2
import exceptions

class RebusException(Exception):
  pass

class Rebus:
  """ Author: John Weigand (10/4/11)
      Description:
        This class retrieves the latest WLCG Rebus topology csv file
        and provides various methods for viewing/using the data.
  """

  #############################
  def __init__(self,verbose=False):
    self.location = "http://gstat-wlcg.cern.ch/apps/topology/all/csv"
    self.accountingNameDict = {}
    self.siteDict           = {}
    self.csvfile            = "Rebus.csv"
    self.verbose            = verbose # show errors on command line execution only
    self.isAccessible       = False # indicates if Rebus was available for this invocation. If not, will use existing csv if it exists.
    self.alreadyTried       = False # so we only do it once
    self.rebus              = None  # instance of Rebus retrieval
 
    self.headerDict1 = { "tier"     : "Tier",
                         "acctname" : "Accounting Name",
                         "site"     : "Site", }
    self.headerDict2 = { "tier"     : "-----",
                         "acctname" : "---------------",
                         "site"     : "-----", }

  #############################
  def __getRebus__(self):
    if self.alreadyTried:
       return   # only want to do this once
    self.alreadyTried = True
    try:
      # the wget creates an empty file if it fails. we want to preserve the old one
      tmp_csv = self.csvfile + ".tmp"
      cmd = 'wget -t1 -O %s "%s"' % (tmp_csv,self.location)
      rtn = subprocess.call("%s >/dev/null 2>&1" % cmd,shell=True)
      if rtn == 0:
        self.isAccessible = True
        subprocess.call("mv %s %s" % (tmp_csv, self.csvfile),shell=True) # use the new one
      else:
        subprocess.call("rm -f %s" % (tmp_csv),shell=True) # clean up
        self.isAccessible = False
        if self.verbose: 
          raise RebusException("ERROR: Problem performing: %s" % cmd)
    except:
      raise

    #-- see if a file exists ----
    if not os.path.isfile(self.csvfile):
      if self.verbose:
        raise RebusException("ERROR: csv file from REBUS not found: %s" % self.csvfile)
      # this is so no errors are shown when called from another module
      self.rebus = None
      return # No data.  New or old

    #-- read the existing csv file ----
    try:
      self.rebus = csv.reader(open(self.csvfile), delimiter=",")
    except:
      self.rebus = None
      if self.verbose:
        raise RebusException("ERROR: Problem reading csv file: %s" % self.csvfile)
      return
    for line in self.rebus:
      self.__populateDicts__(line)

##################################
# Could be useful maybe
#      if line[5] == "OSG":
#        self.__populateDicts__(line)
#        continue
#      if line[1] == "USA":
#        self.__populateDicts__(line)
#        continue
##################################

  #############################
  def __populateDicts__(self,line):
    tier           = line[0]
    country        = line[1]
    federation     = line[2]
    accountingName = line[3]
    site           = line[4]
    infrastructure = line[5]
    tmpDict =  {  "tier"           : line[0],
                  "country"        : line[1],
                  "federation"     : line[2],
                  "accountingName" : line[3],
                  "site"           : [ line[4],],
                  "infrastructure" : line[5],
                }
    if accountingName in self.accountingNameDict:
      self.accountingNameDict[accountingName]["site"].append(site)
    else:
      self.accountingNameDict[accountingName] = tmpDict

#    if site in self.siteDict:
#      print "ERROR: site (%(site)s) defined in multiple heirarchies" % \
#              { "site" : site }
#    else:
#      self.siteDict[site] = tmpDict
    self.siteDict[site] = tmpDict
      
  #############################
  def wasAccessible(self):
    self.__getRebus__()
    return self.isAccessible
      
  #############################
  def isAvailable(self):
    self.__getRebus__()
    if self.rebus == None:
      return False
    return True
      
  #############################
  def showAccountingNames(self):
    self.__getRebus__()
    format =  "%(tier)-6s %(acctname)-20s %(site)s" 
    print format % self.headerDict1
    print format % self.headerDict2
    for name in sorted(self.accountingNameDict.keys()):
      print format % { "tier"     : self.accountingNameDict[name]["tier"], 
                       "acctname" : name,
                       "site"     : self.accountingNameDict[name]["site"], }

  #############################
  def showSite(self, site = "all"):
    self.__getRebus__()
    format = "%(site)-20s %(acctname)-20s %(tier)s" 
    print format % self.headerDict1
    print format % self.headerDict2
    if site == "all":
      names = sorted(self.siteDict.keys())
    else:
      names = [site,]
    for name in names:
      print format % { "tier"     : self.siteDict[name]["tier"], 
                       "acctname" : self.siteDict[name]["accountingName"],
                       "site"     : name, }

  #############################
  def isRegistered(self,site):
    """ Returns Trues if a resource group/site is registered in the WCLG."""
    self.__getRebus__()
    if site in self.siteDict:
      return True
    return False

  #############################
  def accountingName(self,site):
    """ Returns the WLCG REBUS Federation Accounting Name for a 
        registered resource group/site.
        If not registered, it will return an empty string.
    """
    self.__getRebus__()
    if self.isRegistered(site):
      return self.siteDict[site]["accountingName"]
    return ""

  #############################
  def tier(self,site):
    self.__getRebus__()
    if self.isRegistered(site):
      return self.siteDict[site]["tier"]
    return ""

## end of class ###

#----------------
def usage():
  global gProgramName
  print """
Usage: %(program)s action [-help]

  Provides visibility into the WLCG Rebus topology for use in the
  Gratia/APEL/WLCG interface.

  Actions:
    --show all | accountingnames | sites
        Displays the Rebus topology for the criteria specificed
    --is-registered SITE
        Shows information for a site registered in WLCG REBUS topology
    --is-available 
        Shows status of query against Rebus url.
"""

#----------------
def main(argv):
  global gProgramName
  gProgramName = argv[0]

  action = ""
  type   = ""
  arglist = [ "help", "show=", "is-registered=", "is-available", ]
  try:
    opts, args = getopt.getopt(argv[1:], "", arglist)
    if len(opts) == 0:
      usage()
      print "ERROR: No command line arguments specified"
      return 1
    for o, a in opts:
      if o in ("--help"):
        usage()
        return 1
      if o in ("--is-available"):
        action = o
        continue
      if o in ("--is-registered"):
        action = o
        site   = a
        continue
      if o in ("--show"):
        action = o
        type = a
        if type not in ["all","accountingnames","sites",]:
          usage()
          print "ERROR: Invalid arg of --show: %s" % type
          return 1
        continue

    rebus = Rebus(verbose=True)
    if action == "--show":
      if type == "all":
        rebus.showAccountingNames()
      if type == "sites":
        rebus.showSite()
    elif action == "--is-registered":
      if not rebus.isRegistered(site):
        print "Site (%s) is not registered in Rebus" % site
      else:
        rebus.showSite(site)
    elif action == "--is-available":
      if rebus.isAvailable():
        print "Rebus site is available"
      else:
        print "Rebus site is NOT available"

  except getopt.error, e:
    msg = e.__str__()
    usage()
    print "ERROR: Invalid command line argument: %s" % msg
    return 1
  except RebusException,e:
    print e
    return 1
  except Exception,e:
    traceback.print_exc()
    return 1
  return 0

######################################
#### MAIN ############################
######################################
if __name__ == "__main__":
  sys.exit(main(sys.argv))

