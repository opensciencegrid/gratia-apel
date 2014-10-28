#!/usr/bin/python

import getopt
import commands, os, sys, time, string
import datetime
import traceback
import urllib2
import libxml2
import exceptions

##################################################
class InteropAccounting:
  """
  Author: John Weigand (10/26/10)
  Description: 
    This class accesses MyOsg for the purpose of identifying
    Resource Groups and Resources that are to be interfaced to
    APEL/WLCG accouting.

    Its original intent is for use by the LCG.py process.  There is only
    one method currently used by LCG.py (interfacedResources) which
    provides all the interfaced resources for a specified resource group.

    Since some access was required to query the MyOsg data, it seemed 
    it would be nice to have an easy to use command line view of the
    for trouble shooting purpose.  So this process can be executed
    from the command line with the following usage.

    Actions:
    --show
        Displays MyOsg resource WLCG InteropAccounting and AccountingName data
        for all resouce groups with at least 1 InteropAccounting set to True.
    --is-interfaced=resource_group
        Displays the WLCG InteropAccounting option and AccountingName for the
        resource group specified.
    --interfaced-resource-groups
        Using the interfacedResourceGroups() API, returns a sorted list of the
        resource groups with the InteropAccounting set to True
    --resources=resource_group
        Using the interfacedResources(resource_group) API, returns a sorted list
        of the resources for a specified resource group with the
        Interopaccounting set to True.
    --is-registered=resource_group
        Using the isRegistered(resource_group) API, returns True if
        the resource group specified is defined in MyOsg
  """
  #-------------------------------------- 
  def __init__(self):
    """ url for retrieving all resources from MyOsg with the following criteria:
       Information to display: Resource Group Summary
       For Resource: Show WLCG Informatinon
                     Show services
                     Show FQDN / Aliases
       For Resource Group: Grid Type - OSG
    """
    self.location = "http://myosg.grid.iu.edu/rgsummary/xml?datasource=summary&summary_attrs_showwlcg=on&summary_attrs_showservice=on&summary_attrs_showfqdn=on&gip_status_attrs_showtestresults=on&downtime_attrs_showpast=&account_type=cumulative_hours&ce_account_type=gip_vo&se_account_type=vo_transfer_volume&start_type=7daysago&start_date=03%2F20%2F2009&end_type=now&end_date=03%2F27%2F2009&all_resources=on&facility_10009=on&site_10026=on&gridtype=on&gridtype_1=on&service_1=on&service_5=on&service_2=on&service_3=on&service_central_value=0&service_hidden_value=0&active_value=1&disable_value=1"

    self.accountingDict = {}
    self.myosgResourceGroups = []    # All MyOsg Resource Groups
    self.resourceGroups      = None  # InteropAccounting Resource Groups
    self.doc                 = None  # MyOsg xml document


  ################################# 
  def get_resourceGroups(self):
    """ Retrieve MyOsg data identifying the resource groups with 
        resource having CE services defined and indicating the.
        WLCGInformation InteropAccounting option for that resource
        group. If any resource have the InteropAccounting option set to
        True, then is will apply to the resource group. 
    """
    if self.resourceGroups != None:  # only retrieve ones
      return
    self.resourceGroups = {}
    self.__getMyOsgData__()
    for rg in self.doc.xpathEval("/ResourceSummary/ResourceGroup"):
      resource_grp = self.__get_value__(rg,"GroupName") 
      if self.resourceGroupDisabled(rg):
        continue
      self.myosgResourceGroups.append(resource_grp)
      interfaced  = False
      ceResources = {}
      #--- Resources ----
      for resource in rg.xpathEval("Resources/Resource"):
        if self.resourceDisabled(resource):
          continue
        resource_name     = self.__get_value__(resource,"Name") 
        #--- WLCGInfo ----
        wlcg = resource.xpathEval("WLCGInformation")
        acctName = self.__get_value__(wlcg[0],"AccountingName") 
        #--- Services ----
        for service in resource.xpathEval("Services/Service"):
          if not self.CEService(service):
            continue
          ceResources[resource_name] = [self.interfacedToApel(wlcg[0]),acctName]
          if self.interfacedToApel(wlcg[0]):
            interfaced = True 
            break
        #-- end of Services --
      #-- end of Resources --
      if interfaced:
        self.resourceGroups[resource_grp] = ceResources
    #-- end of resource groups --

  #############################################################
  def isRegistered(self,resource_grp):
    """ Returns True if the resource group is defined in MyOsg. """
    self.get_resourceGroups()
    if resource_grp in self.myosgResourceGroups:
      return True
    return False

  #############################################################
  def isInterfaced(self,resource_grp):
    """ Returns True if the resource group specified interfaces to APEL/WLCG."""
    self.get_resourceGroups()
    if resource_grp in self.resourceGroups.keys():
      return True
    return False

  #############################################################
  def interfacedResourceGroups(self):
    """ 
       Returns a python list of MyOsg resource groups with the InteropAccounting
       flag set to True.
    """
    self.get_resourceGroups()
    return sorted(self.resourceGroups.keys())
    
  #############################################################
  def interfacedResources(self,resource_group):
    """ 
       Returns a python list of MyOsg resources for the resource group specified
        with the InteropAccounting flag set to True.
    """
    self.get_resourceGroups()
    list = []
    if self.isInterfaced(resource_group):
      resources = self.resourceGroups[resource_group]
      for resource in sorted(resources.keys()):
        if resources[resource][0] == True:
          list.append(resource)
    return list
    
  #############################################################
  def WLCGAcountingName(self,resource_grp):
    """ Returns the WLCGInformation Accounting Name for a resource group.
        If not interfaced to WLCG, then returns the None value.
        Since the WLCGInformation is at the resource level and there may be
        multiple resources for a resource group, the 1st resource that is 
        interfaced will be used and hopefully it is correct.
    """
    self.get_resourceGroups()
    accounting_name = None
    if resource_grp in self.resourceGroups:
      resourceDict = self.resourceGroups[resource_grp]
      for resource in resourceDict.keys():
        interopAcct = resourceDict[resource][0]
        if interopAcct == True:
          accounting_name = resourceDict[resource][1]
    return accounting_name

  #############################################################
  def show_data(self):
    self.get_resourceGroups()
    format = "%-18s %-18s %-18s %s"
    print format % ( "Resource Group","Resource","InteropAccounting","AccountingName")
    for rg in sorted(self.resourceGroups.keys()):
      resourceDict = self.resourceGroups[rg]
      displayAcctName = "" 
      displayRG = rg
      for resource in sorted(resourceDict.keys()):
        interopAcct     = resourceDict[resource][0]
        acctName = resourceDict[resource][1]
        print format % (displayRG,resource,interopAcct,acctName)
        displayRG = ""
        
  #############################################################
  def resourceGroupDisabled(self,rg):
    if self.__get_value__(rg,"Disable") == "True":
      return True
    return False 
  #############################################################
  def resourceActive(self,resource):
    if self.__get_value__(resource,"Active") == "True":
      return True
    return False 
  #############################################################
  def resourceDisabled(self,resource):
    if self.__get_value__(resource,"Disable") == "True":
      return True
    return False 
  #############################################################
  def interfacedToApel(self,wlcg):
    if self.__get_value__(wlcg,"InteropAccounting") == "True":
      return True
    return False
  #############################################################
  def CEService(self,service):
    if self.__get_value__(service,"Name") == "CE":
      return True 
    return False
  #############################
  def __getMyOsgData__(self):
    try:
      if self.doc == None:
        html     = urllib2.urlopen(self.location).read()
        self.doc = libxml2.parseDoc(html)
    except:
      raise Exception("Unable to retrieve or parse xml from MyOSG")

    
  #################################################
  def __get_value__(self,element,name):
    """ Returns the value of the xml element."""
    el = element.xpathEval(name)
    if len(el) == 0:
      return ""
    else:
      return  el[0].content 

#### end class #######################
#----------------
def usage():
  global gProgramName
  print """
Usage: %(program)s action [-help] 

  Actions:
    --show
        Displays MyOsg resource WLCG InteropAccounting and AccountingName data
        for all resouce groups with at least 1 InteropAccounting set to True.
    --is-interfaced=resource_group
        Displays the WLCG InteropAccounting option and AccountingName for the
        resource group specified.
    --interfaced-resource-groups
        Using the interfacedResourceGroups() API, returns a sorted list of the
        resource groups with the InteropAccounting set to True
    --resources=resource_group
        Using the interfacedResources(resource_group) API, returns a sorted list
        of the resources for a specified resource group with the 
        Interopaccounting set to True.
    --is-registered=resource_group
        Using the isRegistered(resource_group) API, returns True if
        the resource group specified is registered in MyOsg
""" % {"program":gProgramName}

#----------------
def main(argv):
  global gProgramName
  gProgramName = argv[0]
  gAction = None
  gRG     = None
  gDEBUG = False
  arglist = [ "help", "show", "is-interfaced=", "interfaced-resource-groups", "resources=", "is-registered="]
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
      if o in ("--debug"):
        gDEBUG = True
        continue
      if o in ("--is-interfaced"):
        gAction = o
        gRG = a
        continue
      if o in ("--resources"):
        gAction = o
        gRG = a
        continue
      if o in ("--is-registered"):
        gAction = o
        gRG = a
        continue
      if o[2:] in arglist:
        gAction = o
        continue 
    myosg = InteropAccounting()
    print
    if gAction == "--show":
      myosg.show_data()

    elif gAction == "--is-interfaced":
        format = "%-25s %-13s %s"
        print format % ("Resource Group","Interfaced?","Accounting Name")
        print format % (gRG,myosg.isInterfaced(gRG),myosg.WLCGAcountingName(gRG))
    elif gAction == "--interfaced-resource-groups":
      print myosg.interfacedResourceGroups()

    elif gAction == "--resources":
      if myosg.isRegistered(gRG):
        print "Resources for Resource Group (%s) with InteropAccouting set to True" % gRG
        print myosg.interfacedResources(gRG)
      else:
        print "Resource Group (%s) is not in MyOSG" % gRG

    elif gAction == "--is-registered":
      print "Resource Group (%s) is registered in MyOsg: %s" % (gRG, myosg.isRegistered(gRG))
    else:
      usage()
      print "ERROR: Invalid command line argument: %s" % gAction
      return 1
  except getopt.error, e:
    msg = e.__str__()
    usage()
    print "ERROR: Invalid command line argument: %s" % msg
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


