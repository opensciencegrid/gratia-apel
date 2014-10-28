#!/usr/bin/python

import sys, time, string
import libxml2
import urllib2
import exceptions

  
#------------------------------
class InactiveResources:
  """
     Author: John Weigand (7/2/09)
     Description: 
       This script was written for use in the Gratia-APEL interface (LCG.py).
       The LCG.py script was modified to look for individual days that has had
       no data reported and send an email for investigation.  
     
       If a site is active and not disabled and has planned maintenance, 
       this raises false "red" flags.  These conditions are handled by
       the Downtimes.py class.  However, if a site is inactive or disabled, 
       the downtime data is not available.

       This class will query MyOSG for all inactive resources
       allowing for a boolean resource_is_inactive method to indicate this
       condition.  It retrieves all sites in this state.

       Several problems with this method of doing this that we have to live
       with at this time:
       1. if the url changes, one is never notified.
       2. if the xml changes, one is never notified.
       3. if MyOSG is down (planned/unplanned), even if notified, can't do
          anything about it.
       
  """
  #-------------------------------------- 
  def __init__(self):
    """
    MyOSG url for retrieving all inactive Resources using this criteria:
      Information to display: Resource Group Summary
      For Resource: Show services
      Resource Groups to display: All resource groups
      For Resource Group: Grid Type - OSG
      For Resource: Provides the following services - Grid Services / CE
    """
    self.location = "http://myosg.grid.iu.edu/rgsummary/xml?datasource=summary&summary_attrs_showservice=on&gip_status_attrs_showtestresults=on&downtime_attrs_showpast=&account_type=cumulative_hours&ce_account_type=gip_vo&se_account_type=vo_transfer_volume&bdiitree_type=total_jobs&bdii_object=service&bdii_server=is-osg&start_type=7daysago&start_date=09%2F30%2F2011&end_type=now&end_date=09%2F30%2F2011&all_resources=on&gridtype=on&gridtype_1=on&service=on&service_1=on&active=on&active_value=0&disable_value=0"
    #---
    # old one used until 8/3/11 self.location = "http://myosg.grid.iu.edu/rgsummary/xml?datasource=summary&summary_attrs_showwlcg=on&summary_attrs_showservice=on&summary_attrs_showfqdn=on&gip_status_attrs_showtestresults=on&downtime_attrs_showpast=90&account_type=cumulative_hours&ce_account_type=gip_vo&se_account_type=vo_transfer_volume&start_type=7daysago&start_date=03%2F20%2F2009&end_type=now&end_date=03%2F27%2F2009&all_resources=on&rg_42=on&gridtype=on&gridtype_1=on&service_1=on&service_5=on&service_2=on&service_3=on&service_central_value=0&service_hidden_value=0&active=on&active_value=0&disable_value=1"
  
    self.doc = None
      
  #-------------------------------------- 
  def resource_is_inactive(self,resource):
    """ For a resource/date(YYYY-MM-DD)/service determine if this is a
        planned shutdown.
        Returns:  Boolean
    """
    self.__RetrieveXML__()
    inactive = False
    try:
      filter = "/ResourceSummary/ResourceGroup/Resources/Resource[Name='%s']" % resource
      resourceList =  self.doc.xpathEval(filter)
      if len(resourceList) <> 0:
        inactive = True
    except:
      raise Exception("Cannot find " + filter + " in the xml retrieved")
    return inactive

  #---------------------------------
  def __RetrieveXML__(self):
    """ Retrieve the xml document from MyOSG, only one time, for all
        downtimes (past and future).
    """
    try:
      if self.doc == None:
        html     = urllib2.urlopen(self.location).read()
        self.doc = libxml2.parseDoc(html)
        filter = "/ResourceSummary/ResourceGroup" 
        resourceList =  self.doc.xpathEval(filter)#
        if len(resourceList) == 0:
          raise Exception
    except:
      raise Exception("Unable to retrieve or parse xml from MyOSG")

#-- end of InactiveResources ---------------------------
#### MAIN ############################
if __name__ == "__main__":
  print "#---- test -------------------------------------------------------"
  resources = [ "ASGC_OSG", "HEPGRID_UERJ", "NOT_INACTIVE_RESOURCE" ]
  inactives = InactiveResources()
  try:
    for resource in resources:
      inactive = inactives.resource_is_inactive(resource)
      print "... %s is inactive: %s" % (resource,inactive)
  except Exception,e:
    print "ERROR:",sys.exc_info()[1]
    sys.exit(1)
  print "#----- end of test ---------------------------------------------"
   
  sys.exit(0)

