#!/usr/bin/python

import os, sys, time, string
import subprocess
import getopt
import glob
import traceback
import exceptions
import ConfigParser

class SSMException(Exception):
  pass

class SSMInterface:
  """ Author: John Weigand (11/17/11)
      Description:
        This class retrieves reads the SSM configuration file
        and provides various methods for viewing/using the data.
        This is something that should be provided by APEL/SSM.
  """

  #############################
  def __init__(self,config_file,ssm_file):
    self.config = ConfigParser.ConfigParser()
    if not os.path.isfile(config_file):
      raise SSMException("""The SSM configuration file does not exist (%s).""" % config_file)
    self.configFile = config_file
    self.config.read(config_file)
    self.ssm_master = "/usr/share/gratia-apel/ssm/ssm_master.py"
    self.outgoing = "%s/outgoing" % self.config.get("messagedb","path")
    self.__validate__() 

  #-----------------------
  def __validate__(self):
    if not os.path.isdir(self.outgoing):
      raise SSMException("""The outgoing messages directory defined by the SSM configuration file:
  %(config)s 
is not a directory: %(outgoing)s
This is the "messagedb" section, "path" attribute""" % \
       { "config" : self.configFile, "outgoing" : self.outgoing })
    #-- verify ssm_master exists --
    if not os.path.isfile(self.ssm_master):
      raise SSMException("""The main interface file does not exist:
  %(ssm_master)s""" % { "ssm_master" : self.ssm_master }) 

  #-----------------------
  def show_outgoing(self):
    """Display files in the SSM outgoing messages directory."""
    if self.outgoing_sent():
      return """There are no outgoing messages in:
  %s""" %  self.outgoing  
    else: 
      return """Outgoing messages in %(dir)s
%(files)s""" %  { "dir" : self.outgoing, "files" : os.listdir(self.outgoing) }  

  #-----------------------
  def outgoing_sent(self):
    """ Checks to see if any files are in the SSM outgoing directory.
        Returns: True  if there are no files in the directory.
                 False if there are files in the directory.
    """
    msgs = glob.glob(self.outgoing + '/*')
    if len(msgs) == 0:
      return True
    return False

  #-----------------------
  def send_file(self,file):
    """Copies a file to the SSM outgoing directory and invokes the
       send_outgoing method.
    """
    if not os.path.isfile(file):
      raise SSMException("File to be sent does not exist: %s" % file)
    subprocess.call("cp %(file)s %(outgoing)s" % \
                 { "file" : file, "outgoing": self.outgoing} ,shell=True)
    if not os.path.isfile(file):
      raise SSMException("""Copy failed. File: %(file)s
To: %(dir)s""" % { "file" : file, "dir" : self.outgoing })
    self.send_outgoing()

  #-----------------------
  def send_outgoing(self):
    """Invokes the SSM client process which sends all files in the
       outgoing directory to APEL.  It then verifies that all files
       have been sent, i.e., there are no files left in the outgoing
       directory.  A 2 minute timeout is used in the event the 
       interface hangs.  Since the SSM client runs as a daemon,
       the client is killed on termination of this process.
    """
    cmd = "python %(ssm_master)s %(config)s" % \
       { "ssm_master" : self.ssm_master, "config" : self.configFile }
    try:
      p = subprocess.Popen(cmd, shell=True,
                           stdin=subprocess.PIPE, 
                           stdout=subprocess.PIPE, 
                           stderr=subprocess.PIPE,
                           close_fds=True, env=os.environ)
    except OSError, e:
      raise SSMException("""Interface FAILED.
Command: %(cmd)s
Exception OSError: %(oserror)s
Files in: %(dir)s
%(files)s
""" % { "cmd" : cmd, "oserror" : e, "dir" : self.outgoing,
        "files" : os.listdir(self.outgoing) } )

    maxtime   = 120   # max seconds to wait before giving up and terminating job
    totaltime = 0
    sleep     = 10    # sleep time between checks to see if file was sent
    rtn = p.poll()
    while self.outgoing_sent() == False:
      rtn = p.poll()
      if rtn is not None:
        break
      subprocess.call("sleep %s" % sleep,shell=True)
      if totaltime > maxtime:
        p.terminate()
        msg = """Interface FAILED.  
Command: %(cmd)s
Had to kill process after %(timeout)s seconds.
Files in: %(dir)s
%(files)s
""" % { "timeout" : maxtime, "dir"   : self.outgoing, 
        "cmd"     : cmd,     "files" : os.listdir(self.outgoing) } 
        raise SSMException(msg)
      totaltime = totaltime + sleep
    ##-- end of while --  
    stdoutdata, stderrdata = p.communicate()
    rtn = p.returncode
    if rtn > 0: 
      msg = """Interface FAILED. Messages have not been sent.
Command:  %(cmd)s
Return code: %(rtn)s
SSM stdout: %(stdout)s
SSM stderr: %(stderr)s""" %  \
         { "cmd" : cmd, "rtn" : rtn, "stdout" : stdoutdata, "stderr" : stderrdata}
      raise SSMException(msg)
    #-- double check the SSM program does not always give a non-zero return code
    if not self.outgoing_sent():
      msg = """Interface FAILED. All messages have NOT been sent.
Command:  %(cmd)s
SSM stdout: %(stdout)s
SSM stderr: %(stderr)s""" %  \
           { "cmd" : cmd, "stdout" : stdoutdata, "stderr" : stderrdata}
      raise SSMException(msg)
## end of class ###

#----------------
def usage():
  global gProgramName
  print """
Usage: %(program)s --config <SSM config file> Actions [-help]

  Provides visibility into SSM interface information.

  Note: You must have the SSM_HOME environmental variable set.

  --config <SSM config file>
    This is the SSM configuration file used by the interface.

  Actions:
    --show-outgoing
        Displays the outgoing SSM messages directory contents.
    --send-outgoing
        Sends any outgoing SSM messages directory contents.
    --send-file FILE
        Copies the specified FILE to the SSM outgoing directory and sends it.
"""

#----------------
def main(argv):
  global gProgramName
  gProgramName = argv[0]
  config = None

  action = ""
  type   = ""
  arglist = [ "help", "config=", "show-outgoing", "send-outgoing","send-file=" ]
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
      if o in ("--show-outgoing", ):
        action = o
        continue
      if o in ("--send-outgoing", ):
        action = o
        continue
      if o in ("--send-file"):
        action = o
        file   = a
        continue
      if o in ("--config"):
        action = o
        config = a
        continue

    if config == None:
      usage()
      print "ERROR: you need to specify the --config option"
      return 1

    ssm_home = "xxx"
    ssm = SSMInterface(config,ssm_home)
    if action == "--show-outgoing":
      print ssm.show_outgoing()
    elif action == "--send-outgoing":
      if ssm.outgoing_sent():
        print "There are no messages to send"
        return 1
      ssm.send_outgoing()
      if ssm.outgoing_sent():
        print "All messages have been sent"
        return 0
      else:
        print "ERROR: all messages have NOT been sent"
        print ssm.show_outgoing()
        return 1
    elif action == "--send-file":
      ssm.send_file(file)
      if ssm.outgoing_sent():
        print "File has been sent successfully"
        return 0
      else:
        print "ERROR: File has NOT been sent"
        print ssm.show_outgoing()
        return 1
    else:
      usage()
      print "ERROR: no action options specified"
      return 1

  except getopt.error, e:
    msg = e.__str__()
    usage()
    print "ERROR: Invalid command line argument: %s" % msg
    return 1
  except SSMException,e:
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

