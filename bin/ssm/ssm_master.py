#!/usr/bin/env python
"""
Master script for running the SSM.  Given the path to a configuration file
as an argument, it will construct an SSM and run it according to the 
configuration.

An incorrect configuration will lead to the SSM quitting immediately.
"""

import os
import sys
import time
import logging
import ConfigParser
from daemon import DaemonContext

import ssm
from message_db import MessageDB
from encrypt_utils import EncryptException

log = None

def get_basic_config(config):
    """
    Read general SSM configuration info from a ConfigParser object,
    and return Config object to pass to the SSM.
    """
    
    c = ssm.Config()
    # Start the logging
    c.logconf = config.get('logging', 'log-conf-file')
    c.logconf = os.path.normpath(os.path.expandvars(c.logconf))
    if not os.access(c.logconf, os.R_OK):
        raise Exception("Can't find or open logging config file " + c.logconf)
    logging.config.fileConfig(c.logconf)
    global log
    log = logging.getLogger(ssm.SSM_LOGGER_ID)  
    
    try:
        c.bdii = config.get('broker', 'bdii-url')
        c.broker_network = config.get('broker', 'broker-network')
    except (ConfigParser.NoSectionError, ConfigParser.NoOptionError):
        c.bdii = None
        c.broker_network = None
        c.host = config.get('broker','host')
        c.port = config.getint('broker','port')
        
    # whether to use an SSL connection
    c.use_ssl = not (config.get('broker', 'use-ssl').lower() == 'false')
    
    c.use_pwd = not (config.get('broker', 'use-pwd').lower() == 'false')
    if c.use_pwd:
        c.username = config.get('broker', 'username')
        c.password = config.get('broker', 'password')
    
    
    # As well as fetching filepaths, expand any environment variables.
    c.capath = config.get('certificates','cadir')
    c.capath = os.path.normpath(os.path.expandvars(c.capath))
    c.certificate = config.get('certificates','certificate')
    c.certificate = os.path.normpath(os.path.expandvars(c.certificate))
    c.key = config.get('certificates','key')
    c.key = os.path.normpath(os.path.expandvars(c.key))
        
    c.pidfile = config.get('pidfile','pidfile')
    c.pidfile = os.path.normpath(os.path.expandvars(c.pidfile))
    # Check CRLs unless specifically told not to.
    c.check_crls = not (config.get('certificates', 'check-crls').lower() == 'false')

    
    # messagedb configuration 
    c.messages = config.get('messagedb','path')
    c.messages = os.path.normpath(os.path.expandvars(c.messages))
    # Only turn on test mode if specified.
    c.test = (config.get('messagedb', 'test').lower() == 'true')
    
    return c

    
def get_consumer_config(config):
    """
    Read SSM consumer configuration info from a ConfigParser object,
    and return ConsumerConfig object to pass to the SSM.
    """
    c = ssm.ConsumerConfig()
        
    c.listen_to = config.get('consumer','topic')
    c.valid_dn = config.get('consumer','valid-dns')
    c.valid_dn = os.path.normpath(os.path.expandvars(c.valid_dn))
    c.read_valid_dn_interval = config.getint('consumer', 'read-valid-dns')
        
    if not os.path.isfile(c.valid_dn):
        log.warn("Valid DN file doesn't exist: not starting the consumer")
        raise ssm.SsmException(c.valid_dn + ' not a file.')
    
    return c
        
        
def get_producer_config(config):
    """
    Read SSM producer configuration info from a ConfigParser object,
    and return ProducerConfig object to pass to the SSM.
    """    
    c = ssm.ProducerConfig()
    
    try:
        c.msg_check_time = config.getint('producer', 'msg-check-time')
    except ConfigParser.NoOptionError:
        pass
        
    c.consumerDN = config.get('producer','consumer-dn')
    c.send_to = config.get('producer','topic')

    # perform variable expansions
    ack = config.get('producer', 'ack')
    ack = ack.replace('$host', os.uname()[1])
    ack = ack.replace('$pid', str(os.getpid()))

    c.ack_queue = ack
    
    return c


def run_once(ssm_inst):
    """
    Given an SSM object, try to send all messages in its outgoing
    directory, then exit.
    """
    log.info("Running the SSM once only.")
    try:
        try:
            ssm_inst.startup()
            log.info("The SSM started successfully.")
        except Exception, err:
            print "Failed to connect: " + str(err)
            raise
        while ssm_inst.process_outgoing():
            pass
        
        # A final ping message will contain a reack for the last 
        # message received
        ssm_inst._send_ping()
        
        log.info("All outgoing messages have been processed.")
        ssm_inst.shutdown()
        
    except Exception, e:
        error_msg = "Error processing outgoing messages: " + str(e)
        print error_msg
        log.warn(error_msg)
        print "The SSM will exit"
        log.warn("The SSM will exit")
        ssm_inst.shutdown()
     
        
def run_as_daemon(ssm_inst):
    """
    Given an SSM object, start it as a daemon process.
    """
    log.info("The SSM will run as a daemon.")
    # We need to preserve the file descriptor for any log files.
    log_files = [x.stream for x in log.handlers]
    dc = DaemonContext(files_preserve=log_files)
    
    try:
        # Note: because we need to be compatible with python 2.4, we can't use
        # with dc:
        # here - we need to call the open() and close() methods 
        # manually.
        dc.open()
        try:
            ssm_inst.startup()
        except Exception, err:
            print err
            print type(err)
            print dir(err)
            log.info("SSM failed to start: " + str(err))
            raise
        
        # Only an exception will break this loop.
        # A SystemExit exception will be raised if the process is killed.
        while True:
            
            if ssm_inst.is_dead():
                raise ssm_inst.get_death_exception()
                
            # Process all the messages one at a time before continuing
            try:
                while ssm_inst.process_outgoing():
                    pass
            except ssm.SsmException, err:
                # SsmException if the message is rejected by the consumer.
                # We can wait and try again.
                log.error('Error in message processing: '+str(err))
            except EncryptException, err:
                # EncryptException if something went wrong trying to encrypt
                # or sign. Give up.
                log.error("Failed to encrypt or sign:" + str(err))
                raise
                
            time.sleep(ssm_inst.msg_check_time)
                
    except SystemExit, e:
        log.info("Received the shutdown signal: " + str(e))
        ssm_inst.shutdown()
        dc.close()
    except (ssm.SsmException, EncryptException), e:
        log.error("An unrecoverable exception was thrown:")
        log.error(str(e))
        log.error("The SSM will exit.")  
        ssm_inst.shutdown()
        dc.close()
    except Exception, e:
        log.error(type(e))
        log.error(str(e))
        log.error("Unexpected exception: " + str(e))
        log.error("The SSM will exit.")  
        ssm_inst.shutdown()
        dc.close()
    
        
def run_process(config_file, asdaemon):
    """
    Retrieve the configuration from the file and start the SSM.
    """
    # Retrieve configuration from file
    config_parser = ConfigParser.ConfigParser()
    config_parser.read(config_file)
    
    # Required basic SSM configuration, including logging
    try:
        config = get_basic_config(config_parser)
        config.daemon = asdaemon
    except Exception, err:
        print "Error in configuration file: " + str(err)
        print "System will exit."
        sys.exit(1)
        
    log.info("=======================================================")
    log.info("Starting the SSM...")
        
    # Optional consumer configuration
    try:
        consumer_config = get_consumer_config(config_parser)
    except:
        consumer_config = None
        
    # Optional producer configuration
    try:
        producer_config = get_producer_config(config_parser)
    except:
        producer_config = None
       
    try:
        messagedb = MessageDB(config.messages, config.test)
        ssm_inst = ssm.SecureStompMessenger(messagedb, config, producer_config, consumer_config)
    except Exception, err:
        print err
        print type(err)
        print "FATAL STARTUP ERROR: " + str(err)
        log.error("SSM failed to start: " + str(err))
        sys.exit(1)
        
    # Finally, as long as initial checks pass, set it going.
    if asdaemon:
        run_as_daemon(ssm_inst)
    else:
        run_once(ssm_inst)
        
    log.info("The SSM has shut down.")
    log.info("=======================================================")
        
        
def usage():
    print "Usage: python %s <path-to-config-file> [-d]" % sys.argv[0]
    print "       -d:  run as a daemon"
    sys.exit(1)

if __name__ == "__main__":
    
    daemon = sys.argv[1] == "-d"
    
    if daemon:
        if len(sys.argv) != 3:
            usage()
        else:
            run_process(sys.argv[2], True)
    else:
        if len(sys.argv) != 2:
            usage()
        else:
            run_process(sys.argv[1], False)
