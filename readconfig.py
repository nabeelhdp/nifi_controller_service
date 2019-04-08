#!/usr/bin/python

import ConfigParser
from ConfigParser import SafeConfigParser

def get_config_params(config_file):
  try:
    with open(config_file) as f:
      try:
        parser = SafeConfigParser()
        parser.readfp(f)
      except ConfigParser.Error, err:
        print 'Could not parse: %s ', err
        return False
  except IOError as e:
    print "Unable to access %s. Error %s \nExiting" % (config_file, e)
    sys.exit(1)

  # Prepare dictionary object with config variables populated
  config_dict = {}
  config_dict["host"] = parser.get('nifi_config', 'host')
  config_dict["port"] = parser.get('nifi_config', 'port')
  config_dict["nifi_url"] = parser.get('nifi_config', 'nifi_url')
  config_dict["nifi_user"] = parser.get('nifi_config', 'nifi_user')
  config_dict["nifi_pass"] = parser.get('nifi_config', 'nifi_pass')
 
  return config_dict
