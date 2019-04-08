

import time
from getpass import getpass
import requests
import json
import urllib
import urllib2
import base64
import csv
import ssl

def send_http_request( url,headers,data="",conf=""):

    #auth_string = "%s:%s" (conf['nifi_user'], conf['nifi_pass'])
    #headers['Authorization'] = 'Basic %s' % base64.b64encode(auth_string).strip()

    if data != "":
        jsondata = json.dumps(data).encode('utf-8')
        headers['Content-Length'] = len(jsondata)

    response = requests.put(
           url,
           data = jsondata,
           headers = headers,
           verify = False
           )
    print(response)


def set_ssl():

    # BAD CODE : Ignores SSL - equivalent to cancelling cert prompt.
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    return context


def set_controller_service_state( version, conf, headers, state):

    data_ = {
        "revision" : {
            "version" : version
            },
        "component" : {
            "id" : conf['cs_id'],
            "state" : state
            }
        }
    data = json.dumps(data_)

    end = 0
    start = 0
    start = time.time()
    resp = requests.put(
        conf['base_url'],
        data = data,
        headers = headers,
        verify = False
        )
    end = time.time()
    print("Setting controller service id {} to state {} took {} seconds".format(conf['cs_id'],state, end - start))

def set_ref_component_state( csinfo, conf, headers, state):

    for component in csinfo['component']['referencingComponents']:
        ver = component['revision']['version']
        ref_comp_id = component['id']
        data_ = {
            "id" : conf['cs_id'],
            "state" : state,
            "referencingComponentRevisions" : {
                ref_comp_id : {
                    "version" : ver
                    }
                }
            }
        data = json.dumps(data_)
        end = 0
        start = 0
        start = time.time()

        resp = requests.put(
            conf['ref_url'],
            data = data,
            headers = headers,
            verify = False
            )

        end = time.time()
        print("Setting component id {} to run state {} took {} seconds".format(ref_comp_id,state, end - start))

def set_controller_service_properties( conf, headers ):

    ## Get json object by invoking REST URL.
    csinfo_ = requests.get(conf['base_url'],headers=headers)
    csinfo = csinfo_.json()

    ## Update json object as needed
    #csinfo['component']['properties']['hive-db-connect-url'] = conf['hive-db-connect-url']
    csinfo['component']['properties']['Kerberos Principal'] =  conf['Kerberos Principal']
    csinfo['component']['properties']['Kerberos Keytab'] =  conf['Kerberos Keytab']

    data = {
         "revision": {
           "version": csinfo['revision']['version']
           },
         "component": {
           "id":  csinfo['component']['id'],
           "properties": csinfo['component']['properties']
          }
         }

    ## Send POST request
    end = 0
    start = 0
    start = time.time()

    send_http_request( conf['base_url'], data=data, headers=headers)
    end = time.time()
    print("Setting controller service properties to {} took {} seconds".format(csinfo['component']['properties'],end - start))

    return True

def disable_all_dependencies(csinfo, conf, headers):

    # Stop referencing component
    set_ref_component_state(
        csinfo = csinfo,
        conf = conf,
        headers = headers,
        state = "STOPPED")

    # Disable referencing component
    set_ref_component_state(
        csinfo = csinfo,
        conf = conf,
        headers = headers,
        state = "DISABLED")


    # Disable controller service
    set_controller_service_state(
        version = csinfo['revision']['version'],
        conf = conf,
        headers = headers,
        state = "DISABLED")

    return True

def start_all_dependencies( csinfo, conf, headers):

    # Enable controller service
    set_controller_service_state(
        version = csinfo['revision']['version'],
        conf = conf,
        headers = headers,
        state = "ENABLED")

    # Enable referencing component
    set_ref_component_state(
        csinfo = csinfo,
        conf = conf,
        headers = headers,
        state = "ENABLED")

    # Start referencing component
    set_ref_component_state(
        csinfo = csinfo,
        conf = conf,
        headers = headers,
        state = "RUNNING")

    return True

def get_auth_token(conf):
   httpHandler = urllib2.HTTPSHandler(context=set_ssl())
   opener = urllib2.build_opener(httpHandler)
   req = get_auth_request(conf)
   try:
     resp = opener.open(req)
     tdata = resp.read()
     token = 'Bearer ' + tdata
     print (tdata)
     return token
   except (urllib2.URLError, urllib2.HTTPError) as err:
     return 'NiFi authentication failed with error: %s' % err



def get_auth_request(conf):
   ###print(conf)
   data = {}
   data['username' ] = 'admin'
   data['password'] = 'admin'

   url_values = urllib.urlencode(data)
   token_url = "https://%s:%d/nifi-api/access/token" % (conf['host'], conf['port'])
   headers = {'Content-Type':'application/x-www-form-urlencoded; charset=UTF-8'}
   req = urllib2.Request(token_url,url_values,headers=headers)

   return req


def update_cs_properties(conf, csinstance, headers):

    cs_orig_state = {}

    conf['cs_id'] = csinstance['cs_id']
    conf['hive-db-connect-url'] = csinstance['hive-db-connect-url']
    conf['Kerberos Principal'] = csinstance['Kerberos Principal']
    conf['Kerberos Keytab'] = csinstance['Kerberos Keytab']
    conf['base_url'] = '{}/nifi-api/controller-services/{}'.format(
          conf['nifi_url'],
          conf['cs_id']
          )

    conf['ref_url'] = '{}/references'.format( conf['base_url'] )

    # Obtain controller service related details and convert into dictionary
    csinfo_ = requests.get(conf['base_url'],headers=headers, verify =False)
    csinfo = csinfo_.json()
    cs_orig_state[conf['cs_id']] = {}

    if csinfo['component']['name'] == "HiveConnectionPool":

        # Save state of "DRHiveConnectionPool" controller service
        # Assumes no referencing component for the controller service is named "parent"
        cs_orig_state[conf['cs_id']]['parent'] = csinfo['component']['state']

        # Save state of each referencing component
        for items in csinfo['component']['referencingComponents']:
            cs_orig_state[conf['cs_id']][items['component']['id']] = items['component']['state']

        if csinfo['component']['state'] == "DISABLED":
            print("HiveConnectionPool in DISABLED state. Skipping")
        if csinfo['component']['state'] == "ENABLED":
            # Stop and disable all services
            disable_status = disable_all_dependencies( csinfo, conf, headers )

        # Update controller service properties
        set_controller_service_properties( conf, headers=headers )

        # Start all services again
        start_status = start_all_dependencies( csinfo, conf, headers )
    else :
        cs_orig_state[conf['cs_id']]['parent'] == "SKIPPED"
        print("Controller Service is not of type DRHiveConnectionPool. Skipping {}".format(conf['cs_id']))
    return True


def main() :

    conf = {}

    csv_file = 'input.csv'
    conf['nifi_url'] = 'https://NIFI.domain.com:9091'
    conf['host'] = 'NIFI.domain.com'
    conf['port'] = 9091
    conf['nifi_user'] = 'admin'
    conf['nifi_pass'] = getpass()

    token = get_auth_token(conf)

    headers = {}
    headers['Content-Type'] = 'application/json'
    headers['charset'] = 'UTF-8'
    headers['Accept'] = '*/*'
    headers['Authorization'] = token

    fieldnames = ['cs_id', 'hive-db-connect-url', 'Kerberos Principal', 'Kerberos Keytab']

    with open(csv_file) as f:
        reader = csv.DictReader(f, fieldnames=fieldnames)
        for row in reader:
            update_cs_properties(conf=conf,csinstance=row,headers=headers)


if __name__ == "__main__":
    main()
