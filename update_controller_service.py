
#!/usr/local/bin/python3
import requests
import json
import time
import urllib2
import base64
import csv

def gen_http_request( url, configs, data=""):

    req = ""
    auth_string = "%s:%s" % (configs['atlas_user'], configs['atlas_pass'])
    auth_encoded = 'Basic %s' % base64.b64encode(auth_string).strip()
    if data == "":
        req = urllib2.Request(url)
    else:
        req = urllib2.Request(url,data=json.dumps(data))
        #print(json.dumps(data))
    req.add_header('Authorization', auth_encoded)
    req.add_header('Content-Type', 'application/json')
    req.add_header('charset', 'UTF-8')
    req.add_header('Accept', 'application/json')

    return req

def send_http_request( req, timeout):

    httpHandler = urllib2.HTTPHandler()
    opener = urllib2.build_opener(httpHandler)
    end = 0
    start = 0
    try:
       start = time.time()
       response = opener.open(req)
       end = time.time()
       print "Request to {} took {} seconds".format( req.get_full_url(), end - start)
       return json.load(response)
    except (urllib2.URLError, urllib2.HTTPError) as e:
       print 'Error', e
    except ValueError as e:
       print("Empty response likely {}".format(response))
       return response

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
    resp = requests.put(
        conf['base_url'],
        data = data,
        headers = headers
        )
    print(resp2.content)

def set_ref_component_runstate( csinfo, conf, headers, state):

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
        resp = requests.put(
            conf['ref_url'],
            data = data,
            headers = headers
            )
        print(resp.content)
        time.sleep(10)

def set_ref_component_state( conf, headers, state):

    data_ = {
        "id"  : conf['cs_id'],
        "state"  : state,
        "referencingComponentRevisions" : {}
        }
    data = json.dumps(data_)
    resp = requests.put(
        conf['ref_url'],
        data = data,
        headers = headers
        )
    print(resp.content)

def set_controller_service_properties( conf, headers, change):

    ## Get json object by invoking REST URL.
    csinfo_ = requests.get(conf['base_url'])
    csinfo = csinfo_.json()

    ## Update json object
    csinfo['component']['properties']['hive-db-connect-url'] = conf['hive-db-connect-url']
    csinfo['component']['properties']['Kerberos Principal'] =  conf['Kerberos Principal']
    csinfo['component']['properties']['Kerberos Keytab'] =  conf['Kerberos Keytab']

    ## Send POST request
    req = gen_http_request( conf['base_url'])
        resp = send_http_request( req, 20)
    return True

def disable_all_dependencies(csinfo, conf, headers):

    # Stop referencing component
    set_ref_component_runstate(
        csinfo = csinfo,
        conf = conf,
        headers = headers,
        state = "STOPPED")

    # Disable referencing component
    set_ref_component_state(
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
        conf = conf,
        headers = headers,
        state = "ENABLED")

    # Start referencing component
    set_ref_component_runstate(
        csinfo = csinfo,
        conf = conf,
        headers = headers,
        state = "RUNNING")

    return True


def main() :

    conf = {}

    csv_file = 'kerberizeNiFi.csv'
    # Enter the Nifi URL
    conf['nifi_url'] = 'http://10.10.10.10:9090'

    headers = {
            'Content-Type' : 'application/json',
            'Accept' : '*/*'
            }

    fieldnames = ['cs_id', 'hive-db-connect-url', 'Kerberos Principal', 'Kerberos Keytab']

    # Enter UID for Controller Service
    #conf['cs_id'] = 'e1495a25-0168-1000-0000-00000851f482'

    with open(csv_file, newline='') as f:
        reader = csv.DictReader(f, fieldnames=fieldnames)
        for row in reader:
            conf['cs_id'] = row['cs_id']
            conf['hive-db-connect-url'] = row['hive-db-connect-url']
            conf['Kerberos Principal'] = row['Kerberos Principal']
            conf['Kerberos Keytab'] = row['Kerberos Keytab']
            conf['base_url'] = '{}/nifi-api/controller-services/{}'.format(
                  conf['nifi_url'],
                  conf['cs_id']
                  )

            ref_url = '{}/references'.format( conf['base_url'] )


            # Obtain controller service related details and convert into dictionary
            csinfo_ = requests.get(conf['base_url'])
            csinfo = csinfo_.json()

            # Stop all services
            disable_status = disable_all_dependencies( csinfo, conf, headers )

            # Update controller service properties
            set_controller_service_properties()

            # Start all services again
            start_status = start_all_dependencies( csinfo, conf, headers )

