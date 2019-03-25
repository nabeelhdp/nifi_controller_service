import time
import requests
import json
import urllib
import base64
import csv

def send_http_request( url,headers,data="",conf=""):

    #auth_string = "%s:%s" % (conf['nifi_user'], conf['nifi_pass'])
    #auth_encoded = 'Basic %s' % base64.b64encode(auth_string).strip()
    if data != "":
        jsondata = json.dumps(data).encode('utf-8')   # needs to be bytes
    #    req.add_header('Content-Length', len(jsondata))
    #req.add_header('Authorization', auth_encoded)

    response = requests.put(
           url,
           data = jsondata,
           headers = headers
           )
    print(response)

def set_controller_service_state( version, conf, headers, state):

    print("Setting controller service id {} to state {}".format(conf['cs_id'],state))
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
    # _ = input("Confirm state {} of controller_service id {}\n".format(state,conf['cs_id']))
    # print(resp.content)

def set_ref_component_state( csinfo, conf, headers, state):

    for component in csinfo['component']['referencingComponents']:
        ver = component['revision']['version']
        ref_comp_id = component['id']
        print("Setting component id {} to run state {}".format(ref_comp_id,state))
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
        # print(resp.content)
        #_ = input("Confirm run state {} for component id {}\n".format(state,ref_comp_id))
        time.sleep(10)

def set_controller_service_properties( conf, headers ):

    ## Get json object by invoking REST URL.
    csinfo_ = requests.get(conf['base_url'])
    csinfo = csinfo_.json()

    ## Update json object as needed
    csinfo['component']['properties']['hive-db-connect-url'] = conf['hive-db-connect-url']
    csinfo['component']['properties']['Kerberos Principal'] =  conf['Kerberos Principal']
    csinfo['component']['properties']['Kerberos Keytab'] =  conf['Kerberos Keytab']

    print("Setting controller service properties to {} ".format(csinfo['component']['properties']))
    ## Send POST request
    send_http_request( conf['base_url'], data=csinfo, headers=headers)
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


def main() :

    conf = {}

    csv_file = 'kerberizeNiFi.csv'
    # Enter the Nifi URL
    conf['nifi_url'] = 'http://1.1.1.1:9090'
    #conf['nifi_user'] = 'username'
    #conf['nifi_pass'] = 'password'

    headers = {}
    headers['Content-Type'] = 'application/json'
    headers['charset'] = 'UTF-8'
    headers['Accept'] = '*/*'


    fieldnames = ['cs_id', 'hive-db-connect-url', 'Kerberos Principal', 'Kerberos Keytab']

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

            conf['ref_url'] = '{}/references'.format( conf['base_url'] )

            print(conf)

            # Obtain controller service related details and convert into dictionary
            csinfo_ = requests.get(conf['base_url'])
            csinfo = csinfo_.json()

            # Stop all services
            disable_status = disable_all_dependencies( csinfo, conf, headers )

            # Update controller service properties
            set_controller_service_properties( conf, headers=headers )

            # Start all services again
            start_status = start_all_dependencies( csinfo, conf, headers )


if __name__ == "__main__":
    main()
