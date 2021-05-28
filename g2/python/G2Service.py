from flask import Flask
from flask import request
from flask import jsonify
import signal
import json
import sys

#--g2 generic config class
from G2Mappings import g2Mapper

from G2Exception import G2ModuleException
from G2Module import G2Module

#load and start json converter
g2GenLib = g2Mapper('g2WebService', './G2Generic.map')
if not g2GenLib.success:
    sys.stderr.write("Cannot load G2 JSON conversion module\n")
    sys.exit()

#load and start g2 module
g2Module = G2Module('pyG2Module', 'g2.ini')
g2Module.init()

#install signal handler for graceful shutdown
def shutdown_handler(signal_num, frame):
    print("shutting down...")
    g2Module.destroy()
    signal.signal(signal.SIGINT, old_sigint_handler)
    sys.exit(0)
old_sigint_handler = signal.signal(signal.SIGINT, shutdown_handler)


app = Flask(__name__)

@app.route('/entity/search', methods=['GET', 'POST'])
def entitySearch():
    responseMsg = 'Something went wrong!'
    if request.method == 'POST':
        jsonMsg = request.get_json()        

        #--defaults for searching
        if 'SOURCE_CODE' not in jsonMsg:
            jsonMsg['SOURCE_CODE'] = 'SEARCH'
        if 'ROUTE_CODE' not in jsonMsg:
            jsonMsg['ROUTE_CODE'] = 'SEARCH_ALL'
        if 'ENTITY_TYPE' not in jsonMsg:
            jsonMsg['ENTITY_TYPE'] = 'GENERIC'
        #if 'ENTITY_TYPE' not in jsonMsg:
        #    jsonMsg['ENTITY_TYPE'] = 'GENERIC'



        #--convert to umf
        umf_dict = g2GenLib.createUmfFromJson(jsonMsg)
        umfErrors = umf_dict['ERROR'] 
        umf = umf_dict['UMF_STRING']
        if not umfErrors:

            #--resolve here
            #--g2module.post?
            
            #--we will need to parse umf_response back to json!
            #--does module return internal document or after the message builder?  
            responseMsg = umf

        else:
            responseMsg = '\n'.join(umfErrors)
 
        print(responseMsg)
        '''
        #-- first validate
        jsonError, jsonMessages, jsonMsg, jsonMappings = g2GenLib.validateJsonMessage()
        if not jsonError:
        else:
            responseMsg = 'Invalid Json Message!'
            for msg in jsonMessages:
                responseMsg + '\n' + msg
        '''

    return responseMsg



@app.route('/entity/load', methods=['POST'])
def entity_load_post():
    ''' syncronously processes a message with G2 and returns the response '''
    response_msg = {}
    json_msg = request.get_json()

    print(json_msg)

    #convert from json to umf/xml
    umf_dict = g2GenLib.createUmfFromJson(json_msg)
    umfErrors = umf_dict['ERROR'] 
    umf = umf_dict['UMF_STRING']

    print(umf)

    if not umf_errors:
        try:
            response = g2Module.processWithResponse(umf)
            response_msg['STATUS'] = 'SUCCESS'
            response_msg['RESPONSE_MESSAGE'] = response
        except G2ModuleException as ex:
            response_msg['STATUS'] = 'ERROR'
            response_msg['ERROR_DETAIL'] = str(ex)
    else:
        response_msg['STATUS'] = 'ERROR'
        response_msg['ERROR_DETAIL'] = '|'.join(umf_errors)

    '''
    #-- first validate
    jsonError, jsonMessages, jsonMsg, jsonMappings = g2GenLib.validateJsonMessage()
    if not jsonError:
    else:
        responseMsg = 'Invalid Json Message!'
        for msg in jsonMessages:
            responseMsg + '\n' + msg
    '''

    return jsonify(response_msg)
