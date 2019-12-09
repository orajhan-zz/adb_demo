import oci
import csv
import json
import requests
import os
import io
from base64 import b64encode, b64decode
import time
import sys
import ast
from fdk import response

def handler(ctx, data: io.BytesIO=None):
    signer = oci.auth.signers.get_resource_principals_signer()

    try:
        #body = json.loads(data.getvalue())
        bucket_name = os.environ.get("OCI_BUCKETNAME")
        resp = do(signer, bucket_name)

    except Exception as e:
        error = 'Unable to find out the object in Bucket!'
        raise Exception(error)

    return response.Response(
        ctx, response_data=json.dumps(resp),
        headers={"Content-Type": "application/json"}
    )

#Get Streaming OCID
def get_streaming_ocid(stream_admin_client, compartment_id):
    list_streams = stream_admin_client.list_streams(compartment_id, lifecycle_state=oci.streaming.models.StreamSummary.LIFECYCLE_STATE_ACTIVE)
    #print(list_streams.data)

    if list_streams.data:
        # If we find an active stream with the correct name, we'll use it.
        print("Streaming Name {} :".format(list_streams.data[0].name))
        sid = list_streams.data[0].id
        messages_endpoint = list_streams.data[0].messages_endpoint
        return sid, messages_endpoint

#Start Streaming
def put_messages_streaming(stream_client, streaming_ocid, data):

    print("Streaming Starts at {}".format(time.strftime("%Y-%m-%d %H:%M:%S")))

    # Stream the content of the object into my code
    put_messages_details = []
    batch_size = int(os.environ.get("batch_size"))

    for i in range(0, len(data), batch_size):
        for row in data[i:i + batch_size]:
            try:
                # Encode key and value, Append into list
                encoded_value = b64encode(str(row).encode()).decode()
                # Append into list
                put_messages_details.append(oci.streaming.models.PutMessagesDetailsEntry(value=encoded_value))
                # Create Message Details with list
                messages = oci.streaming.models.PutMessagesDetails(messages=put_messages_details)

            except Exception as e:
                print("----------Failed to push the following message -------------------")
                print(row)
                print(e)
                print("--------------------------End----------------------------")
        try:
            #print("Current Message: {}".format(messages))
            # PUT messages
            stream_client.put_messages(streaming_ocid, messages)
            # Clear List
            put_messages_details.clear()
        except Exception as e:
            print("----------Failed to put the stream_client message -------------------")
            print(e)
            print("--------------------------End----------------------------")

    print("All messages have been pushed at {}".format(time.strftime("%Y-%m-%d %H:%M:%S")))

def conversionCSVtoJSON(namespace, object_storage,bucket_name, object_name):
    try:
        # Get object name and whow contents of object
        object = object_storage.get_object(namespace, bucket_name, object_name)
        #print(object.data.text)
        reader = csv.DictReader(io.StringIO(object.data.text))
        json_data = json.dumps(list(reader))
        data = json.loads(json_data)
        new_object_name = object_name + '-Completed'
        rename_object_details = oci.object_storage.models.RenameObjectDetails(source_name=object_name,new_name=new_object_name)
        object_storage.rename_object(namespace, bucket_name, rename_object_details)
        #print(json_data)
        #print(data)
    except Exception as e:
        print("----------------- Error while converting from CSV to JSON -------------------")
        print(e)
        print("--------------------------------------End------------------------------------")

    return data
    
#Consumer will be invoked via REST API
def call_consumer():
    try:
        requests.get("Your_Consumer_REST_Endpoint", timeout=5)
    except requests.exceptions.ReadTimeout:
        print("Consumer has been invoked", file=sys.stderr)

def do(signer, bucket_name):
    try:
        # Create StreamAdminClient
        stream_admin_client = oci.streaming.StreamAdminClient(config={}, signer=signer)

        # Get Streaming OCID
        compartment_id = os.environ.get("OCI_COMPARTMENT")
        streaming_summary = get_streaming_ocid(stream_admin_client, compartment_id)
        streaming_ocid = streaming_summary[0]
        stream_service_endpoint = streaming_summary[1]
        # Create StreamClient
        stream_client = oci.streaming.StreamClient(config={},service_endpoint=stream_service_endpoint, signer=signer)
        print("Searching for bucket and object", file=sys.stderr)

        object_storage = oci.object_storage.ObjectStorageClient(config={}, signer=signer)
        namespace = object_storage.get_namespace().data
        all_objects = object_storage.list_objects(namespace, bucket_name).data
        for new in range(len(all_objects.objects)):
            # Get object name
            object_name = all_objects.objects[new].name
            if str(object_name).lower().endswith('.csv'):
                # conversion from CSV to JSON
                data = conversionCSVtoJSON(namespace, object_storage, bucket_name, object_name)
                # Put message into Streaming
                put_messages_streaming(stream_client, streaming_ocid, data)

            else:
                print("There is no CSV in the bucket")
        message = "Streaming is done!!!"
        call_consumer()
    except Exception as e:
        message = "Failed: " + str(e.message)
        #Add trigger oci notification with file name

    response = {
        "content": message
    }
    return response

