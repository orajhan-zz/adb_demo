import oci
import csv
import json
import requests
import io
import time
import ast
from base64 import b64encode, b64decode


#Replace with your ADW URL
#url = ["url1","url2"]
url = ["Your_url"]


#delete all data in ADW. Start from scratch
def delete_ords(url):
    for adw_url in url:
        res = requests.delete(adw_url)
        print("status code of deleting data in ADW: {}".format(res.status_code))

#Data loading to ADW
def post_ords(url, decoded_list_messages):
    #print(decoded_list_messages)
    for adw_url in url:
        for row in decoded_list_messages:
            #print("working on :{}".format(row))
            headers = {'Content-type': 'application/json'}
            res = requests.post(adw_url, json=json.loads(row), headers=headers)
            #print("status code: {}".format(res.status_code))

#Get Streaming OCID
def get_streaming_ocid(stream_admin_client, compartment_id):
    list_streams = stream_admin_client.list_streams(compartment_id, lifecycle_state=oci.streaming.models.StreamSummary.LIFECYCLE_STATE_ACTIVE)
    if list_streams.data:
        # If we find an active stream with the correct name, we'll use it.
        print("Streaming Name : {}".format(list_streams.data[0].name))
        sid = list_streams.data[0].id
        messages_endpoint = list_streams.data[0].messages_endpoint
        return sid,messages_endpoint

#Get Cursor by partition
def get_cursor_by_partition(stream_client, streaming_ocid, partition):
    print("Creating a cursor for partition {}".format(partition))
    cursor_details = oci.streaming.models.CreateCursorDetails(partition=partition, type=oci.streaming.models.CreateCursorDetails.TYPE_TRIM_HORIZON)
    response = stream_client.create_cursor(streaming_ocid, cursor_details)
    cursor = response.data.value
    return cursor

#Get cursor by group
def get_cursor_by_group(stream_client, streaming_ocid, group_name, instance_name):
    print(" Creating a cursor for group {}, instance {}".format(group_name, instance_name))
    cursor_details = oci.streaming.models.CreateGroupCursorDetails(group_name=group_name,instance_name=instance_name,type=oci.streaming.models.CreateGroupCursorDetails.TYPE_TRIM_HORIZON, commit_on_get=True)
    response = stream_client.create_group_cursor(streaming_ocid, cursor_details)
    return response.data.value

#read data in streaming to update ADW
def consume_messages_streaming(stream_client, streaming_ocid, group_cursor):
    #Get Cursor
    cursor = group_cursor
    decoded_list_messages = []
    while True:
        #By default, the service returns as many messages as possible. Consider your average message size to help avoid exceeding throughput on the stream.
        get_response = stream_client.get_messages(streaming_ocid, cursor)
        if not get_response.data:
            print("All messages have been consumed at {}".format(time.strftime("%Y-%m-%d %H:%M:%S")))
            print("No Message to process!!!")
            time.sleep(5)
            break

        #Process the messages
        print("How many rows? :{}".format(len(get_response.data)))
        print("ADW update starts at {}".format(time.strftime("%Y-%m-%d %H:%M:%S")))
        print(stream_client.get_group(streaming_ocid, "adb-group").data)

        try:
            for message in get_response.data:
                decoded_data = str(b64decode(message.value.encode()).decode()).replace("'","\"")
                decoded_list_messages.append(decoded_data)


            #Loading data into ADW
            post_ords(url, decoded_list_messages)
            decoded_list_messages.clear()
            last_offset = message.offset
            print("Offset: {}".format(last_offset))

            # use the next-cursor for iteration
            cursor = get_response.headers["opc-next-cursor"]
            print("ADW update ends at {}".format(time.strftime("%Y-%m-%d %H:%M:%S")))
        except Exception as e:
                print("----------------- Failed to consume -------------------")
                print(e)
                print("--------------------------End----------------------------")


if __name__ == "__main__":
    #change bucketname to yours
    bucket_name = "Your bucket"
    #change compartment ocid to yours
    compartment_id = 'Your Compartment'
    signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
    identity_client = oci.identity.IdentityClient(config={}, signer=signer)

    object_storage = oci.object_storage.ObjectStorageClient(config={}, signer=signer)
    namespace = object_storage.get_namespace().data
    #clear data in ADW
    try:
        delete_ords(url)
    except Exception as e:
        print(e)
    #Create StreamAdminClient / StreamClient
    stream_admin_client = oci.streaming.StreamAdminClient(config={}, signer=signer)
    streaming_summary = get_streaming_ocid(stream_admin_client, compartment_id)
    streaming_ocid = streaming_summary[0]
    stream_service_endpoint = streaming_summary[1]
    # Create StreamClient
    # Now service_endpoint is required field
    stream_client = oci.streaming.StreamClient(config={}, service_endpoint=stream_service_endpoint, signer=signer)

    #Get Cursor
    group_cursor = get_cursor_by_group(stream_client, streaming_ocid, "Your-group-name", "Your-instance")
    print(stream_client.get_group(streaming_ocid,"Your-group-name").data)
    while True:
        try:
            consume_messages_streaming(stream_client, streaming_ocid, group_cursor)
            time.sleep(5)
        except Exception as e:
            print("----------------- Error while consuming the messages-------------------")
            print(e)
            print("----------------------------------End----------------------------------")
            break
