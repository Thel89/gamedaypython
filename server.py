from __future__ import print_function

import os
import logging
import json
import urllib2
import boto3

from boto3.dynamodb.conditions import Key

# configure logging
logging.basicConfig(level=logging.INFO)

# environment vars
API_TOKEN = os.getenv("GD_API_TOKEN")
if API_TOKEN is None:
    raise Exception("Must define GD_API_TOKEN environment variable")
API_BASE = os.getenv("GD_API_BASE")
if API_BASE is None:
    raise Exception("Must define GD_API_BASE environment variable")
DYNAMO_TABLE = os.getenv("GD_DYNAMO_TABLE")
if DYNAMO_TABLE is None:
    raise Exception("Must define GD_DYNAMO_TABLE environment variable")
SQS_QUEUE = os.getenv("GD_SQS_QUEUE")
if SQS_QUEUE is None:
    raise Exception("Must define SQS_QUEUE environment variable")

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(DYNAMO_TABLE)

SQS = boto3.resource('sqs')
QUEUE = SQS.get_queue_by_name(QueueName=SQS_QUEUE)


def server():
    while True:
        logging.info("Getting messages from queue...")
        # get messages from queue
        messages = QUEUE.receive_messages(MaxNumberOfMessages=10, WaitTimeSeconds=20)
        for message in messages:
            # parse message
            body = json.loads(message.body)
            msg_id = body['Id']
            part_number = body['PartNumber']
            data = body['Data']
            # process message
            logging.info("Body: {}".format(body))
            # put the part received into dynamo
            proceed = store_message(msg_id, part_number, data)
            # delete message from queue
            message.delete()
            # keep processing
            if proceed is False:
                # we have already processed this message so don't proceed
                logging.info("skipping duplicate message")
                continue
            # try to get the parts of the message from the Dynamo.
            check_messages(msg_id)

def store_message(msg_id, part_number, data):
    """
    stores the message locally on a file on disk for persistence
    """
    try:
        table.put_item(
            Item={
                'msg_id': msg_id,
                'part_number': part_number,
                'data': data
            },
            ConditionExpression='attribute_not_exists(msg_id)')
        return True
    except Exception:
        # conditional update failed since we have already processed this message
        # at this point we can bail since we don't want to process again
        # and lose cash moneys
        return False

def check_messages(msg_id):
    """
    checking to see in dynamo if we have the part already
    """
    # do a get item from dynamo to see if item exists
    db_messages = table.query(KeyConditionExpression=Key('msg_id').eq(msg_id))
    # check if both parts exist
    if db_messages["Count"] == 2:
        # app.logger.debug("got a complete message for %s" % msg_id)
        logging.info("Have both parts for msg_id={}".format(msg_id))
        # We can build the final message.
        result = db_messages["Items"][0]["data"] + db_messages["Items"][1]["data"]
        logging.debug("Assembled message: {}".format(result))
        # sending the response to the score calculator
        # format:
        #   url -> api_base/jFgwN4GvTB1D2QiQsQ8GHwQUbbIJBS6r7ko9RVthXCJqAiobMsLRmsuwZRQTlOEW
        #   headers -> x-gameday-token = API_token
        #   data -> EaXA2G8cVTj1LGuRgv8ZhaGMLpJN2IKBwC5eYzAPNlJwkN4Qu1DIaI3H1zyUdf1H5NITR
        url = API_BASE + '/' + msg_id
        logging.debug("Making request to {} with payload {}".format(url, result))
        req = urllib2.Request(url, data=result, headers={'x-gameday-token':API_TOKEN})
        resp = urllib2.urlopen(req)
        logging.debug("Response from server: {}".format(resp.read()))
        resp.close()

if __name__ == "__main__":
    server()
