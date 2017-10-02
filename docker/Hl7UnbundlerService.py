import os
import sys
import traceback
import json
import urllib
import pandas.io.json as pd
import boto3

flattened_dir = '/hl7/flattened'
input_bucket_name = os.environ['s3InputBucket']
output_bucket_name = os.environ['s3OutputBucket']
sqsqueue_name = os.environ['SQSBatchQueue']
aws_region = os.environ['AWSRegion']
s3 = boto3.client('s3', region_name=aws_region)
sqs = boto3.resource('sqs', region_name=aws_region)


def create_dirs():
  # create directories locally
    for dirs in [flattened_dir]:
        if not os.path.exists(dirs):
            os.makedirs(dirs)


def process_files():
    """Process the json document

    No real error handling in this sample code. In case of error we'll put
    the message back in the queue and make it visable again. It will end up in
    the dead letter queue after five failed attempts.

    """
    for message in get_messages_from_sqs():
        try:
            sys.stdout.write('Processing message...\n')
            message_content = json.loads(message.body)
            file = urllib.unquote_plus(message_content
                                       ['Records'][0]['s3']['object']
                                       ['key']).encode('utf-8')
            sys.stdout.write('Processing file ' + file + '...\n')
            # Download object at input_bucket_name with name 'file' to 'file'
            s3.download_file(input_bucket_name, file, file)
            outFileName = os.path.splitext(file)[0] + "tabular.csv"
            sys.stdout.write('Will be writing to ' + outFileName + '...\n')
            flatten_file(file, outFileName)
            upload_file(file, outFileName)
            cleanup_file(file, outFileName)
        except:
            message.change_visibility(VisibilityTimeout=0)
            traceback.print_exc(file=sys.stderr)
            continue
        else:
            message.delete()


def cleanup_file(file, outFileName):
    # remove file from local dirs
    os.remove(file)
    os.remove(flattened_dir + '/' + outFileName)


def upload_file(file, outFileName):
    # Upload the tabular file to the out bucket
    s3.upload_file(flattened_dir + '/' + outFileName,
                   output_bucket_name, 'flattened/' + outFileName)


def flatten_file(file, outFileName):
    with open(file) as data_file:
        # open file connection and read the json
        data = json.load(data_file)
    entries = []
    for entry in data["entry"]:
        # Unbundle at the "entry" key level
        entries.append(flatten_json(entry))
    # Use Pandas library to flatten the JSON struct
    tabular = pd.json_normalize(entries)
    try:
        # Calculate output file path and write to it it
        outPath = flattened_dir + '/' + outFileName
        sys.stderr.write('Writing csv to ' + outPath + '\n')
        tabular.to_csv(outPath)
    except IOError as e:
        sys.stderr.write('Unable to save csv file to ' + outPath + '... :(\n')
        traceback.print_exc(file=sys.stderr)


def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            # We can go deeper..
            for a in x:
                # recurse
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
              # We can go deeper..
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            if isinstance(x, unicode):
                # Add the built key to the out object
                out[name[:-1]] = x.replace('\n', '').replace('\r\n', '').replace('\r', '') 
                # Remove the pesky new line breaks
            else:
                out[name[:-1]] = x
    flatten(y)
    return out


def get_messages_from_sqs():
    results = []
    queue = sqs.get_queue_by_name(QueueName=sqsqueue_name)
    sys.stdout.write('Checking queue...\n')
    for message in queue.receive_messages(VisibilityTimeout=120,
                                          WaitTimeSeconds=20,
                                          MaxNumberOfMessages=10):
        sys.stdout.write('Got a message!\n')
        results.append(message)
    return(results)


def main():
    sys.stdout.write('Entering main routine for hl7-unbundler\n')
    sys.stdout.write('Evalutaing env variables...\n')
    sys.stdout.write('input_bucket_name: ' + input_bucket_name + '\n')
    sys.stdout.write('output_bucket_name: ' + output_bucket_name + '\n')
    sys.stdout.write('sqsqueue_name: ' + sqsqueue_name + '\n')
    sys.stdout.write('aws_region: ' + aws_region + '\n')
    try:
        sys.stdout.write('Calling create dirs...\n')
        create_dirs()
        while True:
            process_files()
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        raise


if __name__ == "__main__":
    main()
