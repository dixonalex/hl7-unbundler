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


def main():
    '''
    list of failures that the solution should be designed to handle
    1. Environment Exceptions/Failures
        a. Missing or incorrect environment variable values should be handled by either reverting to default values
            where appropriate or failing quickly to alert someone to fix the OS level config.
    2. Local IO Exceptions/Failures
        a. Permissions issues would be non-transient and require fast failure and alerting someone to fix
            configuration at the OS level
        b. File locks would potentially be transient and a candidate for a retry policy (though- it seems like our solution
            should not be sharing file resources with other processes)
        c. File name already exists could be handled by appending timestamp or UUIDs to file names
    3. Network Exceptions/Failures
        a. Clients that access resources over a network typically classify their exception types (e.g.: invalid credentials,
         connection timeout, broken connection, etc).
         b. A broken connection is a good example of a candidate for a circuit breaker retry policy
         c. Invalid credentials could raise an exception to alert someone to troubleshoot the issue (i.e. update the
            credentials config)
         d. Connection timeout might indicate that, for a request that typically completes under this time,
            the server is experiencing heavy load. An exponential backoff retry policy could fit here.
    4. HTTP Exceptions/Failures
        a. Each 4xx status codes could have a different handling decision routine that is specific to each use case.
        b. 5xx status codes have some generic handling: 503 Unavailable could undergo a circuit breaker retry policy, and so forth.
        c. 3xx status codes though not explicitly exceptions do require client decisions for redirection
        d. Take care to make sure for any Web APIs we are exposing that we follow the HTTP verb / safety + idempotency
            guidelines
    5. 3rd Party Library Exceptions/Failures
        a. The S3 client's ClientError exception appears to have retry logic baked into it, if the client supplies it
            in the metadata
        b. It would be wise to investigate the obvious failure points, but not go over board. Good generic exception handling, 
            integration testing, and logging would go a long way to inform which exceptions ought to be handled before 
            moving to production.
    6. Data Transformation Failures:
        a. Files that are not properly formatted JSON? These should be routed to a 'quarantine' queue for further analysis
        b. Current solution reads full file into memory for processing. A lazy evaluation would be better suited for large files.
        c. A deeply nested json entry might choke with a recursion error. Though if handling one entry at a time this seems unlikely
            (famous last words :)).
        d. Empty file- do we still want it to write a CSV file?
    7. Other Concerns
        a. Do we need to allow 'replays'? I.e. the scenario where data is processed with the requirements of today
            but new context forces us to alter some rules and re-process tomorrow. On the flip side- should there
            be an enforcement that we process a given file only once (by name or some other pre-defiend identifier).
        b. What do we do with files that fall to dead letter and/or quarantine queues? What is our monitoring and alerting
            solution?
        c. Who operates and maintains this in production? Are there any integrations with monitoring services
            that we need to understand and develop to?
        d. Does the user find this acceptable? How can I get their feedback sooner rather than later? Does it
            satisfy all of their use cases? What about the edge cases that they won't think of?
        e. How (fast) do I deliver this? Can I deliver features/fixes in under a day? An hour? Minutes? How
            automated is my infrastructure provisioning?
        f. Can other developers pick up the codebase and work at the same pace as in 7.e.? Is there a test suite
            to document the code and offer assurance the solution works the way it was designed on their machine?
        g. What is our logging strategy?
    '''
    try:
        create_dirs()
        while True:
            process_files()
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        raise


if __name__ == "__main__":
    main()


def create_dirs():
    '''Create local file processing directories'''
    for dirs in [flattened_dir]:
        if not os.path.exists(dirs):
            os.makedirs(dirs)


def process_files():
    """Process the json document

    Polls the SQS queue, processes any files found, uploads, and cleans up the local files.

    No real error handling in this sample code. In case of error we'll put
    the message back in the queue and make it visable again. It will end up in
    the dead letter queue after five failed attempts.
    """
    for message in get_messages_from_sqs():
        try:
            sys.stdout.write('Processing message...\n')
            message_content = json.loads(message.body) # Lazy eval here
            file = urllib.unquote_plus(message_content
                                       ['Records'][0]['s3']['object']
                                       ['key']).encode('utf-8')
            sys.stdout.write('Processing file ' + file + '...\n')
            # Download object at input_bucket_name with name 'file' to 'file'
            s3.download_file(input_bucket_name, file, file)
            outFileName = os.path.splitext(file)[0] + "tabular.csv"
            sys.stdout.write('Will be writing to ' + outFileName + '...\n')
            flatten_file(file, outFileName)
            upload_file(outFileName)
            cleanup_file(file, outFileName)
        except:
            message.change_visibility(VisibilityTimeout=0)
            traceback.print_exc(file=sys.stderr)
            continue
        else:
            message.delete()


def cleanup_file(file, outFileName):
    '''
    Cleanup the files written locally

    Parameters
    ----------
    file : str
        Name of downloaded file
    outFileName : str
        Name of file that was uploaded
    '''
    os.remove(file)
    os.remove(flattened_dir + '/' + outFileName)


def upload_file(outFileName):
    '''
    Uploads the processed file to the S3 out bucket

    Parameters
    ----------
    outFileName : str
        Name of file to be uploaded
    '''
    s3.upload_file(flattened_dir + '/' + outFileName,
                   output_bucket_name, 'flattened/' + outFileName)


def flatten_file(file, outFileName):
    '''
    Reads file, loads to JSON, flattens to tabular format, writes to CSV

    Parameters
    ----------
    file : str
        Name of downloaded file
    outFileName : str
        Name of CSV file to be written
    '''
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
    '''Recurses through JSON structure and returns string in tabular format

    Builds a dictionary of tabular column names constructed from the JSON key parent/child values.
    Values of these keys are the JSON value itself at that index in the tree.

    Parameters
    ----------
    y : dict
        JSON structure to flatten

    Returns
    -------
    dict
        Tabular structure of the input data
    '''
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            # We can go deeper..
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
              # We can go deeper..
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            if isinstance(x, unicode):
                # Add the built key to the out object and remove new line breaks
                out[name[:-1]] = x.replace('\n', '').replace('\r\n', '').replace('\r', '')
            else:
                out[name[:-1]] = x
    flatten(y)
    return out


def get_messages_from_sqs():
    '''Polls SQS Queue on a fixed time interval and returns an SQS message

    Returns
    -------
    SQS.Message
        A resource representing an Amazon Simple Queue Service (SQS) Message
    '''
    results = []
    queue = sqs.get_queue_by_name(QueueName=sqsqueue_name)
    sys.stdout.write('Checking queue...\n')
    for message in queue.receive_messages(VisibilityTimeout=120,
                                          WaitTimeSeconds=20,
                                          MaxNumberOfMessages=10):
        sys.stdout.write('Got a message!\n')
        results.append(message)
    return(results)
