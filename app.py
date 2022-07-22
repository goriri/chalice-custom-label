from chalice import Chalice
import boto3
import logging

app = Chalice(app_name='chalice-custom-label')
app.log.setLevel(logging.DEBUG)

rek = boto3.client('rekognition')
sns = boto3.client('sns')
comp = boto3.client('comprehend')
s3 = boto3.resource('s3')

bucket_name = 'so0050-128d09dcc3e5-348052051973-us-east-1-ingest'
img_suffix = ('jpg', 'png')
txt_suffix = ('csv', 'txt', 'json')


@app.on_s3_event(bucket=bucket_name,
                 events=['s3:ObjectCreated:*'])
def invoke_custom_label(event):
    app.log.debug("Received event for bucket: %s, key: %s",
                  event.bucket, event.key)
    s3uri = 's3://' + event.bucket + '/' + event.key
    app.log.debug("s3uri: %s", s3uri)

    if event.key.endswith(img_suffix):
        handle_image(event.bucket, event.key)
    elif event.key.endswith(txt_suffix):
        handle_text(event.bucket, event.key)


def handle_image(bucket, key):
    s3uri = 's3://' + bucket + '/' + key
    response = rek.detect_custom_labels(
        ProjectVersionArn='arn:aws:rekognition:us-east-1:348052051973:project/bayc2/version/bayc2.2022-06-02T17.38.48/1654162729074',
        Image={
            'S3Object': {
                'Bucket': bucket,
                'Name': key
            }
        },
        MinConfidence=80.0
    )
    app.log.debug("Custom labels: %s", response['CustomLabels'])
    detected_labels = [item['Name'] for item in response['CustomLabels']]

    if len(detected_labels) == 0:
        return

    app.log.debug("Detected labels: %s", detected_labels)

    response = sns.publish(
        TopicArn='arn:aws:sns:us-east-1:348052051973:MyTopic',
        Message=f'{key} is very similar to {detected_labels}, please have a look at {s3uri}',
        Subject=f'Copy cat alert',
    )
    app.log.debug("response: %s", response)


def handle_text(bucket, key):
    s3uri = 's3://' + bucket + '/' + key
    text = s3.Object(bucket, key).get()['Body'].read().decode('utf-8')
    scam_response = comp.classify_document(
        Text=text[:4000],
        EndpointArn='arn:aws:comprehend:us-east-1:348052051973:document-classifier-endpoint/scam'
    )
    app.log.debug("scam_response: %s", scam_response)
    is_scam = False
    is_toxic = False
    for item in scam_response['Classes']:
        if item['Score'] > 0.5 and item['Name'] == '0':
            is_scam = True
            app.log.debug("Document is scam")

    toxic_response = comp.classify_document(
        Text=text[:4000],
        EndpointArn='arn:aws:comprehend:us-east-1:348052051973:document-classifier-endpoint/toxic'
    )
    app.log.debug("toxic_response: %s", toxic_response)

    toxic_labels = [item['Name'] for item in toxic_response['Labels'] if item['Score'] > 0.5 and item['Name'] != 'non-toxic']
    if len(toxic_labels) > 0:
        is_toxic = True
        app.log.debug("Document is toxic")

    if is_scam or is_toxic:
        response = sns.publish(
            TopicArn='arn:aws:sns:us-east-1:348052051973:MyTopic',
            Message=f'Scam: {is_scam}, toxic toxic_labels: {toxic_labels}, please have a look at {s3uri}',
            Subject=f'Scam/toxic document detected',
        )
        app.log.debug("response: %s", response)

    # response = comp.start_document_classification_job(
    #     JobName='test',
    #     DocumentClassifierArn='arn:aws:comprehend:us-east-1:348052051973:document-classifier/ScamModel/version/2',
    #     InputDataConfig={
    #         'S3Uri': s3uri,
    #         'InputFormat': 'ONE_DOC_PER_LINE',
    #     },
    #     OutputDataConfig={
    #         'S3Uri': output_s3uri+'scam',
    #     },
    #     DataAccessRoleArn='arn:aws:iam::348052051973:role/service-role/AmazonComprehendServiceRoleS3FullAccess-ComprehendLabs',
    # )
