from chalice import Chalice
import boto3
import logging

app = Chalice(app_name='chalice-custom-label')
app.log.setLevel(logging.DEBUG)

rek = boto3.client('rekognition')
sns = boto3.client('sns')

bucket_name = 'so0050-128d09dcc3e5-348052051973-us-east-1-ingest'
suffix = ('jpg', 'png')

@app.on_s3_event(bucket=bucket_name,
                 events=['s3:ObjectCreated:*'], prefix='input')
def invoke_custom_label(event):
    if not event.key.endswith(suffix):
        return

    app.log.debug("Received event for bucket: %s, key: %s",
                  event.bucket, event.key)
    s3uri = 's3://' + event.bucket + '/' + event.key
    app.log.debug("s3uri: %s", s3uri)

    response = rek.detect_custom_labels(
        ProjectVersionArn='arn:aws:rekognition:us-east-1:348052051973:project/bayc2/version/bayc2.2022-06-02T17.38.48/1654162729074',
        Image={
            'S3Object': {
                'Bucket': bucket_name,
                'Name': event.key
            }
        },
        MinConfidence=80.0
    )
    app.log.debug("Custom labels: %s", response['CustomLabels'])
    detected_labels = [item['Name'] for item in response['CustomLabels']]
    app.log.debug("Detected labels: %s", detected_labels)

    response = sns.publish(
        TopicArn='arn:aws:sns:us-east-1:348052051973:MyTopic',
        Message=f'{event.key} is very similar to {detected_labels}, please have a look at {s3uri}',
        Subject=f'Copy cat alert',
    )
    app.log.debug("response: %s", response)