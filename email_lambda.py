import json
import boto3

def lambda_handler(event, context):
    if event['httpMethod'] == 'POST':
        body = json.loads(event['body'])
        event_name = body['event_name']
        email = body['email']
        
        ses = boto3.client('ses')
        body = "Do not forget to attend " + event_name + " with your friends at Movie Night."
        ses.send_email(
            Source = 'verified@email.com',
            Destination = {
                'ToAddresses': [
                    email
                ]
            },
            Message = {
                'Subject': {
                    'Data': 'Movie Night Reminder',
                    'Charset': 'UTF-8'
                },
                'Body': {
                    'Text': {
                        'Data': body,
                        'Charset': 'UTF-8'
                    }
                }
            }
        )

        return {
            'statusCode': 200,
            'headers': {
              "Access-Control-Allow-Origin" : "*"  
            },
            'body': 'Email was successfully sent!'
        }
