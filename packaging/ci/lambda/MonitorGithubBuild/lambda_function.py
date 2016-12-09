#!/usr/bin/env python

import json
import boto3
import traceback
import os

from dateutil import parser
from datetime import datetime as dt
from dateutil.tz import tzlocal

from lemur_ci import pipeline, commit_status

s3 = boto3.client('s3')
sns = boto3.client('sns')

def lambda_handler(event, context):
    try:
        # for debugging
        print json.dumps(event)

        # Extract the Job
        job = pipeline.Job(event['CodePipeline.job'])

        token = None
        sourceStartTime = None
        sourceUrl = None
        sourceRevision = None
        if 'continuationToken' in job.data:
            token = json.loads(job.data['continuationToken'])
            sourceStartTime = parser.parse(token['sourceStartTime'])
            sourceUrl = token['sourceUrl']
            sourceRevision = token['sourceRevision']

        # Get the pipeline object for details about the pipeline
        pl = job.pipeline

        if 'continuationToken' not in job.data:
            # Get the pipeline config so that we can check the source object
            # metadata.
            # NB: This is somewhat racy -- If a new commit comes in before
            # we have a chance to do this, we'll get the wrong commit
            # metadata. There must be a better way to communicate
            # between pipeline stages!
            stages = pl.stages
            sourceConfig = stages['Source']['actions'][0]['configuration']
            sourceMeta = s3.head_object(Bucket=sourceConfig['S3Bucket'], Key=sourceConfig['S3ObjectKey'])['Metadata']
            sourceUrl = sourceMeta['source_html_url']
            sourceRevision = sourceMeta['source_revision']

        # Get the pipeline states in order to figure out where we are
        states = pl.states

        # NB: There can only be 1 source action in the source stage
        sourceAction = [s for s in states['Source']['actionStates'] if 'currentRevision' in s][0]
        if sourceStartTime is None and 'latestExecution' in sourceAction:
            sourceStartTime = sourceAction['latestExecution']['lastStatusChange']

        buildAction = [s for s in states['Build']['actionStates'] if s['actionName'] == 'CodeBuild'][0]
        lastBuild = buildAction['latestExecution']

        now = dt.now().replace(tzinfo=tzlocal())
        state = 'pending'
        targetUrl = None
        if lastBuild['lastStatusChange'] > sourceStartTime:
            if 'externalExecutionUrl' in lastBuild:
                targetUrl = lastBuild['externalExecutionUrl']
            if lastBuild['status'] == 'Succeeded':
                state = 'success'
            elif lastBuild['status'] == 'Failed':
                state = 'failure'
            elif lastBuild['status'] != 'InProgress':
                state = 'error'

        if state == 'pending' and 'continuationToken' in job.data:
            # We've already set the github status and now we're waiting
            # for the build to finish.
            return pipeline.continue_job_later(job.id, token, "Waiting for build to %s" % ("start" if lastBuild['status'] != 'InProgress' else "finish"))
            
        message = commit_status.Message(
            repoUrl=sourceUrl,
            sha=sourceRevision,
            state=state,
            context="lemur-ci/build",
            statusUrl=targetUrl
        )
        print sns.publish(
            TopicArn=os.environ['STATUS_TOPIC_ARN'],
            Message=message.json
        )
        if state == 'pending':
            token = dict(
                previous_job_id = job.id,
                sourceStartTime = sourceStartTime.isoformat(),
                sourceUrl = sourceUrl,
                sourceRevision = sourceRevision
            )
            return pipeline.continue_job_later(job.id, token, "Waiting for build to start")
        pipeline.put_job_success(job.id, 'Notified %s/%s of success' % (sourceUrl, sourceRevision))

    except Exception as e:
        # If any other exceptions which we didn't expect are raised
        # then fail the job and log the exception message.
        print('Function failed due to exception.')
        print(e)
        traceback.print_exc()
        pipeline.put_job_failure(job.id, 'Function exception: ' + str(e))

    print('Function complete.')
    return "Complete."
