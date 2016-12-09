#!/usr/bin/env python

import json
import boto3
import traceback
import re
import os

from base64 import b64decode
from dateutil import parser
from datetime import datetime as dt
from dateutil.tz import tzlocal

from lemur_ci import pipeline
from github import Github
from github.GithubObject import NotSet

s3 = boto3.client('s3')

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
            # metadata
            stages = pl.stages
            sourceConfig = stages['Source']['actions'][0]['configuration']
            sourceMeta = s3.head_object(Bucket=sourceConfig['S3Bucket'], Key=sourceConfig['S3ObjectKey'])['Metadata']
            sourceUrl = sourceMeta['source_html_url']

        # Get the pipeline states in order to figure out where we are
        states = pl.states

        # NB: There can only be 1 source action in the source stage
        sourceAction = [s for s in states['Source']['actionStates'] if 'currentRevision' in s][0]
        (sourceScheme, sourceHost, sourceOrgOrUser, sourceRepo) = re.match(r'^(https?://)([^/]+)/([^/]+)/([^/]+)$', sourceUrl).groups()
        if sourceRevision is None:
            sourceRevision = sourceMeta['source_revision']
        if sourceStartTime is None and 'latestExecution' in sourceAction:
            sourceStartTime = sourceAction['latestExecution']['lastStatusChange']

        buildAction = [s for s in states['Build']['actionStates'] if s['actionName'] == 'CodeBuild'][0]
        lastBuild = buildAction['latestExecution']

        now = dt.now().replace(tzinfo=tzlocal())
        state = 'pending'
        targetUrl = NotSet
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
            
        authToken = boto3.client('kms').decrypt(CiphertextBlob=b64decode(os.environ['GITHUB_TOKEN']))['Plaintext']
        g = Github(authToken, base_url=sourceScheme+'api.'+sourceHost)
        u = g.get_user()
        repo = None
        if u.login == sourceOrgOrUser:
            repo = u.get_repo(sourceRepo)
        else:
            o = g.get_organization(sourceOrgOrUser)
            repo = o.get_repo(sourceRepo)

        commit = repo.get_commit(sourceRevision)
        print commit.create_status(state, description="CodePipeline job %s status: %s" % (job.id, state), target_url=targetUrl, context="continuous-integration/aws-codepipeline")
        if state == 'pending':
            token = dict(
                previous_job_id = job.id,
                sourceStartTime = sourceStartTime.isoformat(),
                sourceUrl = sourceUrl,
                sourceRevision = sourceRevision
            )
            return pipeline.continue_job_later(job.id, token, "Waiting for build to start")
        pipeline.put_job_success(job.id, 'Notified %s/%s' % (repo.full_name, sourceRevision))

    except Exception as e:
        # If any other exceptions which we didn't expect are raised
        # then fail the job and log the exception message.
        print('Function failed due to exception.')
        print(e)
        traceback.print_exc()
        pipeline.put_job_failure(job.id, 'Function exception: ' + str(e))

    print('Function complete.')
    return "Complete."
