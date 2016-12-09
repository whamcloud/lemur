#!/usr/bin/env python

import json
import boto3
import traceback
import re
import os

from base64 import b64decode

from lemur_ci import commit_status
from github import Github
from github.GithubObject import NotSet


def lambda_handler(event, context):
    try:
        # for debugging
        print json.dumps(event)

        message = commit_status.Message(**json.loads(event['Records'][0]['Sns']['Message']))

        authToken = boto3.client('kms').decrypt(CiphertextBlob=b64decode(os.environ['GITHUB_TOKEN']))['Plaintext']
        g = Github(authToken, base_url=message.api_url)
        u = g.get_user()
        repo = None
        if u.login == message.repoOrgOrUser:
            repo = u.get_repo(message.repo)
        else:
            o = g.get_organization(message.repoOrgOrUser)
            repo = o.get_repo(message.repo)

        commit = repo.get_commit(message.sha)
        print commit.create_status(**message.as_status())

    except Exception as e:
        # If any other exceptions which we didn't expect are raised
        # then fail the job and log the exception message.
        print('Function failed due to exception.')
        print(e)
        traceback.print_exc()

    print('Function complete.')
    return "Complete."
