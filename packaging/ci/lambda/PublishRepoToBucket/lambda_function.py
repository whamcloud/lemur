#!/usr/bin/env python

import re
import json
import boto3
import traceback
import tempfile
import shutil
import contextlib
import zipfile
import gzip
import xml.etree.ElementTree as ET
from os import path

from lemur_ci import pipeline

from botocore.client import Config
from boto3 import Session, client

@contextlib.contextmanager
def tempdir():
    dirpath = tempfile.mkdtemp()
    def cleanup():
        shutil.rmtree(dirpath)
    yield dirpath

def lambda_handler(event, context):
    try:
        # for debugging
        print json.dumps(event)

        job = pipeline.Job(event['CodePipeline.job'])
        artifact = job.data['inputArtifacts'][0]
        config = job.data['actionConfiguration']['configuration']
        creds = job.data['artifactCredentials']
        from_bucket = artifact['location']['s3Location']['bucketName']
        from_key = artifact['location']['s3Location']['objectKey']
        to_bucket = config['UserParameters']

        session = Session(aws_access_key_id=creds['accessKeyId'],
                          aws_secret_access_key=creds['secretAccessKey'],
                          aws_session_token=creds['sessionToken'])
        s3 = session.client('s3', config=Config(signature_version='s3v4'))

        keyPrefix = 'devel'
        version = 'UNKNOWN'
        zipMembers = []
        with tempdir() as td:
            with tempfile.NamedTemporaryFile() as tf:
                s3.download_file(from_bucket, from_key, tf.name)
                with zipfile.ZipFile(tf.name, 'r') as zf:
                    zf.extractall(td)
                    zipMembers = zf.namelist()
                # TODO: Figure out how to avoid double-wrapping this
                if 'repo.zip' in zipMembers:
                    with zipfile.ZipFile(path.join(td, 'repo.zip'), 'r') as zf:
                        zf.extractall(td)
                        zipMembers = zf.namelist()

            # extract the RPM version
            r = ET.parse(path.join(td, 'repodata/repomd.xml')).getroot()
            pf = r.find(".//*[@type='primary']/{http://linux.duke.edu/metadata/repo}location").attrib['href']
            with gzip.open(path.join(td, pf)) as gz:
                pr = ET.parse(gz).getroot()
                version = pr.find('.//{http://linux.duke.edu/metadata/common}package/{http://linux.duke.edu/metadata/common}version').attrib['ver']

            if re.match(r'^\d+\.\d+\.\d+$', version):
                keyPrefix = 'release'

            # Get a new s3 client to use IAM Role
            s3 = client('s3')
            for fileName in zipMembers:
                if path.isdir(path.join(td, fileName)):
                    continue
                s3.upload_file(path.join(td, fileName), to_bucket, path.join(keyPrefix, version, fileName))

        pipeline.put_job_success(job.id, "Published repo")

    except Exception as e:
        # If any other exceptions which we didn't expect are raised
        # then fail the job and log the exception message.
        print('Function failed due to exception.')
        print(e)
        traceback.print_exc()
        pipeline.put_job_failure(job.id, 'Function exception: ' + str(e))

    print('Function complete.')
    return "Complete."
