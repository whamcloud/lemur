import json
import boto3

client = boto3.client('codepipeline')

class Pipeline:
    def __init__(self, name):
        self.name = name

    @property
    def stages(self):
        return dict([(s['name'], s) for s in client.get_pipeline(name=self.name)['pipeline']['stages']])

    @property
    def states(self):
        return dict((s['stageName'], s) for s in client.get_pipeline_state(name=self.name)['stageStates'])


class Job:
    def __init__(self, val):
        if isinstance(val, dict):
            self.id = val['id']
            self.data = val['data']
        else:
            self.id = val
            self.data = {}

    @property
    def details(self):
        return client.get_job_details(jobId=self.id)

    @property
    def pipeline(self):
        return Pipeline(self.details['jobDetails']['data']['pipelineContext']['pipelineName'])


def continue_job_later(job, token, message):
    """Notify CodePipeline of a continuing job
    
    This will cause CodePipeline to invoke the function again with the
    supplied continuation token.
    
    Args:
        job: The JobID
        message: A message to be logged relating to the job status
        continuation_token: The continuation token
        
    Raises:
        Exception: Any exception thrown by .put_job_success_result()
    
    """
    
    # Use the continuation token to keep track of any job execution state
    # This data will be available when a new job is scheduled to continue the current execution
    continuation_token = json.dumps(token)
    
    print('Putting job continuation (%s)' % token)
    print(message)
    client.put_job_success_result(jobId=job, continuationToken=continuation_token)
 
def put_job_success(job, message):
    """Notify CodePipeline of a successful job
    
    Args:
        job: The CodePipeline job ID
        message: A message to be logged relating to the job status
        
    Raises:
        Exception: Any exception thrown by .put_job_success_result()
    
    """
    print('Putting job success')
    print(message)
    client.put_job_success_result(jobId=job)

def put_job_failure(job, message):
    """Notify CodePipeline of a failed job

    Args:
        job: The CodePipeline job ID
        message: A message to be logged relating to the job status

    Raises:
        Exception: Any exception thrown by .put_job_failure_result()

    """
    print('Putting job failure')
    print(message)
    client.put_job_failure_result(jobId=job, failureDetails={'message': message, 'type': 'JobFailed'})

def get_user_params(job_data, required_params=[]):
    """Decodes the JSON user parameters and validates the required properties.

    Args:
        job_data: The job data structure containing the UserParameters string which should be a valid JSON structure

    Returns:
        The JSON parameters decoded as a dictionary.

    Raises:
        Exception: The JSON can't be decoded or a property is missing.

    """
    try:
        # Get the user parameters which contain the stack, artifact and file settings
        user_parameters = job_data['actionConfiguration']['configuration']['UserParameters']
        decoded_parameters = json.loads(user_parameters)

    except Exception as e:
        # We're expecting the user parameters to be encoded as JSON
        # so we can pass multiple values. If the JSON can't be decoded
        # then fail the job with a helpful message.
        if user_parameters != "":
            raise Exception('UserParameters could not be decoded as JSON')

    for param in required_params:
        if param not in decoded_parameters:
            raise Exception('Your UserParameters JSON must include "%s"' % param)

    return decoded_parameters
