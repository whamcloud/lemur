import json
import re


class Message:
    def __init__(self, repoUrl, sha, state, statusUrl=None, context=None, description=None):
        self.repoUrl = repoUrl
        self.sha = sha
        self.state = state
        self.statusUrl = statusUrl
        self.context = context
        self.description = description

        (self.repoScheme, self.repoHost, self.repoOrgOrUser, self.repo) = re.match(r'^(https?://)([^/]+)/([^/]+)/([^/]+)$', self.repoUrl).groups()

    @property
    def api_url(self):
        return self.repoScheme+'api.'+self.repoHost

    @property
    def json(self):
        return json.dumps(self.as_kwargs())

    def as_kwargs(self):
        return dict((k,v) for k,v in dict(
            repoUrl=self.repoUrl,
            sha=self.sha,
            state=self.state,
            statusUrl=self.statusUrl,
            context=self.context,
            description=self.description
        ).iteritems() if v is not None)

    def as_status(self):
        status = self.as_kwargs()
        if 'statusUrl' in status:
            status['target_url'] = status['statusUrl']
            del status['statusUrl']
        del status['repoUrl']
        del status['sha']
        return status
