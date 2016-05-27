# UAT Manifesto

Be specific enough to be useful for verifying correct functionality, but don't get deep into technical details. Don't confuse UAT as a replacement for unit/integration tests. 

Meant to be a contract between programmer and user: This is what the solution can do. The tests prove it. 

Not necessarily fast enough to run in local CI, but should be fast enough to run in reasonable time (TBD, < 5 min?) before pushing commits. 

Should not require deep domain knowledge to run tests or interpret results. In other words, the intended user of the solution should not need the programmer's knowledge to run the tests or understand the results.

Should be able to run anywhere the product is intended to run. Do not require complicated setup or developer environment.

Keep it simple. Spend time and effort on the UAT code infrastructure so that adding new tests isn't a chore. 

When failures occur, there should be copious amounts of developer-relevant logs so that finding and reproducing the failure isn't a chore.

# Configuration
In order to test the s3 mover, the following requirements must be met:

  1. Have a valid AWS account, and an access key/secret
  1. Have an existing s3 bucket suitable for testing

## Example config
~/.lhsmd-test:

    aws_access_key_id = "your aws access key"
    aws_secret_key = "your aws secret key"
    s3_region = "us-east-1"
    s3_bucket = "your s3 bucket name"

Check harness/config.go for other configuration options.
