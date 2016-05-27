@s3
Feature: S3 data mover
	As a Lustre administrator
	I want to configure a S3 data mover
	In order to migrate Lustre file data to and from a S3 bucket.

Background:
	Given I am the root user
	And I have a Lustre filesystem
	When I configure the HSM Agent
	And I configure the s3 data mover
	And I start the HSM Agent
	Then the HSM Agent should be running
	And the s3 data mover should be running

Scenario: Archive
	When I archive a test file
	Then the test file should be marked as archived
	And the data for the test file should be archived

Scenario: Explicit restore
	Given I have archived a test file
	And I have released the test file
	When I restore the test file
	Then the data for the test file should be restored

Scenario: Implicit restore
	Given I have archived a test file
	And I have released the test file
	Then the data for the test file should be restored

Scenario: Remove
	Given I have archived a test file
	When I remove the test file
	Then the test file should be marked as unmanaged
	And the data for the test file should be removed
