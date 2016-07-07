@posix
Feature: POSIX data mover
	As a Lustre administrator
	I want to configure a POSIX data mover
	In order to migrate Lustre file data to and from a POSIX filesystem.

Background:
	Given I am the root user
	And I have a Lustre filesystem
	And the HSM coordinator is enabled
	When I configure the HSM Agent
	And I configure the posix data mover
	And I start the HSM Agent
	Then the HSM Agent should be running
	And the posix data mover should be running

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
