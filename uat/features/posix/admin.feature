Feature: POSIX data mover
	As a Lustre administrator
	I want to configure a POSIX data mover
	In order to archive Lustre files to a POSIX filesystem.

Background:
	Given I am the root user
	And I have a Lustre filesystem
	When I configure the posix data mover
	And I start the HSM Agent
	Then the HSM Agent should be running
	And the posix data mover should be running

Scenario:
	When I archive a test file
	Then the test file should be marked as archived
	And the data for the test file should exist in the backend
