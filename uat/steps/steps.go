package steps

// HSMTestFileKey is the prefix/key used to generate and refer to the test file
const HSMTestFileKey = "HSM-test-file"

// Bleah. This is what godog expects, though.
type handlerFn interface{}

// WithMatchers holds all registered step matchers and their handlers
var WithMatchers = map[string]handlerFn{}

func addStep(matcher string, handler handlerFn) {
	WithMatchers[matcher] = handler
}
