
host-kernel:
	@case "$(shell uname -s)" in \
		Darwin) \
			$(MAKE) -C ../mac-host-kernel \
			;; \
		Linux) \
			$(MAKE) -C ../linux-host-kernel \
			;; \
		*) \
			echo "Unsupported system"; \
			exit 1; \
	esac; \
	exit $?
