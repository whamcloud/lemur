package pdm

type (
	Request struct {
		Agent      string // Sender of this request
		Cookie     uint64 // Agent-specified id for request
		SourcePath string // posix://mnt/lustre
		Endpoint   string // s3://bucket
		Archive    uint
		Command    CommandType // PDM Command
		Offset     uint64
		Length     uint64
		Params     string
	}

	CommandType int

	Result struct {
		Agent     string
		Cookie    uint64
		Status    string
		Offset    uint64
		Length    uint64
		ErrorCode int
		Error     string
	}
)

// PDM Commands
const (
	ArchiveCommand = CommandType(iota + 1)
	RestoreCommand
	ReleaseCommand
	RemoveCommand
	CancelCommand
	MigrateCommand
)
