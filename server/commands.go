package server

func RegisterCommands(r ServerCommandRegistry) {
	RegisterSnapshotCommand(r)
	RegisterRestoreCommand(r)
	RegisterMultiCommand(r)
	RegisterExecCommand(r)
	RegisterDiscardCommand(r)
}
