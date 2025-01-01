package server

func RegisterCommands(r ServerCommandRegistry) {
	RegisterSnapshotCommand(r)
	RegisterRestoreCommand(r)
	RegisterMultiCommand(r)
	RegisterExecCommand(r)
	RegisterDiscardCommand(r)
	RegisterPublishCommand(r)
	RegisterPPublishCommand(r)
	RegisterSubscribeCommand(r)
	RegisterPSubscribeCommand(r)
	RegisterPUnsubscribeCommand(r)
	RegisterUnsubscribeCommand(r)
}
