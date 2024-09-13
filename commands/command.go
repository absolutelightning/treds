package commands

func RegisterCommands(r CommandRegistry) {
	RegisterGetCommand(r)
	RegisterSetCommand(r)
	RegisterDeleteCommand(r)
	RegisterScanKVSCommand(r)
	RegisterDeletePrefixCommand(r)
	RegisterKeysCommand(r)
	RegisterKVSCommand(r)
	RegisterMGetCommand(r)
	RegisterScanKeysCommand(r)
	RegisterDBSizeCommand(r)
	RegisterZAddCommand(r)
	RegisterZRangeLexCommand(r)
	RegisterZRangeLexKeysCommand(r)
}
