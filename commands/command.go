package commands

func RegisterCommands(r CommandRegistry) {
	RegisterGetCommand(r)
	RegisterSetCommand(r)
	RegisterDeleteCommand(r)
	RegisterPrefixScan(r)
	RegisterDeletePrefixCommand(r)
	RegisterKeysCommand(r)
	RegisterKVSCommand(r)
	RegisterMGetCommand(r)
	RegisterPrefixScanKeys(r)
}
