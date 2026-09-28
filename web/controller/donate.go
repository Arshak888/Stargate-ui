package controller

// DonateEntry is one crypto address the project accepts donations on.
type DonateEntry struct {
	// Chain is the ticker + network as the README writes it ("USDT-TRC20").
	Chain string
	// Address is the receiving address, shown verbatim and copied verbatim.
	Address string
}

// donateAddresses mirrors the "## Donate" section of README.md, in the same
// order. It is duplicated rather than parsed because README.md sits outside
// this package and go:embed cannot reach a parent directory — so the copy is
// pinned by TestDonateAddressesMatchReadme, which fails the build if the two
// ever drift. Edit the README and this list together; the test names the diff.
var donateAddresses = []DonateEntry{
	{"USDC-Polygon", "0xd955379bE5813F066Cb81870bfcEA6f4bb862c24"},
	{"USDT-BEP20", "0xd955379bE5813F066Cb81870bfcEA6f4bb862c24"},
	{"USDT-TRC20", "TE5BGUuRce6vF3F4fXSkYY635oUt3M3hzd"},
	{"TRX", "TE5BGUuRce6vF3F4fXSkYY635oUt3M3hzd"},
	{"LTC", "LTbdsmqt38o9bfa3pvcHTbDz6kKzQJzSLn"},
	{"BTC", "bc1qjnedhmc4x85uwqn5zvh25f697n2pmn7etu69pg"},
	{"ETH", "0xd955379bE5813F066Cb81870bfcEA6f4bb862c24"},
	{"USDT-BEP20", "0x068e3F9B46ffB7449E5b761764ebe2C3fA8A3292"},
	{"USDT-TRC20", "TUXtcWyh5WjZBWZ1TEPLDEcZuKSZixsNX5"},
	{"USDT-ERC20", "0x068e3F9B46ffB7449E5b761764ebe2C3fA8A3292"},
	{"TRX", "TUXtcWyh5WjZBWZ1TEPLDEcZuKSZixsNX5"},
	{"LTC", "ltc1qh34v0fky7dh940df4nrjwjh0nzdw3h0xdetulv"},
	{"BTC", "bc1qyde5v9v8csenvwvardpr66u6pzlal4652kqvmv"},
	{"ETH", "0x068e3F9B46ffB7449E5b761764ebe2C3fA8A3292"},
}
