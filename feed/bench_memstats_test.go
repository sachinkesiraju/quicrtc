package feed

import "runtime"

// runtimeMemStats and readMemStats are thin wrappers around runtime
// so the benchmark file can capture just the fields it needs without
// caring that runtime.MemStats is a large struct.
type runtimeMemStats struct {
	Mallocs uint64
}

func readMemStats(m *runtimeMemStats) {
	var rt runtime.MemStats
	runtime.ReadMemStats(&rt)
	m.Mallocs = rt.Mallocs
}
