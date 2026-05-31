package server

import "testing"

// TestSchedulerEnabled locks in the SchedulerMode policy: Auto is
// opt-in (OFF) today — auto-enabling on "mixed priorities" regressed the
// loopback computer-use benchmark because the scheduler is pure overhead
// without real contention to resolve. On/Off force the choice; the
// deprecated UsePriorityScheduler bool still turns it on under Auto but
// Off overrides it.
func TestSchedulerEnabled(t *testing.T) {
	mk := func(mode SchedulerMode, useBool bool) *Server {
		return &Server{cfg: Config{PriorityScheduler: mode, UsePriorityScheduler: useBool}}
	}
	cases := []struct {
		name string
		srv  *Server
		want bool
	}{
		{"auto default is off (opt-in)", mk(SchedulerAuto, false), false},
		{"auto + deprecated bool => on", mk(SchedulerAuto, true), true},
		{"on forces on", mk(SchedulerOn, false), true},
		{"off forces off", mk(SchedulerOff, false), false},
		{"off overrides deprecated bool", mk(SchedulerOff, true), false},
	}
	for _, tc := range cases {
		if got := tc.srv.schedulerEnabled(); got != tc.want {
			t.Errorf("%s: schedulerEnabled() = %v, want %v", tc.name, got, tc.want)
		}
	}
}
