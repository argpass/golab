package ari

import "expvar"

var (
	waitersNum = expvar.NewInt("ari.waiter.num")
)
