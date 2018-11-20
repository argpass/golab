package utils

import (
	"expvar"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
)

// cmdline flags
var memprofile, cpuprofile, httpprof *string
var cpuOut *os.File

func init() {
	memprofile = flag.String("memprofile", "", "Write memory profile to this file")
	cpuprofile = flag.String("cpuprofile", "", "Write cpu profile to file")
	httpprof = flag.String("httpprof", "", "Start pprof http server")
}

// ProfileEnabled checks whether the beat should write a cpu or memory profile.
func ProfileEnabled() bool {
	return withMemProfile() || withCPUProfile()
}

func withMemProfile() bool { return *memprofile != "" }
func withCPUProfile() bool { return *cpuprofile != "" }

// BeforeRun takes care of necessary actions such as creating files
// before the beat should run.
func BeforeRun() {
	if withCPUProfile() {
		cpuOut, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(cpuOut)
	}

	if *httpprof != "" {
		go func() {
			log.Println("start pprof endpoint")
			mux := http.NewServeMux()

			// register pprof handler
			mux.HandleFunc("/debug/pprof/", func(w http.ResponseWriter, r *http.Request) {
				http.DefaultServeMux.ServeHTTP(w, r)
			})

			// register metrics handler
			mux.HandleFunc("/debug/vars", metricsHandler)

			endpoint := http.ListenAndServe(*httpprof, mux)
			log.Printf("finished pprof endpoint: %v", endpoint)
		}()
	}
}

// todo: move to an individual package, not all programs need the metrics
// report expvar and all metrics
func metricsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	first := true
	report := func(key string, value interface{}) {
		if !first {
			fmt.Fprintf(w, ",\n")
		}
		first = false
		if str, ok := value.(string); ok {
			fmt.Fprintf(w, "%q: %q", key, str)
		} else {
			fmt.Fprintf(w, "%q: %v", key, value)
		}
	}

	fmt.Fprintf(w, "{\n")
	expvar.Do(func(kv expvar.KeyValue) {
		report(kv.Key, kv.Value)
	})
	fmt.Fprintf(w, "\n}\n")
}

// Cleanup handles cleaning up the runtime and OS environments. This includes
// tasks such as stopping the CPU profile if it is running.
func Cleanup() {
	if withCPUProfile() {
		pprof.StopCPUProfile()
		cpuOut.Close()
	}

	if withMemProfile() {
		runtime.GC()

		writeHeapProfile(*memprofile)

		debugMemStats()
	}
}

func debugMemStats() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	log.Printf("mem Memory stats: In use: %d Total (even if freed): %d System: %d",
		m.Alloc, m.TotalAlloc, m.Sys)
}

func writeHeapProfile(filename string) {
	f, err := os.Create(filename)
	if err != nil {
		log.Printf("Failed creating file %s: %s", filename, err)
		return
	}
	pprof.WriteHeapProfile(f)
	f.Close()

	log.Printf("Created memory profile file %s.", filename)
}
