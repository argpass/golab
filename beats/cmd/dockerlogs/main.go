package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/argpass/golab/beats/cmd/dockerlogs/internal"
	"github.com/argpass/golab/internal/dockerx"
	"github.com/argpass/golab/internal/libs/utils"
	"github.com/docker/docker/api"
	"github.com/docker/docker/api/types"
	dc "github.com/docker/docker/client"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
)

const (
	maxWaitSeconds = 10 * time.Second
)

var (
	debug         = flag.Bool("debug", true, "debug level ?")
	interestLabel = flag.String("watch", "logRotate", "label to watch collect with")
)

type program struct {
	cli *dc.Client
}

func newProgram() (*program, error) {
	cli, err := dc.NewClient("unix:///var/run/docker.sock", api.DefaultVersion, nil, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "on connecting the docker daemon")
	}
	p := &program{
		cli: cli,
	}
	return p, nil
}

func (p *program) startCollecting(ctx context.Context, digest *internal.Digest) {
	logger := ctx.Value(ctxKeyOfLogger).(*zap.Logger).With(zap.String("ID", digest.ID[:12])).
		With(zap.String("G", "collecting"))
	logger.Info("running",
		zap.String("container", digest.Name),
	)
	dstout, dsterr, err := p.resolveWriter(digest)
	if err != nil {
		logger.Info("err on resolving writers", zap.String("digest", digest.String()), zap.Error(err))
		return
	}
	defer func() {
		dstout.Close()
		dsterr.Close()
	}()

	// create log reader
	rc, err := p.cli.ContainerLogs(
		ctx, digest.ID, types.ContainerLogsOptions{
			Follow: true, ShowStderr: true, ShowStdout: true,
			// todo: record last collecting time in registry to configure `Since`
			Tail: "0",
			//Since: fmt.Sprintf("%d", time.Now().Unix()),
		})
	if err != nil {
		logger.Info("error occurs on reading container logs",
			zap.String("digest", digest.String()), zap.Error(err))
		return
	}
	n, err := dockerx.StdCopy(dstout, dsterr, rc)
	logger.Info("bye", zap.Int64("read", n), zap.Error(err))
}

func (p *program) checkIsInterestedIn(info types.ContainerJSON) (digest *internal.Digest, isInterested bool) {
	for label := range info.Config.Labels {
		if label == *interestLabel {
			isInterested = true
			digest = &internal.Digest{}
			return
		}
	}
	return
}

func (p *program) checkDigest(info types.ContainerJSON) (digest *internal.Digest) {
	for label, value := range info.Config.Labels {
		if label == *interestLabel {
			if digest == nil {
				now := time.Now()
				// new digest
				digest = &internal.Digest{
					ID:          info.ID,
					Name:        info.Name,
					DiscoveryAt: now.UnixNano(),
					UpdatedAt:   now.UnixNano(),
				}
			}
			digest.Type = value
			break
		}
	}
	return digest
}

func (p *program) checking(ctx context.Context, idC <-chan string) error {
	logger := ctx.Value(ctxKeyOfLogger).(*zap.Logger).With(zap.String("G", "checking"))
	logger.Debug("running")
	ctx, cancel := context.WithCancel(ctx)
	wg := &sync.WaitGroup{}
	defer func() {
		cancel()
		wg.Wait()
		logger.Debug("bye")
	}()

	registry := internal.NewRegistry()
	for {
		select {
		case <-ctx.Done():
			return nil
		case ID := <-idC:
			now := time.Now()
			digest, ok := registry.Get(ID)
			if !ok {
				// inspect the container
				info, err := p.cli.ContainerInspect(ctx, ID)
				if err != nil {
					if err == context.Canceled {
						return nil
					}
					logger.Error("error on inspecting container",
						zap.String("ID", ID), zap.Error(err))
					return errors.WithStack(err)
				}
				digest = p.checkDigest(info)
				if digest == nil {
					// i'm not interested in, ignore it
					continue
				}
				registry.Put(info.ID, digest)
			} else {
				// already exists, never to start collecting thread
				continue
			}

			digest.UpdatedAt = now.UnixNano()

			// start a thread to collecting logs of the container
			wg.Add(1)
			go func(ID string) {
				defer func() {
					registry.Remove(ID)
					wg.Done()
				}()
				p.startCollecting(ctx, digest)
			}(ID)
		}
	}
	return nil
}

func (p *program) resolveWriter(digest *internal.Digest) (dstout, dsterr io.WriteCloser, err error) {
	return
}

// Run the program
func (p *program) Run(ctx context.Context) error {
	logger := ctx.Value(ctxKeyOfLogger).(*zap.Logger)
	logger.Debug("program is running")
	ctx, cancel := context.WithCancel(ctx)
	IDC := make(chan string)
	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		return p.checking(ctx, IDC)
	})

	defer func() {
		cancel()
		close(IDC)
		group.Wait()
		logger.Debug("program bye")
	}()

	// list containers to find those i'm interested in
	containers, err := p.cli.ContainerList(ctx, types.ContainerListOptions{})
	if err != nil {
		logger.Error("on list containers", zap.Error(err))
		return errors.WithStack(err)
	}
	for _, c := range containers {
		select {
		case IDC <- c.ID:
			continue
		case <-ctx.Done():
			return nil
		}
	}

	// watch events to discovery new containers joining in
	msgC, errC := p.cli.Events(ctx, types.EventsOptions{})
	for {
		select {
		case msg := <-msgC:
			logger.Debug("recv msg",
				zap.String("ID", fmt.Sprintf("%.12s", msg.ID)),
				zap.String("type", msg.Type),
				zap.String("status", msg.Status))
			if msg.Type == "container" && (msg.Status == "start") {
				select {
				case <-ctx.Done():
				case IDC <- msg.ID:
				}
			}
		case e := <-errC:
			logger.Info("recv err", zap.String("err", fmt.Sprintf("%+v", e)))
			err = errors.WithStack(err)
			return err
		case <-ctx.Done():
			return nil
		}
	}

	return nil
}

func main() {
	begin := time.Now()
	flag.Parse()

	// logger
	var logger *zap.Logger
	var err error
	encConfig := zap.NewProductionEncoderConfig()
	encConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	cfg := zap.NewProductionConfig()
	cfg.EncoderConfig = encConfig
	if *debug {
		cfg.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	}
	logger, err = cfg.Build()
	if err != nil {
		logger.Fatal(err.Error())
	}
	logger.Info("process running", zap.Int("pid", os.Getpid()), zap.Bool("debug", *debug))
	ctx, cancel := context.WithCancel(
		context.WithValue(
			context.Background(), ctxKeyOfLogger, logger))

	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGINT, syscall.SIGTERM)
	// run program
	doneC := make(chan struct{})
	p, err := newProgram()
	if err != nil {
		panic(err)
	}
	go func() {
		utils.BeforeRun()
		defer utils.Cleanup()
		defer close(doneC)
		err := p.Run(ctx)
		if err != nil {
			logger.Error(fmt.Sprintf("program done with err:%+v\n", err))
			// send a signal to kill the process
			select {
			case sigC <- syscall.SIGABRT:
			default:
			}
		}
	}()

	// wait for signals
	select {
	case sig := <-sigC:
		logger.Info("to exit", zap.String("signal", sig.String()))
		// wait for program to exit
		cancel()
		timeoutC := make(chan struct{})
		go func() {
			select {
			case <-doneC:
				// pass
			case <-time.After(maxWaitSeconds):
				logger.Warn("timeout to wait program to exit",
					zap.Duration("timeout", maxWaitSeconds))
				close(timeoutC)
			}
		}()
		select {
		case <-doneC:
		case <-timeoutC:
		}
	}
	logger.Info("bye", zap.Duration("duration", time.Now().Sub(begin)))
}
