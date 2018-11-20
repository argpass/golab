package main

import (
	"context"
	"flag"
	"fmt"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/argpass/golab/harvester/db"
	_ "github.com/argpass/golab/harvester/db/ari"
	"github.com/argpass/golab/harvester/harvesterd"
	"github.com/argpass/golab/harvester/libs"
	"github.com/argpass/golab/harvester/libs/cluster"
	"github.com/argpass/golab/harvester/libs/constant"
	"github.com/argpass/golab/harvester/libs/utils"
	"github.com/argpass/golab/harvester/shipper"
	_ "github.com/argpass/golab/harvester/shipper/fb"
	_ "github.com/argpass/golab/harvester/shipper/mock"
	"github.com/elastic/beats/libbeat/common"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	maxWaitSeconds = 10 * time.Second
)

var (
	confile = flag.String(
		"config",
		"harvester.yml",
		"harverster config file path")
	debug = flag.Bool("debug", false, "debug level ?")

	defaultCluster = ClusterConfig{
		"myharvester",
		[]string{"localhost:2379"},
		"localhost:8765",
	}
)

type ClusterConfig struct {
	ClusterName string   `config:"cluster_name"`
	EtcdAddrs   []string `config:"etcd_addrs"`
	NodeAddr    string   `config:"node_addr"`
}

type program struct {
	RawConfig *common.Config
}

func newProgram() (*program, error) {
	p := &program{}
	cfg, err := common.LoadFile(*confile)
	if err != nil {
		return nil, errors.Wrap(err, "load config file")
	}
	p.RawConfig = cfg
	return p, nil
}

// Run the program
func (p *program) Run(ctx context.Context) error {
	logger := ctx.Value(constant.KEY_LOGGER).(*zap.Logger)
	logger.Debug("program is running")
	defer logger.Debug("program bye")

	// 1.configure
	var err error
	var clusterConfig *common.Config
	var dbConfig *common.Config
	var shipperConfig *common.Config
	var harvesterdConfig *common.Config

	// cluster
	if p.RawConfig.HasField("cluster") {
		clusterConfig, err = p.RawConfig.Child("cluster", -1)
		if err != nil {
			return errors.Wrap(err, "pick cluster config")
		}
	} else {
		clusterConfig = common.NewConfig()
	}
	confCluster := defaultCluster
	err = clusterConfig.Unpack(&confCluster)
	if err != nil {
		return errors.Wrap(err, "unpack cluster config")
	}

	if p.RawConfig.HasField("db") {
		dbConfig, err = p.RawConfig.Child("db", -1)
		if err != nil {
			return errors.Wrap(err, "pick db config")
		}
	} else {
		dbConfig = common.NewConfig()
	}

	if p.RawConfig.HasField("harvesterd") {
		harvesterdConfig, err = p.RawConfig.Child("harvesterd", -1)
		if err != nil {
			return errors.Wrap(err, "pick harvesterd config")
		}
	} else {
		harvesterdConfig = common.NewConfig()
	}

	if p.RawConfig.HasField("shipper") {
		shipperConfig, err = p.RawConfig.Child("shipper", -1)
		if err != nil {
			return errors.Wrap(err, "pick shipper config")
		}
	} else {
		shipperConfig = common.NewConfig()
	}

	// 2.connect etcd3
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   confCluster.EtcdAddrs,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return errors.Wrap(err, "fail to connect etcd3")
	}
	p.etcd3 = cli

	// 3.
	// init and inject the context
	// all shared resources will be passed in the context,
	// include inject basic context vars, and the cluster node
	// start the cluster node
	node := cluster.NewNode(
		confCluster.ClusterName,
		confCluster.NodeAddr,
		p.etcd3)

	// context, cancel, stop, values
	gp := utils.NewGroup(ctx)

	// start the node
	err = node.Start(ctx)
	logger.Debug("node startted")
	if err != nil {
		return errors.Wrap(err, "start cluster node")
	}
	// errors handler, never to run the method in on `p.wg`
	//go p.handleErrors()

	// 4.start the db service
	dbService, err := db.New(dbConfig)
	if err != nil {
		return err
	}
	err = dbService.Start(ctx)
	if err != nil {
		return errors.Wrap(err, "start db service")
	}
	p.logger.Debug("start db done")

	// todo-remove
	//gp.Go(func() error {
	//	return nil
	//	con, err := dbService.Open("db_8")
	//	if err != nil {
	//		return errors.Wrap(err, "open db_8")
	//	}
	//	result, err := con.Query().WithTag("mock").Do(ctx)
	//	if err != nil {
	//		return errors.Wrap(err, "do query")
	//	}
	//	for {
	//		select {
	//		case <-ctx.Done():
	//			return ctx.Err()
	//		case <-result.Done():
	//			return nil
	//		case doc := <-result.ResultChan():
	//			fmt.Printf("doc:%v\n", doc)
	//		}
	//	}
	//})

	// 5.harvesterd
	// entries channel
	entriesC := make(chan *libs.Entry)

	harvesterD, err := harvesterd.New(
		harvesterdConfig,
		(<-chan *libs.Entry)(entriesC),
		dbService)
	if err != nil {
		return errors.Wrap(err, "new harvesterd")
	}
	err = harvesterD.Start(ctx)
	if err != nil {
		return errors.Wrap(err, "start harvesterd")
	}
	logger.Debug("start harvesterd done")

	// 6.shippers
	shipperStarter, err := shipper.New(
		shipperConfig,
		(chan<- *libs.Entry)(entriesC),
	)
	if err != nil {
		return errors.Wrap(err, "build shipper starter")
	}
	err = shipperStarter.Start(ctx)
	if err != nil {
		return errors.Wrap(err, "start shippers")
	}
	logger.Debug("start shippers done")

	return nil
}

func main() {
	begin := time.Now()
	flag.Parse()

	// logger
	var logger *zap.Logger
	var err error
	if *debug {
		logger, err = zap.NewDevelopment()
		logger.Debug("run on debug level")
	} else {
		logger, err = zap.NewProduction()
	}
	if err != nil {
		logger.Fatal(err.Error())
	}
	ctx, cancel := context.WithCancel(
		context.WithValue(
			context.Background(), constant.KEY_LOGGER, logger))

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
			logger.Warn(fmt.Sprintf("program done with err:%+v\n", err))
		}
	}()

	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGINT, syscall.SIGTERM)

	// wait for signals
	select {
	case sig := <-sigC:
		logger.Warn("to exit", zap.String("signal", sig.String()))
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
	logger.Warn("bye", zap.Duration("duration", time.Now().Sub(begin)))
}
