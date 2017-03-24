package main

import (
	"github.com/dbjtech/golab/pending/utils"
	"go.uber.org/zap"
	"fmt"
	"flag"
	"context"
	"os"
	"sync"
	"github.com/dbjtech/golab/harvester/libs/constant"
	"github.com/dbjtech/golab/harvester/libs"
	"github.com/dbjtech/golab/harvester/db"
	"github.com/elastic/beats/libbeat/common"
	"github.com/dbjtech/golab/harvester/shipper"
	"github.com/dbjtech/golab/harvester/harvesterd"
	_ "github.com/dbjtech/golab/harvester/db/ari"
	_ "github.com/dbjtech/golab/harvester/shipper/fb"
	"github.com/dbjtech/golab/harvester/libs/cluster"
	"github.com/coreos/etcd/clientv3"
	"time"
)

var (
	confile = flag.String(
		"config",
		"harvester.yml",
		"harverster config file path")
	defaultCluster = ClusterConfig{
		"harverster",
		[]string{"localhost:2379"},
		"localhost:8765",
	}
)

type ClusterConfig struct {
	ClusterName string      `json:"cluster_name"`
	EtcdAddrs   []string    `json:"etcd_addrs"`
	NodeAddr    string      `json:"node_addr"`
}

type program struct {
	logger      *zap.Logger
	etcd3       *clientv3.Client
	
	RawConfig *common.Config
	wg        utils.WrappedWaitGroup
	rootCtx   context.Context
	cancel    context.CancelFunc
	onceStop  sync.Once
	errorsC   chan libs.Error
}

func (p *program) configured() {
	cfg, err := common.LoadFile(*confile)
	if err != nil {
		p.Fatal(err, "configured")
	}
	p.RawConfig = cfg
}

// Start the program
func (p *program) Start() error {
	defer utils.BeforeRun()
	p.logger.Debug("program is starting")
	
	// 1.configure
	var err error
	var clusterConfig       *common.Config
	var dbConfig            *common.Config
	var shipperConfig       *common.Config
	var harvesterdConfig    *common.Config
	p.configured()
	// cluster
	if p.RawConfig.HasField("cluster") {
		clusterConfig, err = p.RawConfig.Child("cluster", -1)
		if err != nil {
			p.Fatal(err, "pick cluster config")
		}
	}else{
		clusterConfig = common.NewConfig()
	}
	confCluster := defaultCluster
	err = clusterConfig.Unpack(&confCluster)
	if err != nil {
		p.Fatal(err, "unpack cluster config")
	}
	
	if p.RawConfig.HasField("db") {
		dbConfig, err = p.RawConfig.Child("db", -1)
		if err != nil {
			p.Fatal(err, "pick db config")
		}
	}else{
		dbConfig = common.NewConfig()
	}
	
	if p.RawConfig.HasField("harvesterd") {
		harvesterdConfig, err = p.RawConfig.Child("harvesterd", -1)
		if err != nil {
			p.Fatal(err, "pick harvesterd config")
		}
	}else{
		harvesterdConfig = common.NewConfig()
	}
	
	if p.RawConfig.HasField("shipper") {
		shipperConfig, err = p.RawConfig.Child("shipper", -1)
		if err != nil {
			p.Fatal(err, "pick shipper config")
		}
	}else{
		shipperConfig = common.NewConfig()
	}
	
	// 2.connect etcd3
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   confCluster.EtcdAddrs,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		p.Fatal(err, "connect etcd3")
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
	// errors chan is no buf
	p.errorsC = make(chan libs.Error)
	p.rootCtx, p.cancel = context.WithCancel(
		context.Background())
	p.rootCtx = context.WithValue(
		p.rootCtx, constant.KEY_LOGGER, p.logger)
	p.rootCtx = context.WithValue(
		p.rootCtx, constant.KEY_ERRORS_W_CHAN,
		(chan<-libs.Error)(p.errorsC))
	p.rootCtx = context.WithValue(p.rootCtx,
		constant.KEY_NODE, node)
	p.rootCtx = context.WithValue(p.rootCtx,
		constant.KEY_P_WG, &p.wg)
	// start the node
	err = node.Start(p.rootCtx)
	p.logger.Debug("node startted")
	if err != nil {
		p.Fatal(err, "start cluster node")
	}
	// errors handler, never to run the method in on `p.wg`
	go p.handleErrors()
	
	// 4.start the db service
	dbService, err := db.New(dbConfig)
	if err != nil {
		p.Fatal(err, "build dbService")
	}
	err = dbService.Start(p.rootCtx)
	if err != nil {
		p.Fatal(err, "start db service")
	}
	p.logger.Debug("start db done")
	
	
	// 5.harvesterd
	
	// entries channel
	entriesC := make(chan *libs.Entry)
	
	harvesterD, err := harvesterd.New(
		harvesterdConfig,
		(<-chan *libs.Entry)(entriesC),
		dbService)
	if err != nil {
		p.Fatal(err, "new harvesterd")
	}
	err = harvesterD.Start(p.rootCtx)
	if err != nil {
		p.Fatal(err, "start harvesterd")
	}
	p.logger.Debug("start harvesterd done")
	
	// 6.shippers
	shipperStarter, err := shipper.New(
		shipperConfig,
		(chan <- *libs.Entry)(entriesC),
	)
	if err != nil {
		p.Fatal(err, "build shipper starter")
	}
	err = shipperStarter.Start(p.rootCtx)
	if err != nil {
		p.Fatal(err, "start shippers")
	}
	p.logger.Debug("start shippers done")
	return nil
}

// Fatal kill the program with fatal error `err`
func (p *program) Fatal(err error, msg string) {
	p.logger.Info(fmt.Sprintf(
		"fatal msg:%s, err:%+v", msg, err))
	os.Exit(1)
}

// handleErrors handle Errors raised by sub goroutines
// if get a Fatal Error(IsFatal==true), i should stop the program
// so never to run the method on `p.wg`
func (p *program) handleErrors() {
	p.logger.Debug("handle errors start")
	for {
		select {
		case <-p.rootCtx.Done():
			goto exit
		case err := <-p.errorsC:
			if err.IsFatal {
				// stop the program
				p.prepareToStop(err, nil)
				p.Fatal(err, "handle errors")
			}
		}
	}
	
exit:
	p.logger.Info("handle errors bye")
}

// prepareToStop cleanup resources and stop goroutines
func (p *program) prepareToStop(err error, sigs []os.Signal)  {
	p.onceStop.Do(func(){
		defer utils.Cleanup()
		// cancel root context
		p.cancel()
		// todo: maybe i should kill the program when wait timeout
		// wait all goroutines to exit
		p.logger.Info("waitting for all goroutines to exit")
		p.wg.Wait()
		p.logger.Debug("root context stop done")
	})
}

func (p *program) StoppedBySignals(sigs... os.Signal) error {
	p.prepareToStop(nil, sigs)
	p.Fatal(nil, "")
	p.logger.Warn(fmt.Sprintf("to be stopped by sigs:%+v", sigs))
	return nil
}

func main()  {
	flag.Parse()
	
	// todo: create logger by `-logging` flag
	logger, err := zap.NewDevelopment()
	
	// etcd
	if err != nil {
		fmt.Printf("got err:%v\n", err)
	}
	if err != nil {
		logger.Fatal(err.Error())
	}
	
	p := &program{logger:logger}
	err = utils.RunProgram(p)
}
