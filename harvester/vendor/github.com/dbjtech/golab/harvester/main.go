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
	_ "github.com/dbjtech/golab/harvester/shipper/mock"
	"github.com/dbjtech/golab/harvester/libs/cluster"
	"github.com/coreos/etcd/clientv3"
	"time"
	"github.com/pkg/errors"
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
	ClusterName string      `config:"cluster_name"`
	EtcdAddrs   []string    `config:"etcd_addrs"`
	NodeAddr    string      `config:"node_addr"`
}

type program struct {
	logger      *zap.Logger
	etcd3       *clientv3.Client
	err         error
	
	RawConfig *common.Config
	wg        utils.IsGroup
	//cancel    context.CancelFunc
	onceStop  sync.Once
}

func (p *program) configured() error {
	cfg, err := common.LoadFile(*confile)
	if err != nil {
		return errors.Wrap(err, "load config file")
	}
	p.RawConfig = cfg
	return nil
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
	
	// load config
	err = p.configured()
	if err != nil {
		return err
	}
	
	// cluster
	if p.RawConfig.HasField("cluster") {
		clusterConfig, err = p.RawConfig.Child("cluster", -1)
		if err != nil {
			return errors.Wrap(err, "pick cluster config")
		}
	}else{
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
	}else{
		dbConfig = common.NewConfig()
	}
	
	if p.RawConfig.HasField("harvesterd") {
		harvesterdConfig, err = p.RawConfig.Child("harvesterd", -1)
		if err != nil {
			return errors.Wrap(err, "pick harvesterd config")
		}
	}else{
		harvesterdConfig = common.NewConfig()
	}
	
	if p.RawConfig.HasField("shipper") {
		shipperConfig, err = p.RawConfig.Child("shipper", -1)
		if err != nil {
			return errors.Wrap(err, "pick shipper config")
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
		return errors.Wrap(err, "connect etcd3")
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
	gp := utils.NewGroup(context.Background())
	ctx := context.WithValue(
		gp.Context(), constant.KEY_LOGGER, p.logger)
	ctx = context.WithValue(ctx,
		constant.KEY_NODE, node)
	ctx = context.WithValue(ctx,
		constant.KEY_P_WG, gp)
	
	p.wg = gp
	
	// start the node
	err = node.Start(ctx)
	p.logger.Debug("node startted")
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
	p.logger.Debug("start harvesterd done")
	
	// 6.shippers
	shipperStarter, err := shipper.New(
		shipperConfig,
		(chan <- *libs.Entry)(entriesC),
	)
	if err != nil {
		return errors.Wrap(err, "build shipper starter")
	}
	err = shipperStarter.Start(ctx)
	if err != nil {
		return errors.Wrap(err, "start shippers")
	}
	p.logger.Debug("start shippers done")
	
	return nil
}

// stop cleanups resources and stop goroutines
func (p *program) stop(err error, sigs []os.Signal)  {
	p.onceStop.Do(func(){
		defer utils.Cleanup()
		p.err = err
		// cancel root context
		p.wg.Cancel(err)
		// todo: maybe i should kill the program when wait timeout
		// wait all goroutines to exit
		p.logger.Info("waitting for all goroutines to exit")
		p.wg.Wait()
		p.logger.Debug("root context stop done")
	})
}

func (p *program) Stop(err error, sigs... os.Signal) {
	p.logger.Info(fmt.Sprintf("to be stopped by err:%+v, sigs:%+v", err, sigs))
	p.stop(err, sigs)
}

func (p *program) Context() context.Context {
	return p.wg.Context()
}

func (p *program) Err() error {
	return p.err
}

func main()  {
	flag.Parse()
	
	var logger *zap.Logger
	var err error
	if *debug {
		logger, err = zap.NewDevelopment()
		logger.Debug("run on debug level")
	}else {
		logger, err = zap.NewProduction()
	}
	
	// etcd
	if err != nil {
		fmt.Printf("got err:%v\n", err)
	}
	if err != nil {
		logger.Fatal(err.Error())
	}
	
	p := &program{logger:logger}
	err = utils.RunProgram(p)
	if err != nil {
		logger.Error(fmt.Sprintf("bye, err:%+v\n", err))
		os.Exit(199)
		return
	}
	logger.Info("bye")
}
