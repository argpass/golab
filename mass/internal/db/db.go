package db

import (
	"context"
	"fmt"
	"sync"

	"github.com/argpass/golab/harvester/libs/constant"
	"github.com/elastic/beats/libbeat/common"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// DBService manages all enabled engines
type DBService struct {
	lock sync.RWMutex

	enabledEngines map[string]IsEngine
	RawConfig      *common.Config
	cfg            Config

	logger *zap.Logger
}

// Start the db service and enable configured engines
func (d *DBService) Start(ctx context.Context) error {

	// init
	d.logger = ctx.Value(constant.KEY_LOGGER).(*zap.Logger).
		With(zap.String("mod", "db"))

	// enable engines with `db.engine` config field
	if d.RawConfig.HasField("engine") {
		enginesConfig, err := d.RawConfig.Child("engine", -1)
		if err != nil {
			return err
		}
		err = d.enableEngines(enginesConfig, ctx)
		if err != nil {
			return err
		}
	}

	// ensure dbs
	err := d.ensureDbs()
	if err != nil {
		return err
	}

	return nil
}

// ensureDbs ensures all db initialized by engines
// we can check if config is ok by this way
func (d *DBService) ensureDbs() error {
	for _, db := range d.cfg.Databases {
		en, ok := d.enabledEngines[db.Engine]
		if !ok {
			return fmt.Errorf(
				"engine %s not found",
				db.Engine)
		}
		err := en.Ensure(db.Name, db.EgConfig)
		if err != nil {
			return err
		}
	}
	return nil
}

// Open a connection to a db
func (d *DBService) Open(db string) (IsDBCon, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()
	// find db config and let engine to open a connection
	for _, cf := range d.cfg.Databases {
		if cf.Name == db {
			en, ok := d.enabledEngines[cf.Engine]
			if !ok {
				return nil, fmt.Errorf("engine %s not found", cf.Engine)
			}
			err := en.Ensure(db, cf.EgConfig)
			if err != nil {
				d.logger.Error(fmt.Sprintf("fail to create db %s", db))
			}
			return en.Open(db)
		}
	}
	return nil, fmt.Errorf("db(%s) hasn't been configured yet", db)
}

func (d *DBService) enableEngines(enginesConfig *common.Config, ctx context.Context) error {
	for _, name := range enginesConfig.GetFields() {
		enCreator, exists := GetEngine(name)
		if !exists {
			return errors.New(fmt.Sprintf("engine %s not found", name))
		}
		cf, err := enginesConfig.Child(name, -1)
		if err != nil {
			return errors.Wrap(err, "pick engine config")
		}
		// create and start an engine instance
		d.enabledEngines[name], err = enCreator(name, cf, ctx)
		if err != nil {
			return errors.Wrapf(err, "enable engine - %s", name)
		}
	}
	return nil
}

func New(cfg *common.Config) (*DBService, error) {
	var cf Config
	err := cfg.Unpack(&cf)
	if err != nil {
		return nil, err
	}
	d := &DBService{RawConfig: cfg, cfg: cf,
		enabledEngines: make(map[string]IsEngine)}
	return d, nil
}
