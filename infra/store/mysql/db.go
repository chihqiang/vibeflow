package mysql

import (
	"fmt"
	"time"

	"chihqiang/vibeflow/infra/config"
	"chihqiang/vibeflow/infra/logger"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	gormLogger "gorm.io/gorm/logger"
)

// OpenDB 根据 MySQLConfig 打开 MySQL 连接并配置连接池
func OpenDB(cfg *config.MySQLConfig) (*gorm.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s&parseTime=True&loc=Local",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database, cfg.Charset)
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: gormLogger.Default.LogMode(gormLogger.Warn),
	})
	if err != nil {
		return nil, err
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}
	if cfg.MaxIdle > 0 {
		sqlDB.SetMaxIdleConns(cfg.MaxIdle)
	}
	if cfg.MaxOpen > 0 {
		sqlDB.SetMaxOpenConns(cfg.MaxOpen)
	}
	if cfg.MaxConnLifetime != "" {
		if d, err := time.ParseDuration(cfg.MaxConnLifetime); err == nil {
			sqlDB.SetConnMaxLifetime(d)
		} else {
			logger.Warn("解析 MaxConnLifetime 失败，使用默认值", "value", cfg.MaxConnLifetime, "error", err)
		}
	}
	logger.Info("MySQL 连接已建立")
	return db, nil
}
