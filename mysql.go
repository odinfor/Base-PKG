package Base_PKG

import (
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
	"time"
)

var conn *gorm.DB

func GetDBConn() *gorm.DB {
	return conn
}

func InitConn(dsn string) {
	var err error
	conn, err = gorm.Open(mysql.New(mysql.Config{
		DSN:                      dsn,
		DisableDatetimePrecision: true, // 禁用 datetime 精度，MySQL 5.6 之前的数据库不支持
	}), &gorm.Config{
		NowFunc: func() time.Time {
			return time.Now().Local()
		},
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true, // 禁用表名复数
		},
		SkipDefaultTransaction: true,
	})
	if err != nil {
		panic("init db connect fail, error: " + err.Error())
	}

	if SQLDB, err := conn.DB(); err != nil {
		panic("init db connect pool error, get sql.DB object found error: " + err.Error())
	} else {
		SQLDB.SetMaxIdleConns(10)
		SQLDB.SetMaxOpenConns(100)
		SQLDB.SetConnMaxLifetime(60 * time.Minute)
	}
}
