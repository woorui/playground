package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	gormcache "github.com/woorui/gorm-cache"
)

// GORM_REPO: https://github.com/go-gorm/gorm.git
// GORM_BRANCH: gorm-cache-test
// TEST_DRIVERS: sqlite, mysql, postgres, sqlserver

func TestGORM(t *testing.T) {
	mdb := memdb()

	DB.Use(gormcache.GormCache(mdb, time.Second, gormcache.Models(new(User))))

	user := User{Name: "jz"}
	tx := DB.Begin(&sql.TxOptions{Isolation: sql.LevelDefault})

	tx.Create(&user)

	tx.Create(&User{Name: "zj"})

	var jz User
	err := tx.Model(new(User)).Where("name=?", "jz").First(&jz).Error
	if err != nil {
		t.Fatal("error when call .First")
	}

	var users []User
	err = tx.Model(new(User)).Order("id ASC").Find(&users).Error
	if err != nil {
		t.Fatal("error when call .Find")
	}

	var cacheUser User

	ok, val, _ := mdb.Get(context.TODO(), "SELECT * FROM `users` WHERE name='jz' AND `users`.`deleted_at` IS NULL ORDER BY `users`.`id` LIMIT 1")

	assert.Equal(t, true, ok)

	err = json.Unmarshal([]byte(val), &cacheUser)
	if err != nil {
		assert.Fail(t, "Unmarshal cache data error")
	}

	assert.Equal(t, "jz", cacheUser.Name)

	var cacheUsers []User

	ok, arr, _ := mdb.Get(context.TODO(), "SELECT * FROM `users` WHERE `users`.`deleted_at` IS NULL ORDER BY id ASC")

	assert.Equal(t, true, ok)

	err = json.Unmarshal([]byte(arr), &cacheUsers)
	if err != nil {
		assert.Fail(t, "Unmarshal cache data error")
	}

	assert.Equal(t, "jz", cacheUsers[0].Name)
	assert.Equal(t, "zj", cacheUsers[1].Name)

	tx.Commit()

}

type mdb struct {
	data    *sync.Map
	expires *sync.Map
}

func memdb() gormcache.CacheKV {
	result := &mdb{
		data:    &sync.Map{},
		expires: &sync.Map{},
	}
	return result
}

func (kv *mdb) Get(ctx context.Context, key string) (bool, string, error) {
	ex, ok := kv.expires.Load(key)
	if ok {
		ext := ex.(time.Time)
		if ext.Before(time.Now()) {
			kv.expires.Delete(key)
			kv.data.Delete(key)
			return false, "", nil
		}
		val, ok := kv.data.Load(key)
		if ok {
			return true, val.(string), nil
		}
	}
	return false, "", nil
}

func (kv *mdb) Set(ctx context.Context, key string, value string, exp time.Duration) error {
	kv.data.Store(key, value)
	kv.expires.Store(key, time.Now().Add(exp))
	return nil
}
