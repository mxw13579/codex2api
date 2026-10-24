package database

import (
	"context"
	"database/sql"
	"errors"
	"strings"
)

// Antigravity 渠道级设置（模型重定向等）存放在 system_settings.antigravity_config，
// JSON 由 auth 层解析/校验。和 antigravity_oauth_config 一样用独立的小 UPDATE 读写。

// LoadAntigravityConfig 返回 antigravity_config 原始 JSON，未配置时为 "{}"。
func (db *DB) LoadAntigravityConfig(ctx context.Context) (string, error) {
	var raw string
	err := db.conn.QueryRowContext(ctx, `
		SELECT COALESCE(antigravity_config, '{}')
		FROM system_settings
		WHERE id = 1
	`).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return "{}", nil
	}
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(raw) == "" {
		return "{}", nil
	}
	return raw, nil
}

// SaveAntigravityConfig 持久化 antigravity_config JSON（调用方保证已归一化）。
// 无 guard 的整包写；并发安全的写入口是 SaveAntigravityConfigCAS。
func (db *DB) SaveAntigravityConfig(ctx context.Context, raw string) error {
	if strings.TrimSpace(raw) == "" {
		raw = "{}"
	}
	return db.withSQLiteWriteLock(ctx, func() error {
		if _, err := db.conn.ExecContext(ctx, `
			INSERT INTO system_settings (id) VALUES (1)
			ON CONFLICT (id) DO NOTHING
		`); err != nil {
			return err
		}
		_, err := db.conn.ExecContext(ctx, `
			UPDATE system_settings
			SET antigravity_config = $1
			WHERE id = 1
		`, raw)
		return err
	})
}

// GetAntigravityConfigRaw 返回 antigravity_config 列原文（不做任何解析/重排），空列
// 返回 "{}"。设置 PUT 的读-合并-写循环以它为 CAS 基准。
func (db *DB) GetAntigravityConfigRaw(ctx context.Context) (string, error) {
	return db.LoadAntigravityConfig(ctx)
}

// SaveAntigravityConfigCAS 以整 JSON 比较交换写入 antigravity_config：仅当当前列原文
// 与 oldRaw 完全相同才写入 newRaw，返回是否生效。system_settings 没有 revision/
// updated_at 列，单调性由 JSON 内嵌的 config_revision 承载（Spec §8.2 #2）。
func (db *DB) SaveAntigravityConfigCAS(ctx context.Context, oldRaw, newRaw string) (bool, error) {
	if strings.TrimSpace(oldRaw) == "" {
		oldRaw = "{}"
	}
	if strings.TrimSpace(newRaw) == "" {
		newRaw = "{}"
	}
	var applied bool
	err := db.withSQLiteWriteLock(ctx, func() error {
		if _, err := db.conn.ExecContext(ctx, `
			INSERT INTO system_settings (id) VALUES (1)
			ON CONFLICT (id) DO NOTHING
		`); err != nil {
			return err
		}
		res, err := db.conn.ExecContext(ctx, `
			UPDATE system_settings
			SET antigravity_config = $1
			WHERE id = 1 AND COALESCE(antigravity_config, '{}') = $2
		`, newRaw, oldRaw)
		if err != nil {
			return err
		}
		rows, err := res.RowsAffected()
		if err != nil {
			return err
		}
		applied = rows > 0
		return nil
	})
	return applied, err
}
