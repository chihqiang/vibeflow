// Package logger 提供基于 go.uber.org/zap 的统一日志接口
// 控制台输出为 console 格式（字符串），文件输出为 JSON 格式
package logger

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"chihqiang/vibeflow/infra/config"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

// Logger 是项目统一使用的日志接口
type Logger struct {
	zap *zap.Logger
}

var (
	global   *Logger
	globalMu sync.RWMutex
)

// Init 使用配置初始化全局 Logger，失败返回 error
func Init(cfg config.LoggerConfig) error {
	l, err := New(cfg)
	if err != nil {
		return err
	}
	globalMu.Lock()
	global = l
	globalMu.Unlock()
	return nil
}

// New 创建一个 Logger 实例，控制台 console 格式，文件 JSON 格式
func New(cfg config.LoggerConfig) (*Logger, error) {
	lvl := parseLevel(cfg.Level)

	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderCfg.EncodeLevel = zapcore.CapitalLevelEncoder

	consoleEncoder := zapcore.NewConsoleEncoder(encoderCfg)
	consoleSyncer := zapcore.AddSync(os.Stdout)

	var cores []zapcore.Core
	cores = append(cores, zapcore.NewCore(consoleEncoder, consoleSyncer, lvl))

	if cfg.Dir != "" {
		if err := os.MkdirAll(cfg.Dir, 0755); err != nil {
			return nil, fmt.Errorf("创建日志目录失败 %s: %w", cfg.Dir, err)
		}
		// 使用 lumberjack 实现日志滚动：
		//   - 单文件最大 100MB
		//   - 保留最近 30 个备份文件（按日期和序号）
		//   - 保留最近 30 天的日志
		//   - 旧文件自动压缩为 .gz
		logFile := filepath.Join(cfg.Dir, fmt.Sprintf("vibeflow-%s.log", time.Now().Format("2006-01-02")))
		lj := &lumberjack.Logger{
			Filename:   logFile,
			MaxSize:    100,  // MB
			MaxBackups: 30,
			MaxAge:     30,   // days
			Compress:   true,
		}
		jsonEncoder := zapcore.NewJSONEncoder(encoderCfg)
		fileSyncer := zapcore.AddSync(lj)
		cores = append(cores, zapcore.NewCore(jsonEncoder, fileSyncer, lvl))
	}

	core := zapcore.NewTee(cores...)
	zapLogger := zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1))
	return &Logger{zap: zapLogger}, nil
}

// L 返回全局 Logger 实例，调用方（master/worker）启动时须先调用 Init 初始化
func L() *Logger {
	globalMu.RLock()
	defer globalMu.RUnlock()
	return global
}

func parseLevel(s string) zapcore.Level {
	switch s {
	case "debug":
		return zapcore.DebugLevel
	case "info":
		return zapcore.InfoLevel
	case "warn":
		return zapcore.WarnLevel
	case "error":
		return zapcore.ErrorLevel
	default:
		return zapcore.InfoLevel
	}
}

// Debug 输出 Debug 级别日志
func (l *Logger) Debug(msg string, keysAndValues ...any) {
	l.zap.Sugar().Debugw(msg, keysAndValues...)
}

// Info 输出 Info 级别日志
func (l *Logger) Info(msg string, keysAndValues ...any) {
	l.zap.Sugar().Infow(msg, keysAndValues...)
}

// Warn 输出 Warn 级别日志
func (l *Logger) Warn(msg string, keysAndValues ...any) {
	l.zap.Sugar().Warnw(msg, keysAndValues...)
}

// Error 输出 Error 级别日志
func (l *Logger) Error(msg string, keysAndValues ...any) {
	l.zap.Sugar().Errorw(msg, keysAndValues...)
}

// Fatal 输出 Fatal 级别日志并调用 os.Exit(1) 终止进程
func (l *Logger) Fatal(msg string, keysAndValues ...any) {
	l.zap.Sugar().Fatalw(msg, keysAndValues...)
}

// Sync 刷新缓冲区
func (l *Logger) Sync() {
	_ = l.zap.Sync()
}

// ============================================================================
// 包级便捷函数
// ============================================================================

func Debug(msg string, keysAndValues ...any) { L().Debug(msg, keysAndValues...) }
func Info(msg string, keysAndValues ...any)  { L().Info(msg, keysAndValues...) }
func Warn(msg string, keysAndValues ...any)  { L().Warn(msg, keysAndValues...) }
func Error(msg string, keysAndValues ...any) { L().Error(msg, keysAndValues...) }
func Fatal(msg string, keysAndValues ...any) { L().Fatal(msg, keysAndValues...) }

func Sync() {
	globalMu.RLock()
	defer globalMu.RUnlock()
	if global != nil {
		global.Sync()
	}
}
