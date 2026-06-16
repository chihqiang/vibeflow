// Package circuit 提供轻量级熔断器实现
// 当外部依赖（MySQL、etcd）连续失败达到阈值后，熔断器打开（拒绝请求），
// 经过冷却期后进入半开状态，允许一次探测请求通过，探测成功则关闭熔断器。
//
// 熔断器三种状态：
//   - Closed（闭合）：正常状态，请求正常通过，连续失败累积计数
//   - Open（打开）：熔断状态，直接拒绝请求（快速失败），经过冷却期后自动进入半开
//   - HalfOpen（半开）：允许一次探测请求，成功则回到 Closed，失败则回到 Open
package circuit

import (
	"sync"
	"time"
)

// State 熔断器状态
type State int

const (
	// StateClosed 闭合状态：请求正常通过
	StateClosed State = iota
	// StateOpen 打开状态：熔断中，直接拒绝请求
	StateOpen
	// StateHalfOpen 半开状态：允许一次探测请求
	StateHalfOpen
)

// String 返回状态的字符串表示
func (s State) String() string {
	switch s {
	case StateClosed:
		return "CLOSED"
	case StateOpen:
		return "OPEN"
	case StateHalfOpen:
		return "HALF_OPEN"
	default:
		return "UNKNOWN"
	}
}

// Breaker 熔断器
// 零值不可用，必须通过 NewBreaker 创建
type Breaker struct {
	mu sync.Mutex

	// 配置
	name             string        // 熔断器名称（用于日志区分）
	maxFailures      int           // 连续失败多少次后熔断
	cooldownInterval time.Duration // 熔断后冷却多久进入半开状态

	// 运行时状态
	state           State
	consecutiveFails int
	lastFailureTime  time.Time
	lastStateChange  time.Time
}

// NewBreaker 创建一个新的熔断器
// name: 熔断器名称，用于日志区分不同依赖
// maxFailures: 连续失败多少次后触发熔断
// cooldown: 熔断后的冷却时间，冷却后进入半开状态
func NewBreaker(name string, maxFailures int, cooldown time.Duration) *Breaker {
	return &Breaker{
		name:             name,
		maxFailures:      maxFailures,
		cooldownInterval: cooldown,
		state:            StateClosed,
		lastStateChange:  time.Now(),
	}
}

// Allow 检查当前是否允许请求通过
// 返回 true 表示请求可以发送，false 表示熔断器打开，应直接拒绝
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()

	switch b.state {
	case StateClosed:
		return true
	case StateOpen:
		// 检查冷却期是否已过
		if now.Sub(b.lastStateChange) >= b.cooldownInterval {
			b.state = StateHalfOpen
			b.lastStateChange = now
			return true
		}
		return false
	case StateHalfOpen:
		// 半开状态只允许一个探测请求，之后进来的请求直接拒绝
		return false
	default:
		return true
	}
}

// Success 报告一次成功，重置连续失败计数，闭合熔断器
func (b *Breaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.consecutiveFails = 0
	if b.state != StateClosed {
		b.state = StateClosed
		b.lastStateChange = time.Now()
	}
}

// Failure 报告一次失败，累积失败计数，达到阈值则打开熔断器
func (b *Breaker) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.consecutiveFails++
	b.lastFailureTime = time.Now()

	if b.state == StateHalfOpen {
		// 半开状态下探测失败，立即重新打开熔断器
		b.state = StateOpen
		b.lastStateChange = time.Now()
		return
	}

	if b.state == StateClosed && b.consecutiveFails >= b.maxFailures {
		b.state = StateOpen
		b.lastStateChange = time.Now()
	}
}

// State 返回当前熔断器状态（线程安全）
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// Name 返回熔断器名称
func (b *Breaker) Name() string {
	return b.name
}

// Stats 返回当前统计信息
func (b *Breaker) Stats() (state State, consecutiveFails int, lastFailure time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state, b.consecutiveFails, b.lastFailureTime
}
