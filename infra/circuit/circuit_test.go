package circuit

import (
	"testing"
	"time"
)

func TestBreaker_ClosedToOpen(t *testing.T) {
	b := NewBreaker("test", 3, 100*time.Millisecond)

	// 初始状态应为 Closed
	if b.State() != StateClosed {
		t.Fatalf("expected StateClosed, got %s", b.State())
	}

	// 3 次连续失败后应变为 Open
	b.Failure()
	b.Failure()
	if b.State() != StateClosed {
		t.Fatalf("expected StateClosed after 2 failures, got %s", b.State())
	}
	b.Failure()
	if b.State() != StateOpen {
		t.Fatalf("expected StateOpen after 3 failures, got %s", b.State())
	}

	// Open 状态下 Allow 应返回 false
	if b.Allow() {
		t.Fatal("expected Allow() to return false when Open")
	}
}

func TestBreaker_OpenToHalfOpen(t *testing.T) {
	b := NewBreaker("test", 2, 50*time.Millisecond)

	// 触发熔断
	b.Failure()
	b.Failure()
	if b.State() != StateOpen {
		t.Fatalf("expected StateOpen, got %s", b.State())
	}

	// 等待冷却期
	time.Sleep(60 * time.Millisecond)

	// 冷却期过后，第一次 Allow 应返回 true 并进入 HalfOpen
	if !b.Allow() {
		t.Fatal("expected Allow() to return true after cooldown")
	}
	if b.State() != StateHalfOpen {
		t.Fatalf("expected StateHalfOpen, got %s", b.State())
	}

	// HalfOpen 状态下后续 Allow 应返回 false
	if b.Allow() {
		t.Fatal("expected second Allow() to return false in HalfOpen")
	}
}

func TestBreaker_HalfOpenSuccess(t *testing.T) {
	b := NewBreaker("test", 2, 50*time.Millisecond)

	b.Failure()
	b.Failure()
	time.Sleep(60 * time.Millisecond)
	b.Allow() // 进入 HalfOpen

	// 探测成功，回到 Closed
	b.Success()
	if b.State() != StateClosed {
		t.Fatalf("expected StateClosed after success in HalfOpen, got %s", b.State())
	}
}

func TestBreaker_HalfOpenFailure(t *testing.T) {
	b := NewBreaker("test", 2, 50*time.Millisecond)

	b.Failure()
	b.Failure()
	time.Sleep(60 * time.Millisecond)
	b.Allow() // 进入 HalfOpen

	// 探测失败，回到 Open
	b.Failure()
	if b.State() != StateOpen {
		t.Fatalf("expected StateOpen after failure in HalfOpen, got %s", b.State())
	}
}

func TestBreaker_SuccessResets(t *testing.T) {
	b := NewBreaker("test", 3, 100*time.Millisecond)

	b.Failure()
	b.Failure()
	// 在第三次失败前成功，计数应重置
	b.Success()
	b.Failure()
	b.Failure()
	if b.State() != StateClosed {
		t.Fatalf("expected StateClosed after reset, got %s", b.State())
	}

	b.Failure()
	if b.State() != StateOpen {
		t.Fatalf("expected StateOpen after 3 consecutive failures, got %s", b.State())
	}
}

func TestBreaker_Stats(t *testing.T) {
	b := NewBreaker("test", 5, time.Second)

	b.Failure()
	b.Failure()

	state, fails, lastFail := b.Stats()
	if state != StateClosed {
		t.Fatalf("expected StateClosed, got %s", state)
	}
	if fails != 2 {
		t.Fatalf("expected 2 consecutive failures, got %d", fails)
	}
	if lastFail.IsZero() {
		t.Fatal("expected non-zero lastFailureTime")
	}
}

func TestBreaker_ZeroMaxFailures(t *testing.T) {
	// maxFailures=0 意味着第一次失败就熔断
	b := NewBreaker("test", 0, time.Second)
	b.Failure()
	if b.State() != StateOpen {
		t.Fatalf("expected StateOpen with maxFailures=0, got %s", b.State())
	}
}

func TestBreaker_Name(t *testing.T) {
	b := NewBreaker("mysql-persist", 3, time.Second)
	if b.Name() != "mysql-persist" {
		t.Fatalf("expected name 'mysql-persist', got %s", b.Name())
	}
}
