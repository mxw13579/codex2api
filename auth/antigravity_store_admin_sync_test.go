package auth

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 契约编译期校验：admin 侧通过接口探测挂载 Store 统一手动同步入口（Spec §9），
// auth.Store 必须静态实现该方法签名，否则探测永远失败、手动同步空转回退直连。
var _ interface {
	SyncAntigravityAccountForAdmin(ctx context.Context, accountID int64) error
} = (*Store)(nil)

// newAdminSyncTestServer 提供手动同步全链路（token 换票 + userinfo + 项目/配额
// 事实端点）的假上游；/token 记录并发在途数与调用次数，用于验证 flight 单飞。
// adminSyncExpiredCredentials 让存量 access token 处于过期状态：手动同步通道不
// 强制换票，由 client.Sync 在临近过期时自动换票，从而验证真实换票落库路径。
func adminSyncExpiredCredentials() map[string]any {
	return map[string]any{"expires_at": time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)}
}

// newAdminSyncTestServer 构造带并发跟踪的手动同步测试服务器。
func newAdminSyncTestServer(t *testing.T, accessPrefix string) (*httptest.Server, *atomic.Int32, *atomic.Int32) {
	t.Helper()
	var tokenCalls atomic.Int32
	var active atomic.Int32
	var maxActive atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if writeAntigravityRefreshFixture(w, r) {
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/token":
			tokenCalls.Add(1)
			current := active.Add(1)
			for {
				observed := maxActive.Load()
				if current <= observed || maxActive.CompareAndSwap(observed, current) {
					break
				}
			}
			// 拉开时间窗，让并发调用的重叠可被观测；单飞被破坏时 maxActive 会 >1。
			time.Sleep(120 * time.Millisecond)
			active.Add(-1)
			_, _ = io.WriteString(w, `{"access_token":"`+accessPrefix+`-access-new","refresh_token":"`+accessPrefix+`-refresh-new","expires_in":3600}`)
		case "/userinfo":
			_, _ = io.WriteString(w, `{"id":"subject-1","email":"user@example.com","verified_email":true}`)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	useAntigravityTestEndpoints(t, server.URL)
	return server, &tokenCalls, &maxActive
}

// TestAntigravityStoreAdminSyncRefreshesThroughCoordinator 验证统一入口真实生效：
// 换票落库（DB-first CAS）、运行时账号更新、并携带完整 quota/项目事实。
func TestAntigravityStoreAdminSyncRefreshesThroughCoordinator(t *testing.T) {
	_, tokenCalls, maxActive := newAdminSyncTestServer(t, "admin")
	store, db, account, accountID := newAntigravityRefreshTestAccount(t, adminSyncExpiredCredentials())

	if err := store.SyncAntigravityAccountForAdmin(context.Background(), accountID); err != nil {
		t.Fatalf("SyncAntigravityAccountForAdmin: %v", err)
	}
	if tokenCalls.Load() != 1 || maxActive.Load() != 1 {
		t.Fatalf("token calls=%d maxActive=%d, want 1/1", tokenCalls.Load(), maxActive.Load())
	}
	row, err := db.GetAccountByID(context.Background(), accountID)
	if err != nil {
		t.Fatalf("reload row: %v", err)
	}
	if got := row.GetCredential("access_token"); got != "admin-access-new" {
		t.Fatalf("persisted access_token = %q, want admin-access-new", got)
	}
	if got := row.GetCredential("refresh_token"); got != "admin-refresh-new" {
		t.Fatalf("persisted refresh_token = %q, want admin-refresh-new", got)
	}
	if row.GetCredential("antigravity_last_synced_at") == "" {
		t.Fatal("quota sync fact (antigravity_last_synced_at) not persisted")
	}
	account.mu.RLock()
	published := account.AccessToken
	account.mu.RUnlock()
	if published != "admin-access-new" {
		t.Fatalf("runtime access token = %q, want admin-access-new", published)
	}
	// flight 租约必须已释放：随后再取同账号 flight 应立即成功。
	reacquireCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	lease, err := store.antigravityFlightCoordinator().AcquireFlight(reacquireCtx, accountID, "manual_refresh")
	if err != nil {
		t.Fatalf("flight lease leaked after sync: %v", err)
	}
	lease.Release()
}

// TestAntigravityStoreAdminSyncWaitsWhileFlightHeld 验证入口被协调器纳管：
// 已有在途 flight 时，手动同步在 ctx 内有界等待（不并发触网），释放后继续完成。
func TestAntigravityStoreAdminSyncWaitsWhileFlightHeld(t *testing.T) {
	_, tokenCalls, _ := newAdminSyncTestServer(t, "held")
	store, _, _, accountID := newAntigravityRefreshTestAccount(t, adminSyncExpiredCredentials())

	held, err := store.antigravityFlightCoordinator().AcquireFlight(context.Background(), accountID, "manual_refresh")
	if err != nil {
		t.Fatalf("acquire held flight: %v", err)
	}

	result := make(chan error, 1)
	go func() { result <- store.SyncAntigravityAccountForAdmin(context.Background(), accountID) }()

	select {
	case err := <-result:
		t.Fatalf("manual sync bypassed held flight and returned %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	if tokenCalls.Load() != 0 {
		t.Fatalf("manual sync reached network (%d token calls) while flight held", tokenCalls.Load())
	}

	held.Release()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("manual sync after flight release: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("manual sync did not resume after flight release")
	}
	if tokenCalls.Load() != 1 {
		t.Fatalf("token calls = %d, want 1", tokenCalls.Load())
	}
}

// TestAntigravityStoreAdminSyncSerializesConcurrentManualCalls 验证并发手动同步
// 不会破坏单飞：同账号至多一个 flight 在途（上游观测到的最大并发 == 1）。
func TestAntigravityStoreAdminSyncSerializesConcurrentManualCalls(t *testing.T) {
	_, tokenCalls, maxActive := newAdminSyncTestServer(t, "race")
	store, _, _, accountID := newAntigravityRefreshTestAccount(t, adminSyncExpiredCredentials())

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	start := make(chan struct{})
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- store.SyncAntigravityAccountForAdmin(context.Background(), accountID)
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent manual sync: %v", err)
		}
	}
	if maxActive.Load() != 1 {
		t.Fatalf("concurrent in-flight token exchanges = %d, want 1 (flight single-flight broken)", maxActive.Load())
	}
	// 手动通道按需换票：首个调用轮换出新鲜 AT 后，串行跟随的第二个调用必须复用
	// 它（在途去重、不重复消耗 RT），因此至多发生一次真实换票。
	if tokenCalls.Load() != 1 {
		t.Fatalf("token calls = %d, want exactly 1 (second serialized call must reuse rotated token)", tokenCalls.Load())
	}
}
