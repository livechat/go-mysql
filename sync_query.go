package mysql

import "context"

type transactSyncKey struct{}

type transactSync struct {
	Sync bool
}

func getSyncFromCtx(ctx context.Context) *transactSync {
	sync, ok := ctx.Value(transactSyncKey{}).(*transactSync)
	if !ok {
		return nil
	}
	return sync
}

func markSyncNeeded(ctx context.Context) {
	if sync := getSyncFromCtx(ctx); sync != nil {
		sync.Sync = true
	}
}

func checkIfSyncNeeded(ctx context.Context) bool {
	if sync := getSyncFromCtx(ctx); sync != nil {
		return sync.Sync
	}
	return false
}

func WithSync(ctx context.Context) context.Context {
	if sync := getSyncFromCtx(ctx); sync != nil {
		return ctx
	}
	return context.WithValue(ctx, transactSyncKey{}, &transactSync{Sync: false})
}
