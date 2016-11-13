package pgxx

import (
	"context"
	"fmt"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx"
	"github.com/opentracing/opentracing-go"
	"github.com/satori/go.uuid"
)

type QueryObserver func(ctx context.Context, begin time.Time, name, file string, line int, sql string, vars []interface{}, transactionID uuid.UUID, dur time.Duration)

type DB struct {
	cp            *pgx.ConnPool
	observers     []QueryObserver
	projectPrefix string
}

type Option func(*DB)

func New(cp *pgx.ConnPool, options ...Option) *DB {
	db := DB{cp: cp}

	for _, option := range options {
		option(&db)
	}

	return &db
}

func WithQueryObserver(fn QueryObserver) Option {
	return func(d *DB) {
		d.observers = append(d.observers, fn)
	}
}

func WithProjectPrefix(projectPrefix string) Option {
	return func(d *DB) {
		d.projectPrefix = projectPrefix
	}
}

func (d *DB) location(skip int) (string, string, int) {
	if pc, file, line, ok := runtime.Caller(skip); ok {
		if d.projectPrefix != "" {
			if p := strings.Index(file, d.projectPrefix); p != -1 {
				file = file[p:]
			}
		}

		return runtime.FuncForPC(pc).Name(), file, line
	}

	return "unknown_function", "unknown_file", -1
}

func (d *DB) trigger(ctx context.Context, begin time.Time, name, file string, line int, sql string, vars []interface{}, transactionID uuid.UUID, dur time.Duration) {
	for _, fn := range d.observers {
		fn(ctx, begin, name, file, line, sql, vars, transactionID, dur)
	}
}

func (d *DB) triggerNow(ctx context.Context, begin time.Time, name, file string, line int, sql string, vars []interface{}, transactionID uuid.UUID) {
	d.trigger(ctx, begin, name, file, line, sql, vars, transactionID, time.Since(begin))
}

func (d *DB) prepareTimeAndSpan(ctx context.Context, spanType string, transactionID uuid.UUID, sql string, vars []interface{}) (time.Time, string, string, int, opentracing.Span, context.Context) {
	begin := time.Now()

	name, file, line := d.location(3)

	span, newContext := opentracing.StartSpanFromContext(ctx, spanType, opentracing.Tags{
		"query":          PrintQuery(sql, vars),
		"transaction_id": transactionID.String(),
		"source":         fmt.Sprintf("%s@%s:%d", name, file, line),
	})

	return begin, name, file, line, span, newContext
}

func (d *DB) Query(ctx context.Context, sql string, vars ...interface{}) (*pgx.Rows, error) {
	begin, name, file, line, span, _ := d.prepareTimeAndSpan(ctx, "sql-query", uuid.Nil, sql, vars)

	rows, err := d.cp.Query(sql, vars...)
	if rows != nil {
		rows.AfterClose(func(_ *pgx.Rows) {
			span.Finish()
			d.triggerNow(ctx, begin, name, file, line, sql, vars, uuid.Nil)
		})
	} else {
		span.Finish()
		d.triggerNow(ctx, begin, name, file, line, sql, vars, uuid.Nil)
	}

	return rows, err
}

func (d *DB) QueryRow(ctx context.Context, sql string, vars ...interface{}) *pgx.Row {
	begin, name, file, line, span, _ := d.prepareTimeAndSpan(ctx, "sql-query", uuid.Nil, sql, vars)
	defer span.Finish()
	defer d.triggerNow(ctx, begin, name, file, line, sql, vars, uuid.Nil)

	return d.cp.QueryRow(sql, vars...)
}

func (d *DB) Exec(ctx context.Context, sql string, vars ...interface{}) (pgx.CommandTag, error) {
	begin, name, file, line, span, _ := d.prepareTimeAndSpan(ctx, "sql-query", uuid.Nil, sql, vars)
	defer span.Finish()
	defer d.triggerNow(ctx, begin, name, file, line, sql, vars, uuid.Nil)

	return d.cp.Exec(sql, vars...)
}

func (d *DB) Begin(ctx context.Context) (*Tx, error) {
	id := uuid.NewV4()

	_, _, _, _, _, newContext := d.prepareTimeAndSpan(ctx, "sql-transaction", id, "", nil)
	begin, name, file, line, span, _ := d.prepareTimeAndSpan(newContext, "sql-transaction-begin", id, "BEGIN", nil)
	defer span.Finish()
	defer d.triggerNow(ctx, begin, name, file, line, "BEGIN", nil, id)

	tx, err := d.cp.Begin()
	return &Tx{ctx: newContext, id: id, db: d, tx: tx}, err
}

type Tx struct {
	ctx context.Context
	id  uuid.UUID
	db  *DB
	tx  *pgx.Tx
}

func (t *Tx) Commit(ctx context.Context) error {
	if t.tx.Status() != pgx.TxStatusInProgress {
		return t.tx.Err()
	}

	begin, name, file, line, span, _ := t.db.prepareTimeAndSpan(ctx, "sql-transaction-commit", uuid.Nil, "COMMIT", nil)
	defer opentracing.SpanFromContext(t.ctx).Finish()
	defer span.Finish()
	defer t.db.triggerNow(ctx, begin, name, file, line, "COMMIT", nil, t.id)

	return t.tx.Commit()
}

func (t *Tx) Rollback(ctx context.Context) error {
	if t.tx.Status() != pgx.TxStatusInProgress {
		return t.tx.Err()
	}

	begin, name, file, line, span, _ := t.db.prepareTimeAndSpan(ctx, "sql-transaction-rollback", uuid.Nil, "ROLLBACK", nil)
	defer opentracing.SpanFromContext(t.ctx).Finish()
	defer span.Finish()
	defer t.db.triggerNow(ctx, begin, name, file, line, "ROLLBACK", nil, t.id)

	return t.tx.Rollback()
}

func (t *Tx) Query(ctx context.Context, sql string, vars ...interface{}) (*pgx.Rows, error) {
	begin, name, file, line, span, _ := t.db.prepareTimeAndSpan(ctx, "sql-query", t.id, sql, vars)

	rows, err := t.tx.Query(sql, vars...)
	if rows != nil {
		rows.AfterClose(func(_ *pgx.Rows) {
			span.Finish()
			t.db.triggerNow(ctx, begin, name, file, line, sql, vars, t.id)
		})
	} else {
		span.Finish()
		t.db.triggerNow(ctx, begin, name, file, line, sql, vars, t.id)
	}

	return rows, err
}

func (t *Tx) QueryRow(ctx context.Context, sql string, vars ...interface{}) *pgx.Row {
	begin, name, file, line, span, _ := t.db.prepareTimeAndSpan(ctx, "sql-query", t.id, sql, vars)
	defer span.Finish()
	defer t.db.triggerNow(ctx, begin, name, file, line, sql, vars, t.id)

	return t.tx.QueryRow(sql, vars...)
}

func (t *Tx) Exec(ctx context.Context, sql string, vars ...interface{}) (pgx.CommandTag, error) {
	begin, name, file, line, span, _ := t.db.prepareTimeAndSpan(ctx, "sql-query", t.id, sql, vars)
	defer span.Finish()
	defer t.db.triggerNow(ctx, begin, name, file, line, sql, vars, t.id)

	return t.tx.Exec(sql, vars...)
}

func PrintQuery(sql string, vars []interface{}) string {
	re := regexp.MustCompile(`\$([0-9]+)`)
	ws := regexp.MustCompile(`\s+`)

	return strings.TrimSpace(ws.ReplaceAllString(re.ReplaceAllStringFunc(sql, func(s string) string {
		i, err := strconv.ParseInt(s[1:], 10, 64)
		if err != nil {
			return s
		}

		if i < 1 || int(i) > len(vars) {
			return s
		}

		return fmt.Sprintf("'%v'", vars[i-1])
	}), " "))
}
