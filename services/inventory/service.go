package main

import (
	"fmt"
	"github.com/jackc/pgx/v5"
	"net/http"
	"reliability/internal/contracts"
	"reliability/internal/platform"
)

func setup(a *platform.App) {
	a.Mux.HandleFunc("GET /products", func(w http.ResponseWriter, r *http.Request) {
		rows, e := a.DB.Query(r.Context(), "SELECT id,name,price,stock FROM inventory.products ORDER BY id")
		if e != nil {
			platform.Fail(w, 503, e.Error())
			return
		}
		defer rows.Close()
		out := []any{}
		for rows.Next() {
			var id, name string
			var price, stock int
			rows.Scan(&id, &name, &price, &stock)
			out = append(out, map[string]any{"id": id, "name": name, "price": price, "stock": stock})
		}
		platform.JSON(w, 200, out)
	})
	a.Mux.HandleFunc("POST /reservations", func(w http.ResponseWriter, r *http.Request) {
		var op contracts.Operation
		if !platform.Decode(w, r, &op) {
			return
		}
		if op.ID == "" || op.Quantity < 1 || op.Quantity > 100 || op.Product == "" {
			platform.Fail(w, 400, "invalid reservation")
			return
		}
		tx, e := a.DB.Begin(r.Context())
		if e != nil {
			platform.Fail(w, 503, e.Error())
			return
		}
		defer tx.Rollback(r.Context())
		// Transaction advisory lock serializes the same operation before checking existence.
		_, e = tx.Exec(r.Context(), "SELECT pg_advisory_xact_lock(hashtextextended($1,2))", op.ID)
		if e != nil {
			platform.Fail(w, 503, e.Error())
			return
		}
		var p string
		var q int
		var active bool
		e = tx.QueryRow(r.Context(), "SELECT product,quantity,active FROM inventory.reservations WHERE id=$1", op.ID).Scan(&p, &q, &active)
		if e == nil {
			if p != op.Product || q != op.Quantity {
				platform.Fail(w, 409, "operation payload conflict")
				return
			}
			platform.JSON(w, 200, map[string]any{"id": op.ID, "active": active})
			return
		}
		if e != pgx.ErrNoRows {
			platform.Fail(w, 503, e.Error())
			return
		}
		tag, e := tx.Exec(r.Context(), "UPDATE inventory.products SET stock=stock-$2 WHERE id=$1 AND stock >= $2", op.Product, op.Quantity)
		if e != nil {
			platform.Fail(w, 503, e.Error())
			return
		}
		if tag.RowsAffected() == 0 {
			platform.Outcomes.WithLabelValues("inventory", "insufficient_stock").Inc()
			platform.Fail(w, 409, "unknown product or insufficient stock")
			return
		}
		_, e = tx.Exec(r.Context(), "INSERT INTO inventory.reservations(id,product,quantity) VALUES($1,$2,$3)", op.ID, op.Product, op.Quantity)
		if e == nil {
			e = tx.Commit(r.Context())
		}
		if e != nil {
			platform.Fail(w, 503, e.Error())
			return
		}
		platform.Outcomes.WithLabelValues("inventory", "reserved").Inc()
		platform.JSON(w, 201, map[string]any{"id": op.ID, "active": true})
	})
	a.Mux.HandleFunc("GET /reservations/{id}", func(w http.ResponseWriter, r *http.Request) {
		var active bool
		e := a.DB.QueryRow(r.Context(), "SELECT active FROM inventory.reservations WHERE id=$1", r.PathValue("id")).Scan(&active)
		if e == pgx.ErrNoRows {
			platform.Fail(w, 404, "reservation absent")
			return
		}
		if e != nil {
			platform.Fail(w, 503, e.Error())
			return
		}
		platform.JSON(w, 200, map[string]any{"id": r.PathValue("id"), "active": active})
	})
	a.Mux.HandleFunc("DELETE /reservations/{id}", func(w http.ResponseWriter, r *http.Request) {
		tx, e := a.DB.Begin(r.Context())
		if e != nil {
			platform.Fail(w, 503, e.Error())
			return
		}
		defer tx.Rollback(r.Context())
		var p string
		var q int
		var active bool
		e = tx.QueryRow(r.Context(), "SELECT product,quantity,active FROM inventory.reservations WHERE id=$1 FOR UPDATE", r.PathValue("id")).Scan(&p, &q, &active)
		if e == pgx.ErrNoRows {
			platform.JSON(w, 200, map[string]bool{"released": true})
			return
		}
		if e != nil {
			platform.Fail(w, 503, e.Error())
			return
		}
		if active {
			_, e = tx.Exec(r.Context(), "UPDATE inventory.products SET stock=stock+$2 WHERE id=$1", p, q)
			if e == nil {
				_, e = tx.Exec(r.Context(), "UPDATE inventory.reservations SET active=false WHERE id=$1", r.PathValue("id"))
			}
		}
		if e == nil {
			e = tx.Commit(r.Context())
		}
		if e != nil {
			platform.Fail(w, 503, fmt.Sprint(e))
			return
		}
		platform.JSON(w, 200, map[string]bool{"released": true})
	})
}
