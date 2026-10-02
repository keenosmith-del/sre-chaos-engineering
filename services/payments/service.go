package main

import (
	"github.com/jackc/pgx/v5"
	"net/http"
	"reliability/internal/contracts"
	"reliability/internal/platform"
)

func setup(a *platform.App) {
	a.Mux.HandleFunc("POST /authorizations", func(w http.ResponseWriter, r *http.Request) {
		var op contracts.Operation
		if !platform.Decode(w, r, &op) {
			return
		}
		if op.ID == "" || op.Amount < 1 {
			platform.Fail(w, 400, "id and positive amount required")
			return
		}
		status := "AUTHORIZED"
		if op.Decline {
			status = "DECLINED"
		}
		tag, e := a.DB.Exec(r.Context(), "INSERT INTO payments.authorizations(id,amount,decline,status) VALUES($1,$2,$3,$4) ON CONFLICT(id) DO NOTHING", op.ID, op.Amount, op.Decline, status)
		if e != nil {
			platform.Fail(w, 503, e.Error())
			return
		}
		var amount int
		var decline bool
		e = a.DB.QueryRow(r.Context(), "SELECT amount,decline,status FROM payments.authorizations WHERE id=$1", op.ID).Scan(&amount, &decline, &status)
		if e != nil {
			platform.Fail(w, 503, e.Error())
			return
		}
		if amount != op.Amount || decline != op.Decline {
			platform.Fail(w, 409, "operation payload conflict")
			return
		}
		if tag.RowsAffected() > 0 {
			platform.Outcomes.WithLabelValues("payments", status).Inc()
		}
		platform.JSON(w, 200, map[string]string{"id": op.ID, "status": status})
	})
	a.Mux.HandleFunc("GET /authorizations/{id}", func(w http.ResponseWriter, r *http.Request) {
		var status string
		e := a.DB.QueryRow(r.Context(), "SELECT status FROM payments.authorizations WHERE id=$1", r.PathValue("id")).Scan(&status)
		if e == pgx.ErrNoRows {
			platform.Fail(w, 404, "authorization absent")
			return
		}
		if e != nil {
			platform.Fail(w, 503, e.Error())
			return
		}
		platform.JSON(w, 200, map[string]string{"id": r.PathValue("id"), "status": status})
	})
}
