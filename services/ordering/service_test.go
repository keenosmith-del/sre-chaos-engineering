package main

import "testing"

func TestSagaTransitions(t *testing.T) {
	for _, pair := range [][2]string{{"PENDING", "INVENTORY_RESERVED"}, {"INVENTORY_RESERVED", "PAYMENT_PENDING"}, {"PAYMENT_PENDING", "RECONCILIATION_REQUIRED"}, {"RECONCILIATION_REQUIRED", "CONFIRMED"}, {"PAYMENT_PENDING", "COMPENSATING"}, {"COMPENSATING", "CANCELLED"}} {
		if !allowed(pair[0], pair[1]) {
			t.Fatalf("expected allowed: %v", pair)
		}
	}
	for _, pair := range [][2]string{{"PENDING", "CONFIRMED"}, {"CONFIRMED", "PAYMENT_PENDING"}, {"COMPENSATING", "CONFIRMED"}, {"CANCELLED", "INVENTORY_RESERVED"}} {
		if allowed(pair[0], pair[1]) {
			t.Fatalf("unsafe transition: %v", pair)
		}
	}
}
