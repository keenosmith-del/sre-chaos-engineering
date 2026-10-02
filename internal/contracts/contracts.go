package contracts

type OrderRequest struct {
	Product  string `json:"product"`
	Quantity int    `json:"quantity"`
	Decline  bool   `json:"decline"`
}
type Order struct {
	ID        string `json:"id"`
	Product   string `json:"product"`
	Quantity  int    `json:"quantity"`
	Amount    int    `json:"amount"`
	Decline   bool   `json:"decline"`
	State     string `json:"state"`
	Reason    string `json:"reason"`
	TraceID   string `json:"trace_id"`
	CreatedAt string `json:"created_at"`
}
type Operation struct {
	ID       string `json:"id"`
	Product  string `json:"product"`
	Quantity int    `json:"quantity"`
	Amount   int    `json:"amount"`
	Decline  bool   `json:"decline"`
}
