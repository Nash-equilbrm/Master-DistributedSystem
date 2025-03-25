package models

// Request cho giao dịch
type TransactionRequest struct {
	From   string `json:"from"`
	To     string `json:"to"`
	Amount int    `json:"amount"`
}

// Response sau khi xử lý giao dịch
type TransactionResponse struct {
	Success bool
	Message string
}
