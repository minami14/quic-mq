package broker

// Config is MessageBroker config.
type Config struct {
	Users []User
}

// User is information used for authentication.
type User struct {
	UserID   string
	Password string
}
