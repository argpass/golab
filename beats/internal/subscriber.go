package internal

type Subscriber interface {
	Subscribe() chan<- Beat
}

type IsPublisher interface {
	Connect(subscriber Subscriber)
	DeConnect(subscriber Subscriber)
}
