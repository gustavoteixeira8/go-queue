package main

import (
	"fmt"
	"log"
	"sync"

	"github.com/go-redis/redis"
	queue "github.com/gustavoteixeira8/go-queue"
)

type MailOpts struct {
	To []string
}

type User struct {
	Name string
}

func NewMailQueue() queue.QueueProtocol[MailOpts] {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	mailQueue, err := queue.NewQueue[MailOpts]("mail-queue", nil, rdb)

	if err != nil {
		log.Fatalln(err)
	}

	return mailQueue
}

func NewUserQueue() queue.QueueProtocol[User] {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	userQueue, err := queue.NewQueue[User]("user-queue", nil, rdb)

	if err != nil {
		log.Fatalln(err)
	}

	return userQueue
}

func RunMailQueue(wg *sync.WaitGroup) {
	qMail := NewMailQueue()

	callbackMail := func(args MailOpts) error {
		fmt.Printf("Sending email to %s\n", args.To)

		return nil
	}

	qMail.Listen(callbackMail)

}

func RunUserQueue(wg *sync.WaitGroup) {
	qUser := NewUserQueue()

	callbackUser := func(args User) error {
		fmt.Printf("Hello %s\n", args.Name)

		return nil
	}

	qUser.Listen(callbackUser)

	wg.Done()
}

func AddValuesIntoRedis() {
	// q := external.NewUserQueue()
	qm := NewMailQueue()

	for {
		err := qm.Enqueue(MailOpts{To: []string{"gustavo@email.com"}})

		if err != nil {
			log.Fatalln(err)
		}

		err = qm.Enqueue(MailOpts{To: []string{"ana@email.com"}})

		if err != nil {
			log.Fatalln(err)
		}

		err = qm.Enqueue(MailOpts{To: []string{"iza@email.com"}})

		if err != nil {
			log.Fatalln(err)
		}

		err = qm.Enqueue(MailOpts{To: []string{"nai@email.com"}})

		if err != nil {
			log.Fatalln(err)
		}

		err = qm.Enqueue(MailOpts{To: []string{"tai@email.com"}})

		if err != nil {
			log.Fatalln(err)
		}

		err = qm.Enqueue(MailOpts{To: []string{"ju@email.com"}})

		if err != nil {
			log.Fatalln(err)
		}

		// time.Sleep(time.Millisecond * 500)
	}
}

func ProcessValues() {
	queue.ProcessAsync(RunMailQueue)
}

func main() {
	fmt.Println("Running main")
	// AddValuesIntoRedis()

	ProcessValues()
}
