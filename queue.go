package queue

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"

	"github.com/go-redis/redis"
)

type QueueProtocol[T any] interface {
	Enqueue(args T) error
	Listen(callback CallbackFunc[T])
	Dequeue(callback CallbackFunc[T]) error
}

// Caso uma callback retorne um erro, essa estrutura de dados será salva no redis
// com informações sobre o erro, a data em que o error ocorreu e os dados que estavam na fila.
type QueueError[T any] struct {
	Data         T
	ErrorMessage string
	CreatedAt    time.Time
}

type CallbackFunc[T any] func(args T) error

type QueueConfig struct {
	// Número de vezes que uma callback será reexecutada em caso de erro
	Retries int

	// Tempo de espera para executar a callback em caso de erro
	RetriesAfter time.Duration
}

type Queue[T any] struct {
	redis  *redis.Client
	config *QueueConfig
	name   string
}

// Salva os dados de erro no redis.
func (q *Queue[T]) setQueueError(queueError QueueError[T]) error {
	var err error

	queueErrorName := fmt.Sprintf("%s:errors", q.name)

	queueErrors := []QueueError[T]{}
	var errorsBytes []byte

	resp := q.redis.Get(queueErrorName)

	errorsBytes, _ = resp.Bytes()

	if len(errorsBytes) != 0 {
		err = json.Unmarshal(errorsBytes, &queueErrors)

		log.Println(err)
	}

	queueErrors = append(queueErrors, queueError)

	errorsBytes, err = json.Marshal(queueErrors)

	if err != nil {
		return err
	}

	respSet := q.redis.Set(queueErrorName, string(errorsBytes), 0)

	return respSet.Err()
}

// Adiciona um novo valor no final da fila.
func (q *Queue[T]) Enqueue(args T) error {
	var valueToAdd interface{} = args

	isStruct := reflect.ValueOf(args).Kind() == reflect.Struct
	isMap := reflect.ValueOf(args).Kind() == reflect.Map
	isSlice := reflect.ValueOf(args).Kind() == reflect.Slice

	if isSlice || isMap || isStruct {
		argsBytes, err := json.Marshal(args)
		if err != nil {
			return err
		}
		valueToAdd = string(argsBytes)
	}

	resp := q.redis.RPush(q.name, valueToAdd)

	if resp.Err() != nil {
		return resp.Err()
	}

	return nil
}

// Desenfileira um item.
// Executa a callback passando os dados encontrados na primeira posição da fila.
func (q *Queue[T]) Dequeue(callback CallbackFunc[T]) error {
	var (
		args T
		err  error
	)

	resp := q.redis.BLPop(0, q.name)

	if resp.Err() != nil {
		return resp.Err()
	}

	respString := resp.Val()[1]

	isStruct := reflect.ValueOf(args).Kind() == reflect.Struct
	isMap := reflect.ValueOf(args).Kind() == reflect.Map
	isSlice := reflect.ValueOf(args).Kind() == reflect.Slice

	if isMap || isStruct || isSlice {
		err = json.Unmarshal([]byte(respString), &args)
		if err != nil {
			return err
		}
	}

	runSuccessfully := false

	for i := 0; i < q.config.Retries; i++ {
		err = callback(args)

		if err == nil {
			runSuccessfully = true
			break
		}

		log.Printf("Retrying to execute callback (%d)\n", i+1)
		time.Sleep(q.config.RetriesAfter)
	}

	if !runSuccessfully {
		qError := QueueError[T]{
			Data:         args,
			ErrorMessage: err.Error(),
			CreatedAt:    time.Now(),
		}

		err = q.setQueueError(qError)

		if err != nil {
			log.Println(err)
		}
	}

	return err
}

// Verifica se existe algum item na fila a cada 10 segundos.
// Se existir, essa função chamará a função Queue.Dequeue(callback CallbackFunc[T])
func (q *Queue[T]) Listen(callback CallbackFunc[T]) {
	log.Printf("Running Listen from queue: %s\n", q.name)

	go func() {
		var err error

		for {
			redisLen := q.redis.LLen(q.name).Val()

			for redisLen > 0 {
				err = q.Dequeue(callback)
				if err != nil {
					log.Println(err)
					continue
				}
				redisLen--
			}
			time.Sleep(time.Second * 10)
		}
	}()
}

type ProcessableFunc func(wg *sync.WaitGroup)

// Executa funções de forma assíncrona usando WaitGroup para sincronizar as Goroutines.
// As funções passadas por parâmetro recebem um ponteiro para o WaitGroup e devem executar
// WaitGroup.Done() quando finalizarem sua execução
func ProcessAsync(callbacks ...ProcessableFunc) {
	var wg sync.WaitGroup

	wg.Add(len(callbacks))

	for _, callback := range callbacks {
		go callback(&wg)
	}

	wg.Wait()
}

// Cria uma nova instância da fila.
func NewQueue[T any](qName string, config *QueueConfig, redisClient *redis.Client) (QueueProtocol[T], error) {
	// Ping no redis para garantir que a conexão existe
	if ping := redisClient.Ping(); ping.Err() != nil {
		return nil, ping.Err()
	}

	if config == nil {
		config = &QueueConfig{
			Retries:      1,
			RetriesAfter: time.Second,
		}
	}

	return &Queue[T]{
		name:   qName,
		redis:  redisClient,
		config: config,
	}, nil
}
