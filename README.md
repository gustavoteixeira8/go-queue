# go-queue

Esse projeto implementa uma fila de processamento simples. O usuário pode enfileirar estruturas de dados usando a função ``Enqueue`` para serem processadas pela callback fornecida na função ``Dequeue``.

A função ``Listen`` serve para monitorar se novos valores foram adicionados na fila. Caso tenha algum valor, a função ``Dequeue`` é chamada até que a fila esteja vazia.
