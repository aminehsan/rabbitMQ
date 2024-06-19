# A Python library for interacting with RabbitMQ - message broker

## How to run Python
```bash
broker = RabbitMQ(host='...', port=..., username='...', password='...', heartbeat=...)

def callback(ch, method, properties, body):
    data = body
    broker.accept(method=method)
    broker.reject(method=method, body=body)


broker.listen(prefetch_count=1, queue='...', callback=callback)
broker.send(queue='...', body=data)
broker.close()
```

#### Structure :
```
├── main.py
```
