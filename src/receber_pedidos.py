import json

from kafka import KafkaConsumer
from kafka import KafkaProducer
from json import loads
from json import dumps
from time import sleep

consumer = KafkaConsumer(
    'pedidos',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

print('Aguardando pedidos realizados...')
for event in consumer:
    print('Pedido recebido!')
    dados_recebidos = event.value
    dados = {
        'cliente': dados_recebidos['usuario'],
        'email': dados_recebidos['usuario'] + '@gmail.com',
        'valor': dados_recebidos['valor'],
        'status_pedido': dados_recebidos['status']
    }
    print('---- DADOS DO PEDIDO ----')
    print(json.dumps(dados_recebidos, indent=4))
    print('-------------------------')
    print('Enviando confirmação de pedido...\n\n')
    producer.send('pedidos_confirmados', value=dados)
    sleep(2)
