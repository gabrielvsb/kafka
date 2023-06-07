from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer(
    'pedidos_confirmados',
    bootstrap_servers=['localhost:9092'],
    # auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

print('Aguardando pedidos confirmados...')
for event in consumer:
    print('Enviando email...')
    dados_recebidos = event.value
    print('Email enviado para: ', dados_recebidos['email'])
