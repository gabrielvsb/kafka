from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer(
    'pedidos_confirmados',
    bootstrap_servers=['localhost:9092'],
    # auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

print('Aguardando pedidos confirmados para análise...')

total_pedidos = 0
valor_total = 0
for event in consumer:
    print('Analisando pedido...')
    dados_recebidos = event.value
    total_pedidos += 1
    valor_total += int(dados_recebidos['valor'])
    print('Quantidade de pedidos do dia: ', total_pedidos)
    print('Valor total dos pedidos: R$', valor_total)
