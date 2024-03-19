from pymongo import MongoClient

# Conectando ao banco de dados
client = MongoClient('localhost', 27017)
db = client.mercado  # Nome do banco de dados
collection = db.produtos  # Nome da coleção

# Média de gasto por cliente
pipeline_avg_spending_per_customer = [
    {"$unwind": "$produtos"},
    {"$group": {"_id": "$cliente_id", "total": {"$sum": {"$multiply": ["$produtos.quantidade", "$produtos.preco"]}}}},
    {"$group": {"_id": None, "media": {"$avg": "$total"}}}
]

# Produto mais vendido
pipeline_most_sold_product = [
    {"$unwind": "$produtos"},
    {"$group": {"_id": "$produtos.descricao", "total": {"$sum": "$produtos.quantidade"}}},
    {"$sort": {"total": -1}},
    {"$limit": 1}
]

# Cliente que mais comprou em cada dia
pipeline_top_customer_per_day = [
    {"$unwind": "$produtos"},
    {"$group": {"_id": {"cliente": "$cliente_id", "data": "$data_compra"}, "total": {"$sum": {"$multiply": ["$produtos.quantidade", "$produtos.preco"]}}}},
    {"$sort": {"_id.data": 1, "total": -1}},
    {"$group": {"_id": "$_id.data", "cliente": {"$first": "$_id.cliente"}, "total": {"$first": "$total"}}}
]

# Executando as consultas

# Média de gasto por cliente
cursor_avg_spending_per_customer = collection.aggregate(pipeline_avg_spending_per_customer)
print("Média de gasto por cliente:")
for result in cursor_avg_spending_per_customer:
    print(result['media'])

# Produto mais vendido
cursor_most_sold_product = collection.aggregate(pipeline_most_sold_product)
print("\nProduto mais vendido:")
for result in cursor_most_sold_product:
    print(result['_id'])

# Cliente que mais comprou em cada dia
cursor_top_customer_per_day = collection.aggregate(pipeline_top_customer_per_day)
print("\nCliente que mais comprou em cada dia:")
for result in cursor_top_customer_per_day:
    print(f"Data: {result['_id']}, Cliente: {result['cliente']}, Total: {result['total']}")