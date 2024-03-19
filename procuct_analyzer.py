from helper.writeAJson import writeAJson

class Product_analyzer:
    def __init__(self, db):
        self.database = db

    def total_vendas_por_dia(self):
        result = self.database.collection.aggregate([
            {"$unwind": "$produtos"},
            {"$group": {"_id": "$data_compra","total_vendas": {"$sum": {"$multiply": ["$produtos.quantidade", "$produtos.preco"]}}}}   
        ])
        writeAJson(result, "total de vendas por dia")

    def produto_que_mais_vendido(self):
        result = self.database.collection.aggregate([
            {"$unwind": "$produtos"},
            {"$group": {"_id": "$produtos.descricao", "total_vendas": {"$sum": "$produtos.quantidade"}}},
            {"$sort": {"total_vendas": -1}},
              {"$limit": 1},
             {"$project": {"_id": 0, "produto_mais_vendido": "$_id", "total_vendas": 1}}
            ])
        writeAJson(result, "Produto mais vendido de todos")

    def cliente_que_mais_gastou(self):
        result = self.database.collection.aggregate([
        {"$unwind": "$produtos"},
        {"$group": {"_id": "$cliente_id", "total_gasto": {"$sum": {"$multiply": ["$produtos.quantidade", "$produtos.preco"]}}}},
        {"$sort": {"total_gasto": -1}},
        {"$limit": 1}
        ])
        writeAJson(result, "cliente que mais gastou em uma única compra no mercado")

    def produtos_com_quantidade_acima_de_um(self):
        result = self.database.collection.aggregate([
        {"$unwind": "$produtos"},
        {"$match": {"produtos.quantidade": {"$gt": 1}}},
        {"$group": {"_id": "$produtos.descricao"}}
            ])
        writeAJson(result, "Vendas de produtos acima de uma quantidade")