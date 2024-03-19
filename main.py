from database import Database
from procuct_analyzer import Product_analyzer

db = Database(database="mercado", collection="compras")
#db.resetDatabase()
p =Product_analyzer(db)



ip=p.cliente_que_mais_gastou()

p.produto_que_mais_vendido()

p.produtos_com_quantidade_acima_de_um()

p.total_vendas_por_dia()