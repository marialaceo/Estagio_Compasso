-- Modelo relacional
-- Criei as tabelas tb_cliente, tb_carro, tb_combustivel, tb_locacao, tb_km de acordo com a forma (1FN), (2FN) e (3FN)

CREATE TABLE tb_cliente (
	idCliente integer PRIMARY KEY,
	nomeCliente varchar,
	cidadeCliente varchar,
	estadoCliente varchar,
	paisCliente varchar
)

select * from tb_cliente tc 

CREATE TABLE tb_carro (
	idCarro integer PRIMARY KEY,
	classiCarro varchar,
	marcaCarro varchar,
	modeloCarro varchar,
	anoCarro date,
	FOREIGN KEY (idCarro) REFERENCES tb_km (idCarro)
)

select * from tb_carro tc 

CREATE TABLE tb_combustivel (
	idCombustivel integer PRIMARY KEY,
	tipoCombustivel varchar	
)

select * from tb_combustivel tc  

CREATE TABLE tb_vendedor (
	idVendedor integer PRIMARY KEY,
	nomeVendedor varchar,
	sexoVendedor varchar,
	estadoVendedor varchar
)

select * from tb_vendedor tv 

CREATE TABLE tb_locacao (
	idLocacao integer PRIMARY KEY,
	idCarro integer,
	idCliente integer,
	idCombustivel integer,
	idVendedor integer,
	horaLocacao time,
	dataLocacao data,
	qtdDiaaria integer,
	vlrDiaria integer,
	FOREIGN KEY (idCarro) REFERENCES tb_carro (idCarro),
	FOREIGN KEY (idCliente) REFERENCES tb_cliente (idCliente),
	FOREIGN KEY (idCombustivel) REFERENCES tb_carro (idCombustivel),
	FOREIGN KEY (idVendedor) REFERENCES tb_carro (idVendedor)
)

select * from tb_locacao tl  








