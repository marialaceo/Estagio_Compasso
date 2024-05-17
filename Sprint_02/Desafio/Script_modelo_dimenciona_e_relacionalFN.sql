-- Modelo relacional;
-- Criei as tabelas tb_cliente, tb_carro, tb_combustivel, tb_locacao, tb_km de acordo com a forma (1FN), (2FN) e (3FN);
-- A tabela tb_combustivel se relaciona somente com a tabela tb_carro pois uma precisa somente da outra;

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
	idCombustivel integer,
	classiCarro varchar,
	marcaCarro varchar,
	modeloCarro varchar,
	anoCarro date,
	FOREIGN KEY (idCombustivel) REFERENCES tb_carro (idCombustivel)
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
	idVendedor integer,
	horaLocacao time,
	dataLocacao data,
	qtdDiaaria integer,
	vlrDiaria integer,
	FOREIGN KEY (idCarro) REFERENCES tb_carro (idCarro),
	FOREIGN KEY (idCliente) REFERENCES tb_cliente (idCliente),
	FOREIGN KEY (idVendedor) REFERENCES tb_carro (idVendedor)
)

select * from tb_locacao tl  

-- Modelo Dimencional;
-- Atravez de views;
-- A mudança veio na tabela tb_combustivel, que agora não estava mais ligada ao dim_carro e sim ao view fato_locacao

CREATE view fato_locacao as
select DISTINCT 
	idLocacao ,
	idCarro ,
	idCombustivel,
	idCliente ,
	idVendedor ,
	horaLocacao ,
	dataLocacao ,
	qtdDiaaria ,
	vlrDiaria 
	FROM tb_locacao tl 
	left join tb_carro tc 
		on tl.idCarro = tc.idCarro
		
CREATE view dim_cliente as
select DISTINCT 
	idCliente ,
	nomeCliente ,
	cidadeCliente ,
	estadoCliente ,
	paisCliente 
	FROM tb_cliente tc 
	
CREATE view dim_carro as
select DISTINCT 
	idCarro,
	idCombustivel ,
	classiCarro ,
	marcaCarro,
	modeloCarro ,
	anoCarro  
	FROM tb_carro tc
	
CREATE view dim_combustivel as
select DISTINCT 
	idCombustivel,
	tipoCombustivel 
	FROM tb_combustivel 
	
CREATE view dim_vendedor as
select DISTINCT 
	idVendedor ,
	nomeVendedor ,
	sexoVendedor,
	estadoVendedor
	FROM tb_vendedor	

	
	
	
	
	
	
	
	