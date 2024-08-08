import random
import names

# Definir semente de aleatoriedade
random.seed(40)

# Definir parâmetros
qtd_nomes_unicos = 3000
qtd_nomes_aleatorios = 10000000

# Gerar lista de nomes únicos
aux = []
for _ in range(qtd_nomes_unicos):
    aux.append(names.get_full_name())

print(f"Gerando {qtd_nomes_aleatorios} nomes aleatórios")

# Gerar lista de nomes aleatórios com base na lista de nomes únicos
dados = []
for _ in range(qtd_nomes_aleatorios):
    dados.append(random.choice(aux))

# Salvar os nomes aleatórios em um arquivo de texto
with open('nomes_aleatorios.txt', 'w') as file:
    for nome in dados:
        file.write(nome + '\n')

print("Arquivo 'nomes_aleatorios.txt' gerado com sucesso!")

