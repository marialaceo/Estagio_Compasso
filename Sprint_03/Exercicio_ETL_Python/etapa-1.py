max_filmes = 0
ator_max_filmes = ""


def conversor_int (valor):
    try:
        return int(valor)
    except ValueError:
        return 0



with open('actors.csv', 'r') as entrada:
  linhas = entrada.readlines()
  cabecalho = linhas[0]

  for linha in linhas[1:]:
    colunas = linha.strip().split(',')

    ator = colunas[0]
    numero_de_filmes = conversor_int(colunas[2].strip())

    if numero_de_filmes > max_filmes:
      max_filmes = numero_de_filmes
      ator_max_filmes = ator


sentenca = f'Ator: {ator_max_filmes} com {max_filmes} filmes'

with open( 'etapa-1.txt', 'w') as saida:
  saida.write(sentenca)


print(sentenca)