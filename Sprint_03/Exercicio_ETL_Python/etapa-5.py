
def conversor_float(valor):
  try:
    return float(valor)
  except ValueError:
    return 0


with open('actors.csv', 'r') as entrada:
  linhas = entrada.readlines()
  cabecalho = linhas[0]

  atores = []

  for linha in linhas[1:]:
    colunas = linha.strip().split(',')

    if len(colunas) < 6:
      continue

    bilheterias = colunas[5].strip()
    ator = colunas[0]
    total_receita = conversor_float(colunas[-1])
    atores.append((ator, total_receita))


def get_total_receita(atores):
  return atores[1]


atores.sort(key=get_total_receita, reverse=True)

with open('etapa-5.txt', 'w') as saida:
  for ator, receita in atores:
    saida.write(f'{ator} - {receita}\n')




