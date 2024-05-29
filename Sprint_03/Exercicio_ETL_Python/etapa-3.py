maior_media_filme = 0
ator_maior_media_filme = ""

def conversor_float(valor):
  try:
    return float(valor)
  except ValueError:
    return 0


with open('actors.csv', 'r') as entrada:
  linhas = entrada.readlines()
  cabecalho = linhas[0]

  for linha in linhas[1:]:
    colunas = linha.strip().split(',')

    if len(colunas) < 6:
      continue

    ator = colunas[0].strip()
    media_filme = conversor_float(colunas[3].strip())

    if media_filme > maior_media_filme:
      maior_media_filme = media_filme
      ator_maior_media_filme = ator


sentenca = f'O ator de maior media é {ator_maior_media_filme} com {maior_media_filme} por filme'

with open('etapa-3.txt', 'w') as saida:
  saida.write(sentenca)

print(sentenca)