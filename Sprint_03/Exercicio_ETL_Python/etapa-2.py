media_receita = 0


def conversor_float(valor):
  try:
    return float(valor)
  except ValueError:
    return 0

receitas = []

with open('actors.csv', 'r') as entrada:
  linhas = entrada.readlines()
  cabecalho = linhas[0]


  for linha in linhas[1:]:
    colunas = linha.strip().split(',')

    if len(colunas) < 6:  
      continue

    receita = conversor_float(colunas[5].strip())
    receitas.append(receita)

if receitas:
  media_receita = sum(receitas)/ len(receitas)

sentenca = f'A média da receita é {media_receita:.2f}'


with open('etapa-2.txt', 'w') as saida:
  saida.write(sentenca)

print(sentenca)